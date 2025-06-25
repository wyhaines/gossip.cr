require "./node"
require "../messages/membership"
require "../messages/broadcast"
require "../messages/heartbeat"
require "../debug"
require "wait_group"

module Gossip
  module Protocol
    # Extension of Node class with message handlers
    class Node
      # Handle incoming messages based on their type
      def handle_message(message : Messages::Base::Message)
        case message
        when Messages::Membership::Join
          handle_join(message)
        when Messages::Membership::ForwardJoin
          handle_forward_join(message)
        when Messages::Membership::Shuffle
          handle_shuffle(message)
        when Messages::Membership::ShuffleReply
          handle_shuffle_reply(message)
        when Messages::Membership::InitViews
          handle_init_views(message)
        when Messages::Membership::Redirect
          handle_redirect(message)
        when Messages::Broadcast::BroadcastMessage
          handle_broadcast(message)
        when Messages::Broadcast::LazyPushMessage
          handle_lazy_push(message)
        when Messages::Broadcast::MessageRequest
          handle_message_request(message)
        when Messages::Broadcast::MessageResponse
          handle_message_response(message)
        when Messages::Heartbeat::Heartbeat
          handle_heartbeat(message)
        when Messages::Heartbeat::HeartbeatAck
          handle_heartbeat_ack(message)
        else
          debug_log "Node #{@id}: Unknown message type"
        end
      end

      def handle_redirect(message : Messages::Membership::Redirect)
        sender = message.sender
        target_nodes = message.target_nodes

        debug_log "Node #{@id}: Received REDIRECT from #{sender} suggesting to connect to: #{target_nodes.join(", ")}"

        # Check if we need more connections first
        need_connection = false
        @views_mutex.synchronize do
          need_connection = @active_view.size < min_active_view_size
        end

        # Attempt to connect if we have few connections or with high probability
        # This helps distribute connections more evenly
        if need_connection || rand < 0.8
          # Try to connect to one of the suggested nodes
          target_nodes.shuffle.each do |target|
            # Avoid connecting to ourselves
            next if target == @id

            # Skip if we're already connected to this node
            @views_mutex.synchronize do
              next if @active_view.includes?(target)
            end

            # Try to establish connection
            begin
              join_msg = Messages::Membership::Join.new(@id)
              send_message(target, join_msg)
              debug_log "Node #{@id}: Connected to redirect-suggested node #{target}"

              # Optionally, we could remove our connection to the original node
              # to better distribute the network, but only after successfully
              # connecting to a new node
              if rand < 0.3 # 30% chance to rebalance by dropping original connection
                @views_mutex.synchronize do
                  if @active_view.includes?(sender)
                    @active_view.delete(sender)
                    @passive_view << sender
                    debug_log "Node #{@id}: Moved #{sender} to passive view after redirect"
                  end
                end
              end

              break # Stop after one successful connection
            rescue ex
              debug_log "Node #{@id}: Failed to connect to redirect-suggested node #{target}: #{ex.message}"
              @failures_mutex.synchronize do
                @failed_nodes << target
              end
            end
          end
        else
          debug_log "Node #{@id}: Skipped redirect connection (sufficient connections)"
        end
      end

      # Handle a new node joining via this node
      def handle_join(message : Messages::Membership::Join)
        sender = message.sender
        debug_log "Node #{@id}: Received JOIN from #{sender}"

        # Remove from failed nodes if it was there
        @failures_mutex.synchronize do
          @failed_nodes.delete(sender)
        end

        # Use mutex for thread safety
        should_redistribute = false
        @views_mutex.synchronize do
          # If we're already connected, just update views
          if @active_view.includes?(sender)
            debug_log "Node #{@id}: Already connected to #{sender}"
            return
          end

          # Add to active view if there's space
          if @active_view.size < max_active_view_size
            @active_view << sender
            # Remove from passive view if present (fixes duplication bug)
            @passive_view.delete(sender)
            debug_log "Node #{@id}: Added #{sender} to active view"
          else
            # Move random node to passive view
            displaced = @active_view.sample
            @active_view.delete(displaced)
            @passive_view << displaced unless displaced == sender || @failed_nodes.includes?(displaced)
            @active_view << sender
            # Remove from passive view if present (fixes duplication bug)
            @passive_view.delete(sender)
            debug_log "Node #{@id}: Displaced #{displaced} to passive view"
          end

          # Check if we're at or over the max active limit (bootstrap node issue)
          should_redistribute = @active_view.size >= max_active_view_size
        end

        # Get view snapshots for thread safety
        active_nodes = [] of String
        passive_nodes = [] of String
        @views_mutex.synchronize do
          # Using reject to create new arrays for sending
          active_nodes = @active_view.reject { |n| n == sender || @failed_nodes.includes?(n) }
          passive_nodes = @passive_view.reject { |n| @failed_nodes.includes?(n) }
        end

        # Send our views to the new node
        begin
          init_msg = Messages::Membership::InitViews.new(@id, active_nodes, passive_nodes)
          send_message(sender, init_msg)
          debug_log "Node #{@id}: Sent InitViews to #{sender}"
        rescue ex
          debug_log "Node #{@id}: Failed to send InitViews to #{sender}: #{ex.message}"
          handle_node_failure(sender)
          return
        end

        # Implement connection redistribution for bootstrap node or overloaded nodes
        if should_redistribute
          candidates = [] of String
          @views_mutex.synchronize do
            # Select a few nodes from our active view for redistribution
            candidates = @active_view.reject { |n| n == sender }.to_a.sample(2)
          end

          if !candidates.empty?
            begin
              # Send a redirect message suggesting alternative nodes to connect to
              redirect_msg = Messages::Membership::Redirect.new(@id, candidates)
              send_message(sender, redirect_msg)
              debug_log "Node #{@id}: Sent redistribution suggestion to #{sender}"
            rescue ex
              debug_log "Node #{@id}: Failed to send redistribution suggestion: #{ex.message}"
              # Proceed with normal join process even if redistribution fails
            end
          end
        end

        # Propagate join to some nodes in our active view (original code)
        active_nodes_snapshot = [] of String
        @views_mutex.synchronize do
          active_nodes_snapshot = @active_view.reject { |n| n == sender || @failed_nodes.includes?(n) }
        end

        forward_count = Math.min(active_nodes_snapshot.size, 3) # Forward to at most 3 other nodes for better propagation
        if forward_count > 0
          targets = active_nodes_snapshot.sample(forward_count)
          targets.each do |target|
            begin
              forward_msg = Messages::Membership::ForwardJoin.new(@id, sender, TTL)
              send_message(target, forward_msg)
              debug_log "Node #{@id}: Forwarded join from #{sender} to #{target}"
            rescue ex
              debug_log "Node #{@id}: Failed to forward join to #{target}: #{ex.message}"
              handle_node_failure(target)
            end
          end
        end
      end

      # Handle propagation of a join message
      def handle_forward_join(message : Messages::Membership::ForwardJoin)
        new_node = message.new_node
        ttl = message.ttl

        if ttl > 0
          # Get a snapshot of active view
          active_nodes = [] of String
          @views_mutex.synchronize do
            active_nodes = @active_view
          end

          active_nodes.each do |node|
            next if node == message.sender
            begin
              forward_msg = Messages::Membership::ForwardJoin.new(@id, new_node, ttl - 1)
              send_message(node, forward_msg)
            rescue ex
              debug_log "Node #{@id}: Failed to forward join to #{node}: #{ex.message}"
              handle_node_failure(node)
            end
          end
        else
          @views_mutex.synchronize do
            @passive_view << new_node unless @active_view.includes?(new_node) || new_node == @id
          end
          debug_log "Node #{@id}: Added #{new_node} to passive view"
        end
      end

      # Handle an incoming shuffle request
      def handle_shuffle_reply(message : Messages::Membership::ShuffleReply)
        received_nodes = message.nodes

        @views_mutex.synchronize do
          received_nodes.each do |node|
            # Fix: Don't add nodes that are in active view to passive view
            if node != @id && !@active_view.includes?(node) && @passive_view.size < max_passive_view_size
              @passive_view << node
            end
          end
        end
        
        # Update network size estimate after shuffle reply
        update_network_size_estimate
      end

      def handle_shuffle(message : Messages::Membership::Shuffle)
        sender = message.sender
        received_nodes = message.nodes

        # Create a snapshot of combined views
        combined_view = [] of String
        @views_mutex.synchronize do
          combined_view = (@active_view | @passive_view)
        end

        own_nodes = combined_view.sample([SHUFFLE_SIZE, combined_view.size].min)

        begin
          reply_msg = Messages::Membership::ShuffleReply.new(@id, own_nodes)
          send_message(sender, reply_msg)

          # Update passive view with received nodes
          @views_mutex.synchronize do
            received_nodes.each do |node|
              if node != @id && !@active_view.includes?(node) && @passive_view.size < max_passive_view_size
                @passive_view << node
              end
            end
          end
          debug_log "Node #{@id}: Shuffled with #{sender}"
        rescue ex
          debug_log "Node #{@id}: Failed to send shuffle reply to #{sender}: #{ex.message}"
          handle_node_failure(sender)
        end
      end

      # Initialize views for a new node
      def handle_init_views(message : Messages::Membership::InitViews)
        sender = message.sender
        debug_log "Node #{@id}: Received InitViews from #{sender}"

        # Add sender to our active view if not already present
        should_respond = false
        @views_mutex.synchronize do
          if !@active_view.includes?(sender)
            @active_view << sender
            # Fix: Remove from passive view if present (fixes duplication bug)
            @passive_view.delete(sender)
            debug_log "Node #{@id}: Added #{sender} to active view"
            should_respond = true
          end
        end
        
        # Send back our own InitViews to complete the bidirectional connection
        if should_respond
          begin
            # Get view snapshots for thread safety
            active_nodes = [] of String
            passive_nodes = [] of String
            @views_mutex.synchronize do
              active_nodes = @active_view.reject { |n| n == sender || @failed_nodes.includes?(n) }
              passive_nodes = @passive_view.reject { |n| @failed_nodes.includes?(n) }
            end
            
            response_msg = Messages::Membership::InitViews.new(@id, active_nodes, passive_nodes)
            send_message(sender, response_msg)
            debug_log "Node #{@id}: Sent InitViews response to #{sender}"
          rescue ex
            debug_log "Node #{@id}: Failed to send InitViews response to #{sender}: #{ex.message}"
          end
        end

        # Process suggested active nodes
        message.active_nodes.each do |node|
          next if node == @id

          @views_mutex.synchronize do
            next if @active_view.includes?(node)

            if @active_view.size < max_active_view_size
              # Try to establish bidirectional connection by sending a join
              begin
                join_msg = Messages::Membership::Join.new(@id)
                send_message(node, join_msg)
                debug_log "Node #{@id}: Sent Join to suggested active node #{node}"
              rescue ex
                debug_log "Node #{@id}: Failed to send Join to suggested node #{node}: #{ex.message}"
                @failures_mutex.synchronize do
                  @failed_nodes << node
                end
              end
            else
              # Add to passive view if not full
              if @passive_view.size < max_passive_view_size
                @passive_view << node
                debug_log "Node #{@id}: Added suggested node #{node} to passive view"
              end
            end
          end
        end

        # Add passive nodes
        @views_mutex.synchronize do
          message.passive_nodes.each do |node|
            next if node == @id || @active_view.includes?(node)
            if @passive_view.size < MAX_PASSIVE
              @passive_view << node
              debug_log "Node #{@id}: Added #{node} to passive view"
            end
          end
        end

        debug_log "Node #{@id}: Initialized views - Active: #{@active_view}, Passive: #{@passive_view}"
        
        # Update network size estimate after receiving view information
        update_network_size_estimate
      end

      # Handle a broadcast message (Plumtree eager/lazy push)
      def handle_broadcast(message : Messages::Broadcast::BroadcastMessage)
        message_id = message.message_id
        sender = message.sender
        content = message.content
      
        # Use mutex for thread safety
        already_received = false
        @messages_mutex.synchronize do
          already_received = @received_messages.includes?(message_id)
          unless already_received
            @received_messages << message_id
            @message_contents[message_id] = content
          end
        end
      
        if !already_received
          debug_log "Node #{@id}: Received broadcast '#{content}' from #{sender}"
      
          # Get active view snapshot
          active_nodes = [] of String
          @views_mutex.synchronize do
            active_nodes = @active_view.reject { |node| node == sender || @failed_nodes.includes?(node) }
          end
      
          # Skip forwarding if there are no nodes to forward to
          return if active_nodes.empty?
      
          # Use a WaitGroup to track forwarding completion
          wg = WaitGroup.new(active_nodes.size)
          
          # Forward immediately to all active view nodes except sender in parallel
          active_nodes.each do |node|
            spawn do
              begin
                # Always use eager push (lazy_push_probability is 0.0)
                forward_msg = Messages::Broadcast::BroadcastMessage.new(@id, message_id, content)
                send_message(node, forward_msg)
                debug_log "Node #{@id}: Forwarded broadcast to #{node}"
              rescue ex
                debug_log "Node #{@id}: Failed to forward broadcast to #{node}: #{ex.message}"
                handle_node_failure(node)
              ensure
                wg.done
              end
            end
          end
      
        # TODO: This doesn't work, but a channel based cooperative interrupt could be constructed to implement this intention.
        #     begin
        #       Fiber.timeout(5.seconds) do
        #         wg.wait
        #       end
        #     rescue ex : Fiber::TimeoutError
        #       debug_log "Node #{@id}: Broadcast forward timeout - some nodes may not have received message"
        #     end
        #   end
        end
      end

      # Handle a lazy push notification with improved recovery
      def handle_lazy_push(message : Messages::Broadcast::LazyPushMessage)
        message_id = message.message_id
        sender = message.sender

        # Use mutex for thread safety
        should_request = false
        @messages_mutex.synchronize do
          should_request = !@received_messages.includes?(message_id)

          if should_request
            # Add this sender as a potential provider for this message
            if @missing_messages.has_key?(message_id)
              @missing_messages[message_id] << sender unless @missing_messages[message_id].includes?(sender)
            else
              @missing_messages[message_id] = [sender]
            end
          end
        end

        if should_request
          @pending_requests_mutex.synchronize do
            # Only send request if not already pending
            if !@pending_requests.includes?(message_id)
              @pending_requests << message_id

              # Request the missing message
              request_msg = Messages::Broadcast::MessageRequest.new(@id, message_id, true)
              begin
                send_message(sender, request_msg)
                debug_log "Node #{@id}: Requested missing message #{message_id} from #{sender}"
              rescue ex
                debug_log "Node #{@id}: Failed to request message from #{sender}: #{ex.message}"
                # Don't remove from pending - will be retried by recovery process
                handle_node_failure(sender)
              end
            end
          end
        end
      end

      # Handle a message request
      def handle_message_request(message : Messages::Broadcast::MessageRequest)
        message_id = message.message_id
        sender = message.sender

        # Get content if we have it
        content = nil
        @messages_mutex.synchronize do
          content = @message_contents[message_id]?
        end

        if content
          begin
            response_msg = Messages::Broadcast::MessageResponse.new(@id, message_id, content)
            send_message(sender, response_msg)
            debug_log "Node #{@id}: Sent message response for #{message_id} to #{sender}"
          rescue ex
            debug_log "Node #{@id}: Failed to send message response to #{sender}: #{ex.message}"
            handle_node_failure(sender)
          end
        else
          debug_log "Node #{@id}: Requested message #{message_id} not found for #{sender}"
        end
      end

      # Handle a message response
      def handle_message_response(message : Messages::Broadcast::MessageResponse)
        message_id = message.message_id
        content = message.content

        # Use mutex for thread safety
        was_missing = false
        @messages_mutex.synchronize do
          was_missing = @missing_messages.has_key?(message_id)

          if was_missing
            @missing_messages.delete(message_id)
            @received_messages << message_id
            @message_contents[message_id] = content
          end
        end

        @pending_requests_mutex.synchronize do
          @pending_requests.delete(message_id)
        end

        if was_missing
          debug_log "Node #{@id}: Recovered missing message #{message_id}"

          # Get active view snapshot
          active_nodes = [] of String
          @views_mutex.synchronize do
            active_nodes = @active_view
          end

          # Forward to active view with eager push
          active_nodes.each do |node|
            next if node == message.sender

            @failures_mutex.synchronize do
              next if @failed_nodes.includes?(node)
            end

            begin
              forward_msg = Messages::Broadcast::BroadcastMessage.new(@id, message_id, content)
              send_message(node, forward_msg)
              debug_log "Node #{@id}: Forwarded recovered message to #{node}"
            rescue ex
              debug_log "Node #{@id}: Failed to forward recovered message to #{node}: #{ex.message}"
              handle_node_failure(node)
            end
          end
        end
      end

      # Handle heartbeat message
      private def handle_heartbeat(message : Messages::Heartbeat::Heartbeat)
        # Send acknowledgment
        ack = Messages::Heartbeat::HeartbeatAck.new(@id)
        begin
          send_message(message.sender, ack)
        rescue ex
          debug_log "Node #{@id}: Failed to send heartbeat ack to #{message.sender}: #{ex.message}"
        end
      end

      # Handle heartbeat acknowledgment
      private def handle_heartbeat_ack(message : Messages::Heartbeat::HeartbeatAck)
        # Node is alive, nothing to do
      end
    end
  end
end
