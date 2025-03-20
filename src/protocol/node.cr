require "set"
require "../messages/base"
require "../messages/membership"
require "../messages/broadcast"
require "../messages/heartbeat"
require "../network/node"
require "./config"
require "../debug"

module Gossip
  module Protocol
    # Node class implementing the gossip protocol
    class Node
      include Config

      property id : String
      property active_view : Set(String)
      property passive_view : Set(String)
      property received_messages : Set(String)
      property message_contents : Hash(String, String)
      property missing_messages : Hash(String, Array(String)) # Track missing messages with potential providers
      property lazy_push_probability : Float64
      property network : Network::NetworkNode
      property failed_nodes : Set(String)

      # Mutexes for thread safety
      @views_mutex = Mutex.new
      @messages_mutex = Mutex.new
      @failures_mutex = Mutex.new
      @pending_requests = Set(String).new
      @pending_requests_mutex = Mutex.new

      def initialize(id : String, network : Network::NetworkNode)
        @id = id
        @network = network
        @active_view = Set(String).new
        @passive_view = Set(String).new
        @received_messages = Set(String).new
        @message_contents = Hash(String, String).new
        @missing_messages = Hash(String, Array(String)).new
        @lazy_push_probability = LAZY_PUSH_PROB
        @failed_nodes = Set(String).new

        # Set this node as the network's node
        @network.node = self

        # Start periodic shuffling and view maintenance
        spawn do
          while @network.running
            sleep(Time::Span.new(seconds: SHUFFLE_INTERVAL.to_i, nanoseconds: ((SHUFFLE_INTERVAL % 1) * 1_000_000_000).to_i))
            maintain_views
            send_shuffle
          end
        end

        # Start heartbeat mechanism to detect failed nodes
        spawn do
          while @network.running
            sleep(Time::Span.new(seconds: HEARTBEAT_INTERVAL.to_i, nanoseconds: ((HEARTBEAT_INTERVAL % 1) * 1_000_000_000).to_i))
            send_heartbeats
          end
        end

        # Start message recovery process
        spawn handle_message_recovery
      end

      # Modified send_message to use network layer and handle failures
      def send_message(to : String, message : Messages::Base::Message)
        begin
          @network.send_message(to, message)
        rescue ex : IO::Error | Socket::Error | IO::TimeoutError
          debug_log "Node #{@id}: Error sending #{message.type} to #{to}: #{ex.message}"
          handle_node_failure(to)
          raise ex # Re-raise to allow caller to handle
        end
      end

      # Handle node failures
      def handle_node_failure(node : String)
        # Check if the node is in active view first
        is_active = false
        @views_mutex.synchronize do
          is_active = @active_view.includes?(node)
          if is_active
            @active_view.delete(node)
            debug_log "Node #{@id}: Removed failed node #{node} from active view"
          end
        end

        # Mark as failed
        @failures_mutex.synchronize do
          @failed_nodes << node
        end

        # Promote a passive node if needed (and the failed node was active)
        if is_active
          promote_passive_node
        end
      end

      private def promote_passive_node
        # Get candidate nodes from passive view
        passive_nodes = [] of String
        @views_mutex.synchronize do
          return if @active_view.size >= MIN_ACTIVE || @passive_view.empty?
          passive_nodes = @passive_view.to_a.shuffle
        end

        # Try nodes from passive view until one works
        passive_nodes.each do |candidate|
          # Remove from passive view first
          @views_mutex.synchronize do
            @passive_view.delete(candidate)
          end

          begin
            # Try to establish connection
            join_msg = Messages::Membership::Join.new(@id)
            send_message(candidate, join_msg)

            # Add to active view if successful
            @views_mutex.synchronize do
              @active_view << candidate
            end

            debug_log "Node #{@id}: Promoted #{candidate} from passive to active view"
            break # Exit once we successfully promote one node
          rescue ex
            @failures_mutex.synchronize do
              @failed_nodes << candidate
            end
            debug_log "Node #{@id}: Failed to promote #{candidate}: #{ex.message}"
          end
        end
      end

      # Send heartbeats to all active nodes
      private def send_heartbeats
        active_nodes = [] of String
        @views_mutex.synchronize do
          active_nodes = @active_view
        end

        failed_nodes = [] of String

        active_nodes.each do |node|
          begin
            heartbeat = Messages::Heartbeat::Heartbeat.new(@id)
            send_message(node, heartbeat)
          rescue ex
            debug_log "Node #{@id}: Heartbeat to #{node} failed: #{ex.message}"
            failed_nodes << node
          end
        end

        # Process failed nodes outside the loop to avoid modifying while iterating
        failed_nodes.each do |node|
          handle_node_failure(node)
        end
      end

      # New method to maintain views and handle disconnected nodes
      private def maintain_views
        # Get a snapshot of active view to avoid concurrent modification
        active_nodes = [] of String
        @views_mutex.synchronize do
          active_nodes = @active_view
        end

        failed_nodes = [] of String

        # Test connections to active view nodes
        active_nodes.each do |node|
          begin
            # Use connection test instead of sending a message
            unless @network.test_connection(node)
              failed_nodes << node
            end
          rescue ex
            failed_nodes << node
          end
        end

        # Process failed nodes
        failed_nodes.each do |node|
          handle_node_failure(node)
        end

        # New code: Enforce MAX_ACTIVE limit periodically
        @views_mutex.synchronize do
          # Enforce active view size limit
          while @active_view.size > MAX_ACTIVE
            # Move excess nodes to passive view
            node_to_move = @active_view.to_a.sample # Pick a random node
            @active_view.delete(node_to_move)
            
            # Only add to passive if not already there
            unless @passive_view.includes?(node_to_move) || node_to_move == @id
              @passive_view << node_to_move
            end
            
            debug_log "Node #{@id}: Enforced MAX_ACTIVE by moving #{node_to_move} to passive view"
          end
          
          # Enforce passive view size limit too
          while @passive_view.size > MAX_PASSIVE
            node_to_remove = @passive_view.to_a.sample
            @passive_view.delete(node_to_remove)
            debug_log "Node #{@id}: Enforced MAX_PASSIVE by removing #{node_to_remove}"
          end
          
          # Ensure no duplicates between views (belt and suspenders approach)
          duplicates = @active_view & @passive_view
          unless duplicates.empty?
            duplicates.each do |node|
              @passive_view.delete(node)
              debug_log "Node #{@id}: Removed duplicate node #{node} from passive view"
            end
          end
        end

        # Check if we need to promote passive nodes
        should_promote = false
        @views_mutex.synchronize do
          should_promote = @active_view.size < MIN_ACTIVE
        end

        # Call promote outside of the mutex lock if needed
        if should_promote
          promote_passive_node
        end
      end

      # Send a shuffle message to a random node
      def send_shuffle
        # Get a snapshot of combined views
        all_nodes = [] of String
        @views_mutex.synchronize do
          all_nodes = (@active_view | @passive_view)
        end

        if all_nodes.size > 0
          target = all_nodes.sample
          shuffle_nodes = all_nodes.sample([SHUFFLE_SIZE, all_nodes.size].min)

          begin
            shuffle_msg = Messages::Membership::Shuffle.new(@id, shuffle_nodes)
            send_message(target, shuffle_msg)
          rescue ex
            debug_log "Node #{@id}: Failed to send shuffle to #{target}: #{ex.message}"
            handle_node_failure(target)
          end
        end
      end

      # Initiate a broadcast
      def broadcast(content : String)
        message_id = "#{@id}-#{Time.utc.to_unix_ms}-#{rand(1000)}"
        msg = Messages::Broadcast::BroadcastMessage.new(@id, message_id, content)

        # Mark as received by us
        @messages_mutex.synchronize do
          @received_messages << message_id
          @message_contents[message_id] = content
        end

        debug_log "Node #{@id}: Broadcasting message '#{content}'"

        # Get active view snapshot
        active_nodes = [] of String
        @views_mutex.synchronize do
          active_nodes = @active_view
        end

        # Send to all active view members
        active_nodes.each do |node|
          @failures_mutex.synchronize do
            next if @failed_nodes.includes?(node)
          end

          begin
            # Use eager push for initial broadcast to speed up propagation
            send_message(node, msg)
            debug_log "Node #{@id}: Sent broadcast to #{node}"
          rescue ex
            debug_log "Node #{@id}: Failed to send broadcast to #{node}: #{ex.message}"
            handle_node_failure(node)
          end
        end

        # Return the message ID so applications can track it if needed
        message_id
      end

      # Background fiber to handle message recovery
      private def handle_message_recovery
        while @network.running
          sleep(Time::Span.new(seconds: REQUEST_RETRY_INTERVAL.to_i, nanoseconds: ((REQUEST_RETRY_INTERVAL % 1) * 1_000_000_000).to_i))
          # Get messages that need recovery
          messages_to_recover = {} of String => Array(String)
          @messages_mutex.synchronize do
            @missing_messages.each do |message_id, providers|
              messages_to_recover[message_id] = providers.dup unless providers.empty?
            end
          end

          next if messages_to_recover.empty?

          # Try to recover each missing message
          messages_to_recover.each do |message_id, providers|
            @pending_requests_mutex.synchronize do
              next unless @pending_requests.includes?(message_id)
            end

            # Try each provider
            success = false
            providers.shuffle.each do |provider|
              @failures_mutex.synchronize do
                next if @failed_nodes.includes?(provider)
              end

              begin
                request_msg = Messages::Broadcast::MessageRequest.new(@id, message_id, false)
                send_message(provider, request_msg)
                success = true
                break # Successfully sent request
              rescue ex
                debug_log "Node #{@id}: Failed to request message recovery from #{provider}: #{ex.message}"
                handle_node_failure(provider)
              end
            end

            # If no providers worked, try someone from active view as last resort
            unless success
              active_nodes = [] of String
              @views_mutex.synchronize do
                active_nodes = @active_view
              end

              active_nodes.to_a.shuffle.each do |node|
                begin
                  request_msg = Messages::Broadcast::MessageRequest.new(@id, message_id, false)
                  send_message(node, request_msg)
                  debug_log "Node #{@id}: Last resort message request to #{node}"
                  break # Successfully sent request
                rescue ex
                  debug_log "Node #{@id}: Failed last resort request to #{node}: #{ex.message}"
                  handle_node_failure(node)
                end
              end
            end
          end
        end
      end
    end
  end
end
