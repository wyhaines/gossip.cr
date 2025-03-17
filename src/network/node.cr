require "../messages/base"
require "../messages/membership"
require "../messages/broadcast"
require "../messages/heartbeat"
require "./address"
require "../debug"

module Gossip
  module Network
    # NetworkNode handles TCP communication between nodes
    class NetworkNode
      property address : NodeAddress
      property server : TCPServer
      property connections : Hash(String, TCPSocket)
      property message_queue : Channel(Messages::Base::Message)
      property running : Bool
      @node : Protocol::Node? = nil
      @connections_mutex = Mutex.new

      # Configuration settings
      SOCKET_TIMEOUT      =  5.0 # Seconds
      SOCKET_READ_TIMEOUT = 10.0 # Seconds for read operations
      CONNECTION_RETRIES  =    3 # Number of retries for send operations

      def initialize(@address)
        @server = TCPServer.new(@address.host, @address.port)
        @connections = Hash(String, TCPSocket).new
        @message_queue = Channel(Messages::Base::Message).new(100) # Buffered channel
        @running = true

        # Start accepting connections
        spawn do
          while @running
            if client = @server.accept?
              debug_log "Accepted new connection"
              spawn handle_client(client)
            end
          end
        end

        # Start processing messages
        spawn do
          while @running
            begin
              debug_log "Waiting for message on queue..."
              message = @message_queue.receive
              debug_log "Processing #{message.type} message from #{message.sender}"
              if node = @node
                node.handle_message(message)
              else
                debug_log "Warning: No node set to handle message"
              end
            rescue ex : Channel::ClosedError
              break
            end
          end
        end
      end

      def node=(node : Protocol::Node)
        @node = node
      end

      def node
        @node.not_nil!
      end

      # Handle incoming client connections
      private def handle_client(client : TCPSocket)
        remote_id = ""
        begin
          # Set timeouts on client socket
          client.read_timeout = SOCKET_READ_TIMEOUT.seconds
          client.write_timeout = SOCKET_TIMEOUT.seconds

          while @running
            message = read_message(client)
            debug_log "Received #{message.type} message from #{message.sender}"

            # Clean up the sender ID if it's malformed
            sender = message.sender
            if sender =~ /(.+@.+:\d+)@.+:\d+/
              sender = $1
              debug_log "Cleaned up malformed sender ID from #{message.sender} to #{sender}"
            end

            case message
            when Messages::Membership::Join
              remote_id = sender
              # Store connection for future use if we don't already have one
              @connections_mutex.synchronize do
                if @connections.has_key?(remote_id)
                  debug_log "Already have connection for #{remote_id}, closing old one"
                  @connections[remote_id].close rescue nil
                end
                debug_log "Storing new connection for #{remote_id}"
                @connections[remote_id] = client
              end
              # Update message sender before queuing
              message = Messages::Membership::Join.new(sender)
              @message_queue.send(message)
              debug_log "Queued Join message from #{remote_id}"
            else
              if remote_id.empty?
                remote_id = sender
                @connections_mutex.synchronize do
                  if @connections.has_key?(remote_id)
                    debug_log "Already have connection for #{remote_id}, closing old one"
                    @connections[remote_id].close rescue nil
                  end
                  debug_log "Storing new connection for #{remote_id}"
                  @connections[remote_id] = client
                end
              end
              @message_queue.send(message)
              debug_log "Queued #{message.type} message from #{remote_id}"
            end
          end
        rescue ex : IO::Error | Socket::Error | IO::TimeoutError
          # Only log connection errors if we're still running
          # This prevents noise during shutdown
          if @running
            debug_log "Connection error with #{remote_id}: #{ex.message}"
          end
          @connections_mutex.synchronize do
            @connections.delete(remote_id) unless remote_id.empty?
          end
          client.close rescue nil
        end
      end

      # Read a message from a socket with timeout
      private def read_message(socket : TCPSocket) : Messages::Base::Message
        # Read the length prefix (4 bytes)
        len_bytes = Bytes.new(4)
        bytes_read = socket.read_fully?(len_bytes)
        raise IO::Error.new("Connection closed") unless bytes_read

        # Check if this is a connection test (all bytes should be 0)
        if len_bytes.all? { |b| b == 0 }
          # Send acknowledgment
          socket.write(len_bytes)
          socket.flush
          return read_message(socket)
        end

        len = IO::ByteFormat::NetworkEndian.decode(Int32, len_bytes)
        raise IO::Error.new("Invalid message length: #{len}") if len <= 0 || len > 1024*1024 # Sanity check

        message_bytes = Bytes.new(len)
        bytes_read = socket.read_fully?(message_bytes)
        raise IO::Error.new("Connection closed") unless bytes_read

        message_json = String.new(message_bytes)
        debug_log "Received message: #{message_json}" # Debug log

        # Parse message based on type
        msg_data = JSON.parse(message_json)
        case msg_data["type"].as_s
        when "Join"             then Messages::Membership::Join.from_json(message_json)
        when "ForwardJoin"      then Messages::Membership::ForwardJoin.from_json(message_json)
        when "Shuffle"          then Messages::Membership::Shuffle.from_json(message_json)
        when "ShuffleReply"     then Messages::Membership::ShuffleReply.from_json(message_json)
        when "InitViews"        then Messages::Membership::InitViews.from_json(message_json)
        when "BroadcastMessage" then Messages::Broadcast::BroadcastMessage.from_json(message_json)
        when "LazyPushMessage"  then Messages::Broadcast::LazyPushMessage.from_json(message_json)
        when "MessageRequest"   then Messages::Broadcast::MessageRequest.from_json(message_json)
        when "MessageResponse"  then Messages::Broadcast::MessageResponse.from_json(message_json)
        when "Heartbeat"        then Messages::Heartbeat::Heartbeat.from_json(message_json)
        when "HeartbeatAck"     then Messages::Heartbeat::HeartbeatAck.from_json(message_json)
        else
          raise "Unknown message type: #{msg_data["type"]}"
        end
      end

      # Send a message to another node with retries - NOW THROWS EXCEPTIONS when failures occur
      def send_message(to : String, message : Messages::Base::Message, retry_count = CONNECTION_RETRIES)
        remaining_attempts = retry_count

        while remaining_attempts > 0 && @running
          begin
            if socket = get_or_create_connection(to)
              message_json = message.to_json
              message_bytes = message_json.to_slice
              len = message_bytes.size

              debug_log "Sending #{message.type} message to #{to} (#{len} bytes)"

              # Send length prefix
              len_bytes = Bytes.new(4)
              IO::ByteFormat::NetworkEndian.encode(len, len_bytes)
              socket.write(len_bytes)
              socket.write(message_bytes)
              socket.flush
              return # Success, exit the loop
            else
              if @running
                debug_log "Failed to connect to node #{to}"
              end
              # Throw exception if we can't establish connection - this is important
              # for error propagation to handle failed nodes
              raise Socket::Error.new("Could not establish connection to #{to}")
            end
          rescue ex : IO::Error | Socket::Error | IO::TimeoutError
            remaining_attempts -= 1
            @connections_mutex.synchronize do
              @connections.delete(to)
            end

            if remaining_attempts > 0 && @running
              debug_log "Failed to send message to #{to}, retrying... (#{remaining_attempts} attempts left): #{ex.message}"
            elsif @running
              debug_log "Failed to send message to #{to} after all retries: #{ex.message}"
              # Re-raise the exception so the caller knows about the failure
              raise ex
            end
          end
        end

        # If we get here with no successful send and we're still running,
        # raise an exception to notify the caller
        if @running
          raise Socket::Error.new("Failed to send message to #{to} after #{retry_count} attempts")
        end
      end

      # Get existing connection or create new one with proper handshake
      private def get_or_create_connection(node_id : String) : TCPSocket?
        # Check if we have a valid existing connection
        socket = nil
        @connections_mutex.synchronize do
          socket = @connections[node_id]?
        end

        if socket
          begin
            # Test if connection is still alive
            debug_log "Testing connection to #{node_id}"
            test_bytes = Bytes.new(4, 0_u8) # Four zero bytes for test
            socket.write(test_bytes)
            socket.flush

            # Wait for acknowledgment with timeout
            socket.read_timeout = SOCKET_TIMEOUT.seconds
            response = Bytes.new(4)
            bytes_read = socket.read_fully?(response)
            if !bytes_read || !response.all? { |b| b == 0 }
              debug_log "Invalid test response from #{node_id}"
              raise Socket::Error.new("Invalid test response")
            end

            return socket
          rescue ex
            debug_log "Existing connection to #{node_id} is dead: #{ex.message}"
            @connections_mutex.synchronize do
              @connections.delete(node_id)
            end
          end
        end

        if node_id =~ /(.+)@(.+):(\d+)/
          id, host, port = $1, $2, $3.to_i
          begin
            debug_log "Creating new connection to #{node_id}"
            socket = TCPSocket.new(host, port, connect_timeout: SOCKET_TIMEOUT.seconds)
            socket.tcp_nodelay = true          # Disable Nagle's algorithm
            socket.keepalive = true            # Enable TCP keepalive
            socket.tcp_keepalive_idle = 60     # Start probing after 60 seconds of inactivity
            socket.tcp_keepalive_interval = 10 # Send probes every 10 seconds
            socket.tcp_keepalive_count = 3     # Drop connection after 3 failed probes
            socket.read_timeout = SOCKET_READ_TIMEOUT.seconds
            socket.write_timeout = SOCKET_TIMEOUT.seconds

            # Send join message to identify ourselves
            # Use the node's ID directly to avoid double-appending host:port
            join_msg = Messages::Membership::Join.new(@node.not_nil!.id)
            message_json = join_msg.to_json
            message_bytes = message_json.to_slice
            len = message_bytes.size

            debug_log "Sending initial Join message (#{len} bytes): #{message_json}"
            len_bytes = Bytes.new(4)
            IO::ByteFormat::NetworkEndian.encode(len, len_bytes)
            socket.write(len_bytes)
            socket.write(message_bytes)
            socket.flush

            @connections_mutex.synchronize do
              @connections[node_id] = socket
            end
            debug_log "Successfully established connection to #{node_id}"
            return socket
          rescue ex : Socket::Error | IO::TimeoutError
            debug_log "Failed to connect to #{node_id}: #{ex.message}"
            return nil
          end
        end
        nil
      end

      # Test a connection to a node without sending an actual message
      def test_connection(node_id : String) : Bool
        begin
          if socket = get_or_create_connection(node_id)
            return true
          end
        rescue ex
          debug_log "Connection test to #{node_id} failed: #{ex.message}"
        end
        false
      end

      # Clean up resources
      def close
        @running = false
        @message_queue.close
        @connections_mutex.synchronize do
          @connections.each_value(&.close)
          @connections.clear
        end
        @server.close
      end
    end
  end
end
