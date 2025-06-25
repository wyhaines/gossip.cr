require "../messages/base"
require "../messages/membership"
require "../messages/broadcast"
require "../messages/heartbeat"
require "./address"
require "./metrics_collector"
require "./adaptive_rate_limiter"
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
      property rate_limiter : AdaptiveRateLimiter
      property adaptive_rate_limiting : Bool = true
      property metrics_logging_interval : Int32 = 60 # seconds
      @node : Protocol::Node? = nil
      @connections_mutex = Mutex.new
      @metrics_logging_fiber : Fiber? = nil

      # Configuration settings
      SOCKET_TIMEOUT         =  5.0        # Seconds
      SOCKET_READ_TIMEOUT    = 10.0        # Seconds for read operations
      CONNECTION_RETRIES     =    3        # Number of retries for send operations
      MAX_MESSAGE_SIZE       = 1024 * 1024 # 1MB maximum message size
      DEFAULT_QUEUE_CAPACITY = 10000       # Default message queue capacity

      def initialize(@address, queue_capacity : Int32 = DEFAULT_QUEUE_CAPACITY)
        @server = TCPServer.new(@address.host, @address.port)
        @connections = Hash(String, TCPSocket).new
        @message_queue = Channel(Messages::Base::Message).new(queue_capacity)
        @running = true

        # Initialize the rate limiter
        @rate_limiter = AdaptiveRateLimiter.new(
          capacity: queue_capacity,
          threshold_pct: 0.7, # Start limiting at 70% capacity
          base_delay_ms: 5,   # Start with small 5ms delays
          max_delay_ms: 200,  # Max delay of 200ms at full capacity
          adaptive_curve: 2.0 # Quadratic growth curve
        )

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
              message = @message_queue.receive
              # Apply rate limiting after receiving from the queue
              @rate_limiter.post_receive

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

        # Start metrics logging if enabled
        start_metrics_logging
      end

      # Start periodic metrics logging
      def start_metrics_logging
        return if @metrics_logging_interval <= 0

        @metrics_logging_fiber = spawn do
          while @running
            sleep @metrics_logging_interval.seconds
            debug_log @rate_limiter.metrics.summary
          end
        end
      end

      # Adjust metrics logging interval (0 to disable)
      def metrics_logging_interval=(interval : Int32)
        @metrics_logging_interval = interval

        # Restart metrics logging if needed
        if @metrics_logging_fiber && @metrics_logging_fiber.not_nil!.dead?
          start_metrics_logging
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
            begin
              message = read_message(client)
              debug_log "Received #{message.type} message from #{message.sender}"

              # Clean up the sender ID if it's malformed
              sender = message.sender
              if sender =~ /(.+@.+:\d+)@.+:\d+/
                sender = $1
                debug_log "Cleaned up malformed sender ID from #{message.sender} to #{sender}"
              end

              # Always store connection using the sender ID, regardless of message type
              if remote_id.empty? || remote_id != sender
                remote_id = sender
                @connections_mutex.synchronize do
                  if @connections.has_key?(remote_id)
                    # Don't close the existing connection if it's the same socket
                    existing_socket = @connections[remote_id]
                    if existing_socket != client
                      debug_log "Already have connection for #{remote_id}, closing old one"
                      existing_socket.close rescue nil
                    end
                  end
                  debug_log "Storing connection for #{remote_id}"
                  @connections[remote_id] = client
                end
              end

              # Update message if it's a Join to ensure clean sender ID
              if message.is_a?(Messages::Membership::Join)
                message = Messages::Membership::Join.new(sender)
              end

              # Track queue pressure before attempting to queue
              if @adaptive_rate_limiting
                @rate_limiter.pre_send
              end

              begin
                @message_queue.send(message)
                @rate_limiter.post_send if @adaptive_rate_limiting
                debug_log "Queued #{message.type} message from #{remote_id}"
              rescue ex
                # If we added to count but failed to queue, adjust the count back
                if @adaptive_rate_limiting
                  @rate_limiter.post_receive
                end
                raise ex
              end
            rescue ex : JSON::ParseException
              # Handle JSON parsing errors more gracefully
              debug_log "JSON parsing error with #{remote_id}: #{ex.message}"
              # Continue and try to read the next message if possible
              # This avoids killing the connection on minor parsing issues
              if ex_message = ex.message
                if ex_message.includes?("<EOF>") || ex_message.includes?("unexpected end of input")
                  # Connection likely closed or message truncated, exit the loop
                  raise IO::Error.new("Connection closed or truncated message")
                end
              end
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

      # Read a message from a socket with timeout and improved error handling
      private def read_message(socket : TCPSocket) : Messages::Base::Message
        # Read the length prefix (4 bytes)
        len_bytes = Bytes.new(4)
        bytes_read = socket.read_fully?(len_bytes)

        if bytes_read.nil? || bytes_read < 4
          raise IO::Error.new("Connection closed while reading message length")
        end


        len = IO::ByteFormat::NetworkEndian.decode(Int32, len_bytes)

        # Validate message length for security and to prevent memory issues
        if len <= 0
          raise IO::Error.new("Invalid message length: #{len} (non-positive)")
        elsif len > MAX_MESSAGE_SIZE
          raise IO::Error.new("Message too large: #{len} bytes (max: #{MAX_MESSAGE_SIZE})")
        end

        # Allocate buffer and read the full message
        message_bytes = Bytes.new(len)
        bytes_read = socket.read_fully?(message_bytes)

        if bytes_read.nil? || bytes_read < len
          raise IO::Error.new("Connection closed while reading message body (got #{bytes_read || 0} of #{len} bytes)")
        end

        # Convert bytes to string and parse JSON
        message_json = String.new(message_bytes)

        # Verify we have valid JSON before attempting to parse
        if message_json.empty?
          raise JSON::ParseException.new("Empty message", 1, 1)
        end

        debug_log "Received message: #{message_json[0..50]}..." # Debug log (truncated for readability)

        begin
          # Parse message based on type
          msg_data = JSON.parse(message_json)

          unless msg_data.as_h?.try &.has_key?("type")
            raise JSON::ParseException.new("Missing 'type' field in message", 1, 1)
          end

          msg_type = msg_data["type"].as_s

          case msg_type
          when "Join"             then Messages::Membership::Join.from_json(message_json)
          when "ForwardJoin"      then Messages::Membership::ForwardJoin.from_json(message_json)
          when "Shuffle"          then Messages::Membership::Shuffle.from_json(message_json)
          when "ShuffleReply"     then Messages::Membership::ShuffleReply.from_json(message_json)
          when "InitViews"        then Messages::Membership::InitViews.from_json(message_json)
          when "Redirect"         then Messages::Membership::Redirect.from_json(message_json)
          when "BroadcastMessage" then Messages::Broadcast::BroadcastMessage.from_json(message_json)
          when "LazyPushMessage"  then Messages::Broadcast::LazyPushMessage.from_json(message_json)
          when "MessageRequest"   then Messages::Broadcast::MessageRequest.from_json(message_json)
          when "MessageResponse"  then Messages::Broadcast::MessageResponse.from_json(message_json)
          when "Heartbeat"        then Messages::Heartbeat::Heartbeat.from_json(message_json)
          when "HeartbeatAck"     then Messages::Heartbeat::HeartbeatAck.from_json(message_json)
          when "DigestRequest"    then Messages::AntiEntropy::DigestRequest.from_json(message_json)
          when "DigestReply"      then Messages::AntiEntropy::DigestReply.from_json(message_json)
          when "MessagesRequest"  then Messages::AntiEntropy::MessagesRequest.from_json(message_json)
          when "MessagesResponse" then Messages::AntiEntropy::MessagesResponse.from_json(message_json)
          else
            raise ArgumentError.new("Unknown message type: #{msg_type}")
          end
        rescue ex : JSON::ParseException | TypeCastError
          debug_log "Error parsing message JSON: #{ex.message}, content: #{message_json[0..100]}..."
          raise ex # Re-raise after logging
        end
      end

      # Send a message to another node with retries - THROWS EXCEPTIONS when failures occur
      def send_message(to : String, message : Messages::Base::Message, retry_count = CONNECTION_RETRIES)
        remaining_attempts = retry_count

        while remaining_attempts > 0 && @running
          begin
            if socket = get_or_create_connection(to)
              message_json = message.to_json
              message_bytes = message_json.to_slice
              len = message_bytes.size

              # Apply rate limiting based on queue state before sending
              if @adaptive_rate_limiting
                delay = @rate_limiter.apply_rate_limit
                if delay > 0
                  debug_log "Rate limiting applied: #{delay}ms delay, queue at #{@rate_limiter.status[:fill_percentage]}%"
                end
              end

              debug_log "Sending #{message.type} message to #{to} (#{len} bytes)"

              # Send length prefix
              len_bytes = Bytes.new(4)
              IO::ByteFormat::NetworkEndian.encode(len, len_bytes)
              socket.write(len_bytes)
              socket.write(message_bytes)
              socket.flush

              # Record successful send in metrics
              @rate_limiter.metrics.record_message_sent

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
            # Simple connection test - just check if socket is still connected
            # Avoid sending test bytes that break the protocol
            debug_log "Checking existing connection to #{node_id}"
            
            # Check if socket is closed
            if socket.closed?
              raise Socket::Error.new("Socket is closed")
            end
            
            # Try to get socket info to see if it's still valid
            socket.remote_address
            
            debug_log "Connection to #{node_id} appears alive, reusing it"
            return socket
          rescue ex
            debug_log "Existing connection to #{node_id} is dead: #{ex.message}"
            @connections_mutex.synchronize do
              @connections.delete(node_id)
            end
            socket.close rescue nil
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

            @connections_mutex.synchronize do
              @connections[node_id] = socket
            end
            
            # Spawn a reader fiber for the outgoing connection to make it bidirectional
            spawn handle_client(socket)
            
            debug_log "Successfully established connection to #{node_id} with reader fiber"
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

      # Get current rate limiter status
      def rate_limiter_status
        if @adaptive_rate_limiting
          @rate_limiter.status
        else
          {adaptive_rate_limiting: false}
        end
      end

      # Get metrics summary
      def metrics_summary : String
        @rate_limiter.metrics.summary
      end

      # Get detailed metrics
      def metrics : Hash
        @rate_limiter.metrics.detailed_metrics
      end

      # Adjust rate limiter configuration
      def configure_rate_limiter(
        threshold_pct : Float64? = nil,
        base_delay_ms : Int32? = nil,
        max_delay_ms : Int32? = nil,
        adaptive_curve : Float64? = nil,
      )
        @rate_limiter.threshold_pct = threshold_pct if threshold_pct
        @rate_limiter.base_delay_ms = base_delay_ms if base_delay_ms
        @rate_limiter.max_delay_ms = max_delay_ms if max_delay_ms
        @rate_limiter.adaptive_curve = adaptive_curve if adaptive_curve

        # Return current configuration
        {
          threshold_pct:          @rate_limiter.threshold_pct,
          base_delay_ms:          @rate_limiter.base_delay_ms,
          max_delay_ms:           @rate_limiter.max_delay_ms,
          adaptive_curve:         @rate_limiter.adaptive_curve,
          adaptive_rate_limiting: @adaptive_rate_limiting,
        }
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
