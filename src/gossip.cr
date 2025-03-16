require "set"
require "socket"
require "json"
require "mutex"

# Abstract base struct for all messages
abstract struct Message
  include JSON::Serializable
  property sender : String
  property type : String

  def initialize(@sender)
    @type = self.class.name
  end
end

# Message for a new node joining the network
struct Join < Message
end

# Message to propagate join information
struct ForwardJoin < Message
  property new_node : String
  property ttl : Int32

  def initialize(@sender, @new_node, @ttl)
    super(@sender)
  end
end

# Message for view maintenance via shuffling
struct Shuffle < Message
  property nodes : Array(String)

  def initialize(@sender, nodes : Array(String))
    super(@sender)
    @nodes = nodes
  end
end

# Response to a shuffle message
struct ShuffleReply < Message
  property nodes : Array(String)

  def initialize(@sender, nodes : Array(String))
    super(@sender)
    @nodes = nodes
  end
end

# Message to initialize a new node's views
struct InitViews < Message
  property active_nodes : Array(String)
  property passive_nodes : Array(String)

  def initialize(@sender, @active_nodes, @passive_nodes)
    super(@sender)
  end
end

# Message for broadcasting content (Plumtree)
struct BroadcastMessage < Message
  property message_id : String
  property content : String

  def initialize(@sender, @message_id, @content)
    super(@sender)
  end
end

# Message for lazy push notification (Plumtree)
struct LazyPushMessage < Message
  property message_id : String

  def initialize(@sender, @message_id)
    super(@sender)
  end
end

# Message to request missing content (Plumtree)
struct MessageRequest < Message
  property message_id : String
  property from_lazy_push : Bool

  def initialize(@sender, @message_id, @from_lazy_push = true)
    super(@sender)
  end
end

# Message to respond with missing content (Plumtree)
struct MessageResponse < Message
  property message_id : String
  property content : String

  def initialize(@sender, @message_id, @content)
    super(@sender)
  end
end

# New message type for heartbeats
struct Heartbeat < Message
end

# New message type for heartbeat acknowledgments
struct HeartbeatAck < Message
end

# NodeAddress represents a network location
struct NodeAddress
  include JSON::Serializable
  property host : String
  property port : Int32
  property id : String

  def initialize(@host, @port, @id)
  end

  def to_s
    "#{@id}@#{@host}:#{@port}"
  end
end

# NetworkNode handles TCP communication between nodes
class NetworkNode
  property address : NodeAddress
  property server : TCPServer
  property connections : Hash(String, TCPSocket)
  property message_queue : Channel(Message)
  property running : Bool
  @node : Node? = nil
  @connections_mutex = Mutex.new
  
  # Configuration settings
  SOCKET_TIMEOUT     = 5.0  # Seconds
  SOCKET_READ_TIMEOUT = 10.0 # Seconds for read operations
  CONNECTION_RETRIES = 3     # Number of retries for send operations

  def initialize(@address)
    @server = TCPServer.new(@address.host, @address.port)
    @connections = Hash(String, TCPSocket).new
    @message_queue = Channel(Message).new(100) # Buffered channel to prevent blocking
    @running = true

    # Start accepting connections
    spawn do
      while @running
        if client = @server.accept?
          puts "Accepted new connection"
          spawn handle_client(client)
        end
      end
    end

    # Start processing messages
    spawn do
      while @running
        begin
          puts "Waiting for message on queue..."
          message = @message_queue.receive
          puts "Processing #{message.type} message from #{message.sender}"
          if node = @node
            node.handle_message(message)
          else
            puts "Warning: No node set to handle message"
          end
        rescue ex : Channel::ClosedError
          break
        end
      end
    end
  end

  def node=(node : Node)
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
        puts "Received #{message.type} message from #{message.sender}"

        # Clean up the sender ID if it's malformed
        sender = message.sender
        if sender =~ /(.+@.+:\d+)@.+:\d+/
          sender = $1
          puts "Cleaned up malformed sender ID from #{message.sender} to #{sender}"
        end

        case message
        when Join
          remote_id = sender
          # Store connection for future use if we don't already have one
          @connections_mutex.synchronize do
            if @connections.has_key?(remote_id)
              puts "Already have connection for #{remote_id}, closing old one"
              @connections[remote_id].close rescue nil
            end
            puts "Storing new connection for #{remote_id}"
            @connections[remote_id] = client
          end
          # Update message sender before queuing
          message = Join.new(sender)
          @message_queue.send(message)
          puts "Queued Join message from #{remote_id}"
        else
          if remote_id.empty?
            remote_id = sender
            @connections_mutex.synchronize do
              if @connections.has_key?(remote_id)
                puts "Already have connection for #{remote_id}, closing old one"
                @connections[remote_id].close rescue nil
              end
              puts "Storing new connection for #{remote_id}"
              @connections[remote_id] = client
            end
          end
          @message_queue.send(message)
          puts "Queued #{message.type} message from #{remote_id}"
        end
      end
    rescue ex : IO::Error | Socket::Error | IO::TimeoutError
      # Only log connection errors if we're still running
      # This prevents noise during shutdown
      if @running
        puts "Connection error with #{remote_id}: #{ex.message}"
      end
      @connections_mutex.synchronize do
        @connections.delete(remote_id) unless remote_id.empty?
      end
      client.close rescue nil
    end
  end

  # Read a message from a socket with timeout
  private def read_message(socket : TCPSocket) : Message
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
    puts "Received message: #{message_json}" # Debug log

    # Parse message based on type
    msg_data = JSON.parse(message_json)
    case msg_data["type"].as_s
    when "Join"             then Join.from_json(message_json)
    when "ForwardJoin"      then ForwardJoin.from_json(message_json)
    when "Shuffle"          then Shuffle.from_json(message_json)
    when "ShuffleReply"     then ShuffleReply.from_json(message_json)
    when "InitViews"        then InitViews.from_json(message_json)
    when "BroadcastMessage" then BroadcastMessage.from_json(message_json)
    when "LazyPushMessage"  then LazyPushMessage.from_json(message_json)
    when "MessageRequest"   then MessageRequest.from_json(message_json)
    when "MessageResponse"  then MessageResponse.from_json(message_json)
    when "Heartbeat"        then Heartbeat.from_json(message_json)
    when "HeartbeatAck"     then HeartbeatAck.from_json(message_json)
    else
      raise "Unknown message type: #{msg_data["type"]}"
    end
  end

  # Send a message to another node with retries - NOW THROWS EXCEPTIONS when failures occur
  def send_message(to : String, message : Message, retry_count = CONNECTION_RETRIES)
    remaining_attempts = retry_count

    while remaining_attempts > 0 && @running
      begin
        if socket = get_or_create_connection(to)
          message_json = message.to_json
          message_bytes = message_json.to_slice
          len = message_bytes.size

          puts "Sending #{message.type} message to #{to} (#{len} bytes)"

          # Send length prefix
          len_bytes = Bytes.new(4)
          IO::ByteFormat::NetworkEndian.encode(len, len_bytes)
          socket.write(len_bytes)
          socket.write(message_bytes)
          socket.flush
          return # Success, exit the loop
        else
          if @running
            puts "Failed to connect to node #{to}"
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
          puts "Failed to send message to #{to}, retrying... (#{remaining_attempts} attempts left): #{ex.message}"
        elsif @running
          puts "Failed to send message to #{to} after all retries: #{ex.message}"
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
        puts "Testing connection to #{node_id}"
        test_bytes = Bytes.new(4, 0_u8) # Four zero bytes for test
        socket.write(test_bytes)
        socket.flush

        # Wait for acknowledgment with timeout
        socket.read_timeout = SOCKET_TIMEOUT.seconds
        response = Bytes.new(4)
        bytes_read = socket.read_fully?(response)
        if !bytes_read || !response.all? { |b| b == 0 }
          puts "Invalid test response from #{node_id}"
          raise Socket::Error.new("Invalid test response")
        end

        return socket
      rescue ex
        puts "Existing connection to #{node_id} is dead: #{ex.message}"
        @connections_mutex.synchronize do
          @connections.delete(node_id)
        end
      end
    end

    if node_id =~ /(.+)@(.+):(\d+)/
      id, host, port = $1, $2, $3.to_i
      begin
        puts "Creating new connection to #{node_id}"
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
        join_msg = Join.new(@node.not_nil!.id)
        message_json = join_msg.to_json
        message_bytes = message_json.to_slice
        len = message_bytes.size

        puts "Sending initial Join message (#{len} bytes): #{message_json}"
        len_bytes = Bytes.new(4)
        IO::ByteFormat::NetworkEndian.encode(len, len_bytes)
        socket.write(len_bytes)
        socket.write(message_bytes)
        socket.flush

        @connections_mutex.synchronize do
          @connections[node_id] = socket
        end
        puts "Successfully established connection to #{node_id}"
        return socket
      rescue ex : Socket::Error | IO::TimeoutError
        puts "Failed to connect to #{node_id}: #{ex.message}"
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
      puts "Connection test to #{node_id} failed: #{ex.message}"
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

# Modified Node class to work with NetworkNode
class Node
  property id : String
  property active_view : Set(String)
  property passive_view : Set(String)
  property received_messages : Set(String)
  property message_contents : Hash(String, String)
  property missing_messages : Hash(String, Array(String)) # Track missing messages with potential providers
  property lazy_push_probability : Float64
  property network : NetworkNode
  property failed_nodes : Set(String)
  
  # Mutexes for thread safety
  @views_mutex = Mutex.new
  @messages_mutex = Mutex.new
  @failures_mutex = Mutex.new

  # Configuration constants
  MAX_ACTIVE         =  5 # Max size of active view
  MAX_PASSIVE        = 10 # Max size of passive view
  TTL                =  2 # Time-to-live for ForwardJoin
  SHUFFLE_INTERVAL   = 5.0 # Seconds between shuffles
  SHUFFLE_SIZE       =  3 # Number of nodes to exchange in shuffle
  MIN_ACTIVE         =  2 # Min nodes to send for new node's active view
  MIN_PASSIVE        =  3 # Min nodes to send for new node's passive view
  LAZY_PUSH_PROB     = 0.05 # REDUCED probability for lazy push to improve propagation speed
  HEARTBEAT_INTERVAL = 2.0 # Seconds between heartbeats
  MAX_REQUEST_ATTEMPTS = 3 # Maximum number of times to request a missing message
  REQUEST_RETRY_INTERVAL = 1.0 # Seconds between retries for message requests

  def initialize(id : String, network : NetworkNode)
    @id = id
    @network = network
    @active_view = Set(String).new
    @passive_view = Set(String).new
    @received_messages = Set(String).new
    @message_contents = Hash(String, String).new
    @missing_messages = Hash(String, Array(String)).new
    @lazy_push_probability = LAZY_PUSH_PROB
    @failed_nodes = Set(String).new

    # Track pending message requests to avoid duplicates
    @pending_requests = Set(String).new
    @pending_requests_mutex = Mutex.new

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
  def send_message(to : String, message : Message)
    begin
      @network.send_message(to, message)
    rescue ex : IO::Error | Socket::Error | IO::TimeoutError
      puts "Node #{@id}: Error sending #{message.type} to #{to}: #{ex.message}"
      handle_node_failure(to)
      raise ex # Re-raise to allow caller to handle
    end
  end

  # Send heartbeats to all active nodes
  private def send_heartbeats
    active_nodes = [] of String
    @views_mutex.synchronize do
      active_nodes = @active_view.to_a
    end
    
    failed_nodes = [] of String
    
    active_nodes.each do |node|
      begin
        heartbeat = Heartbeat.new(@id)
        send_message(node, heartbeat)
      rescue ex
        puts "Node #{@id}: Heartbeat to #{node} failed: #{ex.message}"
        failed_nodes << node
      end
    end
    
    # Process failed nodes outside the loop to avoid modifying while iterating
    failed_nodes.each do |node|
      handle_node_failure(node)
    end
  end

  # Handle node failures
  private def handle_node_failure(node : String)
    @views_mutex.synchronize do
      if @active_view.includes?(node)
        @active_view.delete(node)
        puts "Node #{@id}: Removed failed node #{node} from active view"
        
        @failures_mutex.synchronize do
          @failed_nodes << node
        end
        
        # Try to promote a node from passive view
        promote_passive_node
      end
    end
  end
  
  # Promote a node from passive to active view
  private def promote_passive_node
    @views_mutex.synchronize do
      return if @active_view.size >= MIN_ACTIVE || @passive_view.empty?
      
      # Try nodes from passive view until one works
      passive_nodes = @passive_view.to_a.shuffle
      
      passive_nodes.each do |candidate|
        @passive_view.delete(candidate)
        
        begin
          # Try to establish connection
          join_msg = Join.new(@id)
          send_message(candidate, join_msg)
          @active_view << candidate
          puts "Node #{@id}: Promoted #{candidate} from passive to active view"
          break # Exit once we successfully promote one node
        rescue ex
          @failures_mutex.synchronize do
            @failed_nodes << candidate
          end
          puts "Node #{@id}: Failed to promote #{candidate}: #{ex.message}"
        end
      end
    end
  end

  # Handle incoming messages based on their type
  def handle_message(message : Message)
    case message
    when Join
      handle_join(message)
    when ForwardJoin
      handle_forward_join(message)
    when Shuffle
      handle_shuffle(message)
    when ShuffleReply
      handle_shuffle_reply(message)
    when InitViews
      handle_init_views(message)
    when BroadcastMessage
      handle_broadcast(message)
    when LazyPushMessage
      handle_lazy_push(message)
    when MessageRequest
      handle_message_request(message)
    when MessageResponse
      handle_message_response(message)
    when Heartbeat
      handle_heartbeat(message)
    when HeartbeatAck
      handle_heartbeat_ack(message)
    else
      puts "Node #{@id}: Unknown message type"
    end
  end
  
  # Handle heartbeat message
  private def handle_heartbeat(message : Heartbeat)
    # Send acknowledgment
    ack = HeartbeatAck.new(@id)
    begin
      send_message(message.sender, ack)
    rescue ex
      puts "Node #{@id}: Failed to send heartbeat ack to #{message.sender}: #{ex.message}"
    end
  end
  
  # Handle heartbeat acknowledgment
  private def handle_heartbeat_ack(message : HeartbeatAck)
    # Node is alive, nothing to do
  end

  # New method to maintain views and handle disconnected nodes
  private def maintain_views
    # Get a snapshot of active view to avoid concurrent modification
    active_nodes = [] of String
    @views_mutex.synchronize do
      active_nodes = @active_view.to_a
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

    # Try to promote nodes from passive view if active view is low
    @views_mutex.synchronize do
      if @active_view.size < MIN_ACTIVE
        promote_passive_node
      end
    end
  end

  # Handle a new node joining via this node
  def handle_join(message : Join)
    sender = message.sender
    puts "Node #{@id}: Received JOIN from #{sender}"

    # Remove from failed nodes if it was there
    @failures_mutex.synchronize do
      @failed_nodes.delete(sender)
    end

    # Use mutex for thread safety
    @views_mutex.synchronize do
      # If we're already connected, just update views
      if @active_view.includes?(sender)
        puts "Node #{@id}: Already connected to #{sender}"
        return
      end

      # Add to active view if there's space
      if @active_view.size < MAX_ACTIVE
        @active_view << sender
        puts "Node #{@id}: Added #{sender} to active view"
      else
        # Move random node to passive view
        displaced = @active_view.to_a.sample
        @active_view.delete(displaced)
        @passive_view << displaced unless displaced == sender || @failed_nodes.includes?(displaced)
        @active_view << sender
        puts "Node #{@id}: Displaced #{displaced} to passive view"
      end
    end

    # Get view snapshots for thread safety
    active_nodes = [] of String
    passive_nodes = [] of String
    @views_mutex.synchronize do
      # Using to_a.reject to create new arrays for sending
      active_nodes = @active_view.to_a.reject { |n| n == sender || @failed_nodes.includes?(n) }
      passive_nodes = @passive_view.to_a.reject { |n| @failed_nodes.includes?(n) }
    end

    # Send our views to the new node
    begin
      init_msg = InitViews.new(@id, active_nodes, passive_nodes)
      send_message(sender, init_msg)
      puts "Node #{@id}: Sent InitViews to #{sender}"
    rescue ex
      puts "Node #{@id}: Failed to send InitViews to #{sender}: #{ex.message}"
      handle_node_failure(sender)
      return
    end

    # Propagate join to some nodes in our active view
    active_nodes_snapshot = [] of String
    @views_mutex.synchronize do
      active_nodes_snapshot = @active_view.to_a.reject { |n| n == sender || @failed_nodes.includes?(n) }
    end
    
    forward_count = Math.min(active_nodes_snapshot.size, 2) # Forward to at most 2 other nodes
    if forward_count > 0
      targets = active_nodes_snapshot.sample(forward_count)
      targets.each do |target|
        begin
          forward_msg = ForwardJoin.new(@id, sender, TTL)
          send_message(target, forward_msg)
          puts "Node #{@id}: Forwarded join from #{sender} to #{target}"
        rescue ex
          puts "Node #{@id}: Failed to forward join to #{target}: #{ex.message}"
          handle_node_failure(target)
        end
      end
    end
  end

  # Handle propagation of a join message
  def handle_forward_join(message : ForwardJoin)
    new_node = message.new_node
    ttl = message.ttl
    
    if ttl > 0
      # Get a snapshot of active view
      active_nodes = [] of String
      @views_mutex.synchronize do
        active_nodes = @active_view.to_a
      end
      
      active_nodes.each do |node|
        next if node == message.sender
        begin
          forward_msg = ForwardJoin.new(@id, new_node, ttl - 1)
          send_message(node, forward_msg)
        rescue ex
          puts "Node #{@id}: Failed to forward join to #{node}: #{ex.message}"
          handle_node_failure(node)
        end
      end
    else
      @views_mutex.synchronize do
        @passive_view << new_node unless @active_view.includes?(new_node) || new_node == @id
      end
      puts "Node #{@id}: Added #{new_node} to passive view"
    end
  end

  # Handle an incoming shuffle request
  def handle_shuffle(message : Shuffle)
    sender = message.sender
    received_nodes = message.nodes
    
    # Create a snapshot of combined views
    combined_view = [] of String
    @views_mutex.synchronize do
      combined_view = (@active_view | @passive_view).to_a
    end
    
    own_nodes = combined_view.sample([SHUFFLE_SIZE, combined_view.size].min)
    
    begin
      reply_msg = ShuffleReply.new(@id, own_nodes)
      send_message(sender, reply_msg)
      
      # Update passive view with received nodes
      @views_mutex.synchronize do
        received_nodes.each do |node|
          if node != @id && !@active_view.includes?(node) && @passive_view.size < MAX_PASSIVE
            @passive_view << node
          end
        end
      end
      puts "Node #{@id}: Shuffled with #{sender}"
    rescue ex
      puts "Node #{@id}: Failed to send shuffle reply to #{sender}: #{ex.message}"
      handle_node_failure(sender)
    end
  end

  # Handle a shuffle reply
  def handle_shuffle_reply(message : ShuffleReply)
    received_nodes = message.nodes
    
    @views_mutex.synchronize do
      received_nodes.each do |node|
        if node != @id && !@active_view.includes?(node) && @passive_view.size < MAX_PASSIVE
          @passive_view << node
        end
      end
    end
  end

  # Initialize views for a new node
  def handle_init_views(message : InitViews)
    sender = message.sender
    puts "Node #{@id}: Received InitViews from #{sender}"

    # Add sender to our active view if not already present
    @views_mutex.synchronize do
      if !@active_view.includes?(sender)
        @active_view << sender
        puts "Node #{@id}: Added #{sender} to active view"
      end
    end

    # Process suggested active nodes
    message.active_nodes.each do |node|
      next if node == @id
      
      @views_mutex.synchronize do
        next if @active_view.includes?(node)
        
        if @active_view.size < MAX_ACTIVE
          # Try to establish bidirectional connection by sending a join
          begin
            join_msg = Join.new(@id)
            send_message(node, join_msg)
            puts "Node #{@id}: Sent Join to suggested active node #{node}"
          rescue ex
            puts "Node #{@id}: Failed to send Join to suggested node #{node}: #{ex.message}"
            @failures_mutex.synchronize do
              @failed_nodes << node
            end
          end
        else
          # Add to passive view if not full
          if @passive_view.size < MAX_PASSIVE
            @passive_view << node
            puts "Node #{@id}: Added suggested node #{node} to passive view"
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
          puts "Node #{@id}: Added #{node} to passive view"
        end
      end
    end

    puts "Node #{@id}: Initialized views - Active: #{@active_view.to_a}, Passive: #{@passive_view.to_a}"
  end

  # Handle a broadcast message (Plumtree eager/lazy push)
  def handle_broadcast(message : BroadcastMessage)
    message_id = message.message_id
    sender = message.sender
    
    # Use mutex for thread safety
    already_received = false
    @messages_mutex.synchronize do
      already_received = @received_messages.includes?(message_id)
      unless already_received
        @received_messages << message_id
        @message_contents[message_id] = message.content
      end
    end
    
    if !already_received
      puts "Node #{@id}: Received broadcast '#{message.content}' from #{sender}"

      # Get active view snapshot
      active_nodes = [] of String
      @views_mutex.synchronize do
        active_nodes = @active_view.to_a
      end

      # Forward immediately to all active view nodes except sender
      active_nodes.each do |node|
        next if node == sender
        
        @failures_mutex.synchronize do
          next if @failed_nodes.includes?(node)
        end

        begin
          # Reduced lazy push probability for faster propagation
          if rand < @lazy_push_probability
            lazy_msg = LazyPushMessage.new(@id, message_id)
            send_message(node, lazy_msg)
            puts "Node #{@id}: Sent lazy push for message to #{node}"
          else
            forward_msg = BroadcastMessage.new(@id, message_id, message.content)
            send_message(node, forward_msg)
            puts "Node #{@id}: Forwarded broadcast to #{node}"
          end
        rescue ex
          puts "Node #{@id}: Failed to forward broadcast to #{node}: #{ex.message}"
          handle_node_failure(node)
        end
      end
    end
  end

  # Handle a lazy push notification with improved recovery
  def handle_lazy_push(message : LazyPushMessage)
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
          request_msg = MessageRequest.new(@id, message_id, true)
          begin
            send_message(sender, request_msg)
            puts "Node #{@id}: Requested missing message #{message_id} from #{sender}"
          rescue ex
            puts "Node #{@id}: Failed to request message from #{sender}: #{ex.message}"
            # Don't remove from pending - will be retried by recovery process
            handle_node_failure(sender)
          end
        end
      end
    end
  end

  # Background fiber to handle message recovery
  private def handle_message_recovery
    while @network.running
      sleep(REQUEST_RETRY_INTERVAL)
      
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
            request_msg = MessageRequest.new(@id, message_id, false)
            send_message(provider, request_msg)
            success = true
            break # Successfully sent request
          rescue ex
            puts "Node #{@id}: Failed to request message recovery from #{provider}: #{ex.message}"
            handle_node_failure(provider)
          end
        end
        
        # If no providers worked, try someone from active view as last resort
        unless success
          active_nodes = [] of String
          @views_mutex.synchronize do
            active_nodes = @active_view.to_a
          end
          
          active_nodes.shuffle.each do |node|
            begin
              request_msg = MessageRequest.new(@id, message_id, false)
              send_message(node, request_msg)
              puts "Node #{@id}: Last resort message request to #{node}"
              break # Successfully sent request
            rescue ex
              puts "Node #{@id}: Failed last resort request to #{node}: #{ex.message}"
              handle_node_failure(node)
            end
          end
        end
      end
    end
  end

  # Handle a message request
  def handle_message_request(message : MessageRequest)
    message_id = message.message_id
    sender = message.sender
    
    # Get content if we have it
    content = nil
    @messages_mutex.synchronize do
      content = @message_contents[message_id]?
    end
    
    if content
      begin
        response_msg = MessageResponse.new(@id, message_id, content)
        send_message(sender, response_msg)
        puts "Node #{@id}: Sent message response for #{message_id} to #{sender}"
      rescue ex
        puts "Node #{@id}: Failed to send message response to #{sender}: #{ex.message}"
        handle_node_failure(sender)
      end
    else
      puts "Node #{@id}: Requested message #{message_id} not found for #{sender}"
    end
  end

  # Handle a message response
  def handle_message_response(message : MessageResponse)
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
      puts "Node #{@id}: Recovered missing message #{message_id}"
      
      # Get active view snapshot
      active_nodes = [] of String
      @views_mutex.synchronize do
        active_nodes = @active_view.to_a
      end
      
      # Forward to active view with eager push
      active_nodes.each do |node|
        next if node == message.sender
        
        @failures_mutex.synchronize do
          next if @failed_nodes.includes?(node)
        end
        
        begin
          forward_msg = BroadcastMessage.new(@id, message_id, content)
          send_message(node, forward_msg)
          puts "Node #{@id}: Forwarded recovered message to #{node}"
        rescue ex
          puts "Node #{@id}: Failed to forward recovered message to #{node}: #{ex.message}"
          handle_node_failure(node)
        end
      end
    end
  end

  # Send a shuffle message to a random node
  def send_shuffle
    # Get a snapshot of combined views
    all_nodes = [] of String
    @views_mutex.synchronize do
      all_nodes = (@active_view | @passive_view).to_a
    end
    
    if all_nodes.size > 0
      target = all_nodes.sample
      shuffle_nodes = all_nodes.sample([SHUFFLE_SIZE, all_nodes.size].min)
      
      begin
        shuffle_msg = Shuffle.new(@id, shuffle_nodes)
        send_message(target, shuffle_msg)
      rescue ex
        puts "Node #{@id}: Failed to send shuffle to #{target}: #{ex.message}"
        handle_node_failure(target)
      end
    end
  end

  # Initiate a broadcast
  def broadcast(content : String)
    message_id = "#{@id}-#{Time.utc.to_unix_ms}-#{rand(1000)}"
    msg = BroadcastMessage.new(@id, message_id, content)

    # Mark as received by us
    @messages_mutex.synchronize do
      @received_messages << message_id
      @message_contents[message_id] = content
    end

    puts "Node #{@id}: Broadcasting message '#{content}'"

    # Get active view snapshot
    active_nodes = [] of String
    @views_mutex.synchronize do
      active_nodes = @active_view.to_a
    end
    
    # Send to all active view members
    active_nodes.each do |node|
      @failures_mutex.synchronize do
        next if @failed_nodes.includes?(node)
      end
      
      begin
        # Use eager push for initial broadcast to speed up propagation
        send_message(node, msg)
        puts "Node #{@id}: Sent broadcast to #{node}"
      rescue ex
        puts "Node #{@id}: Failed to send broadcast to #{node}: #{ex.message}"
        handle_node_failure(node)
      end
    end
    
    # Return the message ID so applications can track it if needed
    message_id
  end
end
