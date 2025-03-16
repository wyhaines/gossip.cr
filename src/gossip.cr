require "set"
require "socket"
require "json"

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

  def initialize(@sender, @message_id)
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
          if @connections.has_key?(remote_id)
            puts "Already have connection for #{remote_id}, closing old one"
            @connections[remote_id].close
          end
          puts "Storing new connection for #{remote_id}"
          @connections[remote_id] = client
          # Update message sender before queuing
          message = Join.new(sender)
          @message_queue.send(message)
          puts "Queued Join message from #{remote_id}"
        else
          if remote_id.empty?
            remote_id = sender
            if @connections.has_key?(remote_id)
              puts "Already have connection for #{remote_id}, closing old one"
              @connections[remote_id].close
            end
            puts "Storing new connection for #{remote_id}"
            @connections[remote_id] = client
          end
          @message_queue.send(message)
          puts "Queued #{message.type} message from #{remote_id}"
        end
      end
    rescue ex : IO::Error | Socket::Error
      # Only log connection errors if we're still running
      # This prevents noise during shutdown
      if @running
        puts "Connection error with #{remote_id}: #{ex.message}"
      end
      @connections.delete(remote_id) unless remote_id.empty?
      client.close
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
    else
      raise "Unknown message type: #{msg_data["type"]}"
    end
  end

  # Send a message to another node with retries
  def send_message(to : String, message : Message)
    remaining_attempts = 3

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
          return # No point retrying if we can't establish connection
        end
      rescue ex : IO::Error | Socket::Error
        remaining_attempts -= 1
        if remaining_attempts > 0 && @running
          @connections.delete(to)
          puts "Failed to send message to #{to}, retrying... (#{remaining_attempts} attempts left)"
        elsif @running
          puts "Failed to send message to #{to} after all retries: #{ex.message}"
          @connections.delete(to)
        end
      end
    end
  end

  # Get existing connection or create new one with proper handshake
  private def get_or_create_connection(node_id : String) : TCPSocket?
    # Check if we have a valid existing connection
    if socket = @connections[node_id]?
      begin
        # Test if connection is still alive
        puts "Testing connection to #{node_id}"
        test_bytes = Bytes.new(4, 0_u8) # Four zero bytes for test
        socket.write(test_bytes)
        socket.flush

        # Wait for acknowledgment
        response = Bytes.new(4)
        bytes_read = socket.read_fully?(response)
        if !bytes_read || !response.all? { |b| b == 0 }
          puts "Invalid test response from #{node_id}"
          raise Socket::Error.new("Invalid test response")
        end

        return socket
      rescue ex
        puts "Existing connection to #{node_id} is dead: #{ex.message}"
        @connections.delete(node_id)
      end
    end

    if node_id =~ /(.+)@(.+):(\d+)/
      id, host, port = $1, $2, $3.to_i
      begin
        puts "Creating new connection to #{node_id}"
        socket = TCPSocket.new(host, port)
        socket.tcp_nodelay = true          # Disable Nagle's algorithm
        socket.keepalive = true            # Enable TCP keepalive
        socket.tcp_keepalive_idle = 60     # Start probing after 60 seconds of inactivity
        socket.tcp_keepalive_interval = 10 # Send probes every 10 seconds
        socket.tcp_keepalive_count = 3     # Drop connection after 3 failed probes

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

        @connections[node_id] = socket
        puts "Successfully established connection to #{node_id}"
        return socket
      rescue ex : Socket::Error
        puts "Failed to connect to #{node_id}: #{ex.message}"
        return nil
      end
    end
    nil
  end

  # Clean up resources
  def close
    @running = false
    @message_queue.close
    @connections.each_value(&.close)
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
  property missing_messages : Set(String)
  property lazy_push_probability : Float64
  property network : NetworkNode
  property failed_nodes : Set(String)

  # Configuration constants
  MAX_ACTIVE       =   5 # Max size of active view
  MAX_PASSIVE      =  10 # Max size of passive view
  TTL              =   2 # Time-to-live for ForwardJoin
  SHUFFLE_INTERVAL = 5.0 # Seconds between shuffles
  SHUFFLE_SIZE     =   3 # Number of nodes to exchange in shuffle
  MIN_ACTIVE       =   2 # Min nodes to send for new node's active view
  MIN_PASSIVE      =   3 # Min nodes to send for new node's passive view
  LAZY_PUSH_PROB   = 0.1 # Reduced probability for lazy push to improve propagation speed

  def initialize(id : String, network : NetworkNode)
    @id = id
    @network = network
    @active_view = Set(String).new
    @passive_view = Set(String).new
    @received_messages = Set(String).new
    @message_contents = Hash(String, String).new
    @missing_messages = Set(String).new
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
  end

  # Modified send_message to use network layer
  def send_message(to : String, message : Message)
    @network.send_message(to, message)
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
    else
      puts "Node #{@id}: Unknown message type"
    end
  end

  # New method to maintain views and handle disconnected nodes
  private def maintain_views
    # Test connections to active view nodes
    failed_nodes = Set(String).new
    @active_view.each do |node|
      begin
        # Try to send a test message
        send_message(node, Join.new(@id))
      rescue ex
        failed_nodes << node
      end
    end

    # Remove failed nodes from active view
    failed_nodes.each do |node|
      @active_view.delete(node)
      @failed_nodes << node
      puts "Node #{@id}: Removed failed node #{node} from active view"
    end

    # Try to promote nodes from passive view if active view is low
    while @active_view.size < MIN_ACTIVE && !@passive_view.empty?
      candidate = @passive_view.to_a.sample
      @passive_view.delete(candidate)

      begin
        # Try to establish connection
        join_msg = Join.new(@id)
        send_message(candidate, join_msg)
        @active_view << candidate
        puts "Node #{@id}: Promoted #{candidate} from passive to active view"
      rescue ex
        @failed_nodes << candidate
        puts "Node #{@id}: Failed to promote #{candidate} - node unreachable"
      end
    end
  end

  # Handle a new node joining via this node
  def handle_join(message : Join)
    sender = message.sender
    puts "Node #{@id}: Received JOIN from #{sender}"

    # Remove from failed nodes if it was there
    @failed_nodes.delete(sender)

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

    # Send our views to the new node
    active_nodes = @active_view.to_a.reject { |n| n == sender || @failed_nodes.includes?(n) }
    passive_nodes = @passive_view.to_a.reject { |n| @failed_nodes.includes?(n) }
    init_msg = InitViews.new(@id, active_nodes, passive_nodes)
    send_message(sender, init_msg)
    puts "Node #{@id}: Sent InitViews to #{sender}"

    # Propagate join to some nodes in our active view
    forward_count = Math.min(@active_view.size - 1, 2) # Forward to at most 2 other nodes
    if forward_count > 0
      targets = @active_view.to_a.reject { |n| n == sender || @failed_nodes.includes?(n) }.sample(forward_count)
      targets.each do |target|
        forward_msg = ForwardJoin.new(@id, sender, TTL)
        send_message(target, forward_msg)
        puts "Node #{@id}: Forwarded join from #{sender} to #{target}"
      end
    end
  end

  # Handle propagation of a join message
  def handle_forward_join(message : ForwardJoin)
    new_node = message.new_node
    ttl = message.ttl
    if ttl > 0
      @active_view.each do |node|
        if node != message.sender
          forward_msg = ForwardJoin.new(@id, new_node, ttl - 1)
          send_message(node, forward_msg)
        end
      end
    else
      @passive_view << new_node unless @active_view.includes?(new_node) || new_node == @id
      puts "Node #{@id}: Added #{new_node} to passive view"
    end
  end

  # Handle an incoming shuffle request
  def handle_shuffle(message : Shuffle)
    sender = message.sender
    received_nodes = message.nodes
    own_nodes = (@active_view | @passive_view).to_a.sample([SHUFFLE_SIZE, (@active_view | @passive_view).size].min)
    reply_msg = ShuffleReply.new(@id, own_nodes)
    send_message(sender, reply_msg)
    received_nodes.each do |node|
      if node != @id && !@active_view.includes?(node) && @passive_view.size < MAX_PASSIVE
        @passive_view << node
      end
    end
    puts "Node #{@id}: Shuffled with #{sender}"
  end

  # Handle a shuffle reply
  def handle_shuffle_reply(message : ShuffleReply)
    received_nodes = message.nodes
    received_nodes.each do |node|
      if node != @id && !@active_view.includes?(node) && @passive_view.size < MAX_PASSIVE
        @passive_view << node
      end
    end
  end

  # Initialize views for a new node
  def handle_init_views(message : InitViews)
    sender = message.sender
    puts "Node #{@id}: Received InitViews from #{sender}"

    # Add sender to our active view if not already present
    if !@active_view.includes?(sender)
      @active_view << sender
      puts "Node #{@id}: Added #{sender} to active view"
    end

    # Process suggested active nodes
    message.active_nodes.each do |node|
      next if node == @id || @active_view.includes?(node)
      if @active_view.size < MAX_ACTIVE
        # Try to establish bidirectional connection by sending a join
        join_msg = Join.new(@id)
        send_message(node, join_msg)
        puts "Node #{@id}: Sent Join to suggested active node #{node}"
      else
        # Add to passive view if not full
        @passive_view << node if @passive_view.size < MAX_PASSIVE
        puts "Node #{@id}: Added suggested node #{node} to passive view"
      end
    end

    # Add passive nodes
    message.passive_nodes.each do |node|
      next if node == @id || @active_view.includes?(node)
      if @passive_view.size < MAX_PASSIVE
        @passive_view << node
        puts "Node #{@id}: Added #{node} to passive view"
      end
    end

    puts "Node #{@id}: Initialized views - Active: #{@active_view.to_a}, Passive: #{@passive_view.to_a}"
  end

  # Handle a broadcast message (Plumtree eager/lazy push)
  def handle_broadcast(message : BroadcastMessage)
    if !@received_messages.includes?(message.message_id)
      @received_messages << message.message_id
      @message_contents[message.message_id] = message.content

      puts "Node #{@id}: Received broadcast '#{message.content}' from #{message.sender}"

      # Forward immediately to all active view nodes except sender
      @active_view.each do |node|
        next if node == message.sender || @failed_nodes.includes?(node)

        begin
          # Reduced lazy push probability for faster propagation
          if rand < @lazy_push_probability
            lazy_msg = LazyPushMessage.new(@id, message.message_id)
            send_message(node, lazy_msg)
            puts "Node #{@id}: Sent lazy push for message to #{node}"
          else
            forward_msg = BroadcastMessage.new(@id, message.message_id, message.content)
            send_message(node, forward_msg)
            puts "Node #{@id}: Forwarded broadcast to #{node}"
          end
        rescue ex
          @failed_nodes << node
          @active_view.delete(node)
          puts "Node #{@id}: Failed to forward to #{node} - removing from active view"
        end
      end
    end
  end

  # Handle a lazy push notification
  def handle_lazy_push(message : LazyPushMessage)
    message_id = message.message_id
    if !@received_messages.includes?(message_id)
      @missing_messages << message_id
      # Request the missing message
      request_msg = MessageRequest.new(@id, message_id)
      send_message(message.sender, request_msg)
    end
  end

  # Handle a message request
  def handle_message_request(message : MessageRequest)
    message_id = message.message_id
    if content = @message_contents[message_id]?
      response_msg = MessageResponse.new(@id, message_id, content)
      send_message(message.sender, response_msg)
    end
  end

  # Handle a message response
  def handle_message_response(message : MessageResponse)
    message_id = message.message_id
    if @missing_messages.includes?(message_id)
      @missing_messages.delete(message_id)
      @received_messages << message_id
      @message_contents[message_id] = message.content

      # Forward to active view with eager push
      @active_view.each do |node|
        next if node == message.sender
        forward_msg = BroadcastMessage.new(@id, message_id, message.content)
        send_message(node, forward_msg)
      end
    end
  end

  # Send a shuffle message to a random node
  def send_shuffle
    all_nodes = @active_view | @passive_view
    if all_nodes.size > 0
      target = all_nodes.to_a.sample
      shuffle_nodes = all_nodes.to_a.sample([SHUFFLE_SIZE, all_nodes.size].min)
      shuffle_msg = Shuffle.new(@id, shuffle_nodes)
      send_message(target, shuffle_msg)
    end
  end

  # Initiate a broadcast
  def broadcast(content : String)
    message_id = "#{@id}-#{Time.utc.to_unix_ms}-#{rand(1000)}"
    msg = BroadcastMessage.new(@id, message_id, content)

    # Mark as received by us
    @received_messages << message_id
    @message_contents[message_id] = content

    puts "Node #{@id}: Broadcasting message '#{content}'"

    # Send to all active view members
    @active_view.each do |node|
      next if @failed_nodes.includes?(node)
      begin
        # Use eager push for initial broadcast to speed up propagation
        send_message(node, msg)
        puts "Node #{@id}: Sent broadcast to #{node}"
      rescue ex
        @failed_nodes << node
        @active_view.delete(node)
        puts "Node #{@id}: Failed to send to #{node} - removing from active view"
      end
    end
  end
end
