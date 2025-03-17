require "../src/gossip"
require "option_parser"

# Basic node settings
node_role = "" # "bootstrap", "target" or "test"
port = 0
node_id = ""
bootstrap_address = "node1@localhost:7001" # Default bootstrap
wait_time = 10                             # Seconds to wait for ACKs
message_count = 5                          # Number of test messages to send
node_count = 5                             # Expected node count (including bootstrap)

# Parse command line options
OptionParser.parse do |parser|
  parser.banner = "Usage: crystal run network_test.cr [arguments]"

  parser.on("--role=ROLE", "Node role: bootstrap, target, or test") { |r| node_role = r }
  parser.on("--port=PORT", "Node port") { |p| port = p.to_i }
  parser.on("--id=ID", "Node ID") { |i| node_id = i }
  parser.on("--bootstrap=ADDR", "Bootstrap node address") { |b| bootstrap_address = b }
  parser.on("--wait=SEC", "Wait time for ACKs (test node only)") { |w| wait_time = w.to_i }
  parser.on("--messages=COUNT", "Number of test messages (test node only)") { |m| message_count = m.to_i }
  parser.on("--nodes=COUNT", "Expected node count (test node only)") { |n| node_count = n.to_i }
  parser.on("-h", "--help", "Show this help") do
    puts parser
    exit
  end
end

# Validate arguments
if node_role.empty? || port == 0 || node_id.empty?
  puts "Error: Required arguments missing"
  puts "Please specify --role, --port, and --id"
  exit 1
end

# Create a simple test node that extends Node
class SimpleTestNode < Node
  # Track original messages sent and ACKs received
  getter sent_messages = {} of String => String
  getter received_acks = {} of String => Set(String)
  getter node_role : String
  property expected_node_count : Int32
  property test_message_count : Int32
  property ack_wait_time : Int32
  
  # Mutex for safe access to ack tracking
  @ack_mutex = Mutex.new
  # Track last activity timestamp for dynamic timeout
  @last_ack_time : Time::Span = Time.monotonic
  
  def initialize(id : String, network : NetworkNode, @node_role : String,
                 @expected_node_count : Int32 = 5,
                 @test_message_count : Int32 = 5,
                 @ack_wait_time : Int32 = 10)
    super(id, network)
    log "Created node with role: #{@node_role}"
  end

  def log(message : String)
    puts "[#{@id}] #{message}"
  end

  # Handle incoming broadcasts with improved ACK tracking
  def handle_broadcast(message : BroadcastMessage)
    message_id = message.message_id
    content = message.content
    sender = message.sender

    # Check if we've already seen this message
    already_received = false
    @messages_mutex.synchronize do
      already_received = @received_messages.includes?(message_id)
      unless already_received
        @received_messages << message_id
        @message_contents[message_id] = content
      end
    end

    # Only process if this is a new message
    if !already_received
      log "Received: #{content} from #{sender} (message_id: #{message_id})"

      if @node_role != "test" && !content.starts_with?("ACK ")
        begin
          # Send ACK with our node ID embedded in the content to preserve sender identity
          ack_content = "ACK #{content} FROM #{@id}"
          log "Sending ACK: #{ack_content}"
          broadcast(ack_content)
        rescue ex
          log "Error sending ACK: #{ex.message}"
        end
      end

      # Test node: record ACKs received with proper sender tracking
      if @node_role == "test" && content.starts_with?("ACK ")
        original_content = content[4..]

        # Extract sender information if present
        real_sender = sender
        if original_content.includes?(" FROM ")
          parts = original_content.split(" FROM ", 2)
          original_content = parts[0]
          real_sender = parts[1]
          log "Extracted real sender: #{real_sender}"
        end

        log "Processing ACK for original content: '#{original_content}'"

        # Format is now "ID||CONTENT" so we split on "||"
        if original_content.includes?("||")
          parts = original_content.split("||", 2)
          original_id = parts[0]
          
          # Safely update ACK tracking with mutex
          @ack_mutex.synchronize do
            if @sent_messages.has_key?(original_id)
              log "MATCH FOUND! Original message ID: #{original_id} matches sent message"
              @received_acks[original_id] ||= Set(String).new
              @received_acks[original_id] << real_sender
              log "Added ACK from #{real_sender} for message #{original_id}"
              log "Current ACK count for #{original_id}: #{@received_acks[original_id].size}"
              
              # Update last activity timestamp whenever we receive an ACK
              @last_ack_time = Time.monotonic
            else
              log "WARNING: No matching sent message found for ID: #{original_id}"
            end
          end
        else
          log "WARNING: ACK content doesn't contain expected '||' delimiter: #{original_content}"
        end
      end

      # Forward message to active view
      forward_to_active_view(message)
    end
  end

  def log_message_status
    log "\n=== MESSAGE STATUS ===\n"
    
    # Use mutex for thread-safe access
    @ack_mutex.synchronize do
      log "Sent messages (#{@sent_messages.size}):"
      @sent_messages.each do |id, content|
        log "  #{id} -> #{content}"
      end

      log "\nReceived ACKs (#{@received_acks.size} message ids):"
      @received_acks.each do |id, senders|
        log "  #{id} -> #{senders.size} ACKs from: #{senders.to_a.join(", ")}"
      end
    end

    @views_mutex.synchronize do
      log "\nActive view (#{@active_view.size} nodes):"
      log "  #{@active_view.to_a.join(", ")}"

      log "\nPassive view (#{@passive_view.size} nodes):"
      log "  #{@passive_view.to_a.join(", ")}"
    end
    
    log "=====================\n"
  end

  # Forward message to active view (except sender)
  private def forward_to_active_view(message : BroadcastMessage)
    active_nodes = [] of String
    @views_mutex.synchronize do
      active_nodes = @active_view.to_a
    end

    active_nodes.each do |node|
      next if node == message.sender

      @failures_mutex.synchronize do
        next if @failed_nodes.includes?(node)
      end

      begin
        # Direct forward - no lazy push for test application
        forward_msg = BroadcastMessage.new(@id, message.message_id, message.content)
        send_message(node, forward_msg)
      rescue ex
        log "Failed to forward to #{node}: #{ex.message}"
        handle_node_failure(node)
      end
    end
  end

  # Send a test message
  def send_test_message(content : String) : String
    message_id = "#{@id}-#{Time.utc.to_unix_ms}-#{rand(10000)}"
    # Use double pipe (||) as delimiter between ID and content
    full_content = "#{message_id}||#{content}"

    # Store message ID for tracking ACKs
    @ack_mutex.synchronize do
      @sent_messages[message_id] = content
    end
    
    log "Stored message ID for tracking: #{message_id} -> #{content}"
    log "Sending test message: #{content} (ID: #{message_id})"
    broadcast(full_content)

    return message_id
  end

  # Wait for ACKs with dynamic timeout that resets on activity
  def wait_for_all_acks(message_ids : Array(String), expected_count : Int32, max_wait_time : Int32) : Bool
    log "\n---> Waiting for ACKs for #{message_ids.size} messages (expecting #{expected_count} ACKs per message)...\n"
    
    start_time = Time.monotonic
    last_log_time = start_time
    last_status_time = start_time
    
    # Initialize the last activity time
    @ack_mutex.synchronize do
      @last_ack_time = Time.monotonic
    end
    
    # Keep track of completed messages
    completed_messages = Set(String).new
    
    while true
      current_time = Time.monotonic
      elapsed = current_time - start_time
      
      # Get the latest activity timestamp under mutex protection
      last_activity_time = @ack_mutex.synchronize do
        @last_ack_time
      end
      
      # Time since last activity
      idle_time = current_time - last_activity_time
      
      # Calculate current progress
      completed_count = 0
      total_acks = 0
      message_status = {} of String => Int32
      
      @ack_mutex.synchronize do
        message_ids.each do |msg_id|
          ack_count = (@received_acks[msg_id]? || Set(String).new).size
          message_status[msg_id] = ack_count
          total_acks += ack_count
          
          if ack_count >= expected_count && !completed_messages.includes?(msg_id)
            completed_messages << msg_id
            log "✅ Message #{msg_id} received all expected ACKs"
          end
        end
        
        completed_count = completed_messages.size
      end
      
      # Log progress every second
      if (current_time - last_log_time).total_seconds >= 1.0
        remaining = message_ids.size - completed_count
        avg_acks_per_message = total_acks.to_f / message_ids.size
        pct_complete = (completed_count.to_f / message_ids.size * 100).round(2)
        
        log "Progress: #{completed_count}/#{message_ids.size} messages complete (#{pct_complete}%)"
        log "Average ACKs per message: #{avg_acks_per_message.round(2)}/#{expected_count}"
        log "Time elapsed: #{elapsed.total_seconds.round(2)}s, Idle time: #{idle_time.total_seconds.round(2)}s"
        
        last_log_time = current_time
      end
      
      # Log detailed status every 5 seconds
      if (current_time - last_status_time).total_seconds >= 5.0
        log_message_status
        last_status_time = current_time
      end
      
      # Success condition: all messages have received expected ACKs
      if completed_count >= message_ids.size
        log "✅ SUCCESS: All #{message_ids.size} messages received their expected ACKs"
        return true
      end
      
      # Timeout conditions:
      # 1. Exceeded max wait time AND haven't seen activity for at least 3 seconds
      # 2. No activity for a long time (2x the max wait time)
      if (elapsed.total_seconds > max_wait_time && idle_time.total_seconds > 15.0) || 
         (idle_time.total_seconds > max_wait_time * 2)
         
        # Log timeout details
        log "❌ TIMEOUT: Only #{completed_count}/#{message_ids.size} messages completed"
        log "Last activity was #{idle_time.total_seconds.round(2)}s ago"
        
        # Log incomplete messages
        log "\nIncomplete messages:"
        @ack_mutex.synchronize do
          message_ids.each do |msg_id|
            next if completed_messages.includes?(msg_id)
            ack_count = (@received_acks[msg_id]? || Set(String).new).size
            log "  #{msg_id}: #{ack_count}/#{expected_count} ACKs"
          end
        end
        
        log_message_status
        return false
      end
      
      sleep(0.1.seconds)
    end
  end

  # Run flood test - send all messages at once, then wait for all ACKs
  def run_test_sequence
    log "Network status before test:"
    log_message_status
    
    # Expected ACKs = nodes minus ourselves
    expected_acks = @expected_node_count - 1
    log "Expecting #{expected_acks} ACKs for each message"

    # Wait for network to stabilize
    log "Waiting 15 seconds for network to stabilize..."
    sleep(15.seconds)

    # Basic connectivity test first
    log "===== Starting basic connectivity test..."
    msg_id = send_test_message("Basic connectivity test")
    log "===== Message ID: #{msg_id}"
    
    basic_test_ids = [msg_id]
    if !wait_for_all_acks(basic_test_ids, expected_acks, @ack_wait_time)
      log "❌ Basic connectivity test failed"
      return false
    end

    log "✅ Basic connectivity test passed\n"
    sleep(2.seconds)
    
    # Multi-message flood test
    if @test_message_count > 1
      log "===== Starting flood test with #{@test_message_count} messages..."
      
      # Send all messages as fast as possible
      message_ids = [] of String
      @test_message_count.times do |i|
        msg_id = send_test_message("Flood test message #{i + 1}")
        message_ids << msg_id
      end
      
      log "===== Sent #{message_ids.size} messages, now waiting for ACKs..."
      
      # Wait for ACKs from all messages
      if !wait_for_all_acks(message_ids, expected_acks, @ack_wait_time)
        log "❌ Flood test failed - not all messages received ACKs"
        return false
      end
      
      log "✅ Flood test passed successfully - all messages received expected ACKs\n"
    end

    log "✅ All tests passed successfully"
    return true
  end
end

puts "Starting #{node_role} node #{node_id} on port #{port}"

active_connections = 0
passive_connections = 0

# Initialize network components
address = NodeAddress.new("localhost", port, node_id)
network = NetworkNode.new(address)
node = SimpleTestNode.new(
  node_id,
  network,
  node_role,
  node_count,    # Pass node_count to the SimpleTestNode
  message_count, # Pass message_count to the SimpleTestNode
  wait_time      # Pass wait_time to the SimpleTestNode
)

# Bootstrap node just waits
if node_role == "bootstrap"
  puts "[#{node_id}] Bootstrap node ready. Waiting for connections..."

  # Keep node running until Ctrl+C
  Signal::INT.trap do
    puts "[#{node_id}] Shutting down..."
    network.close
    exit(0)
  end

  # Wait forever
  while true
    if node.active_view.size != active_connections || node.passive_view.size != passive_connections
      active_connections = node.active_view.size
      passive_connections = node.passive_view.size
      puts "[#{node_id}] Active connections: #{active_connections}, Passive connections: #{passive_connections}"
      puts "[#{node_id}] Active connections: #{node.active_view.to_a.join(", ")}, Passive connections: #{node.passive_view.to_a.join(", ")}"
    end
    sleep(1.second)
  end
end

# Target and test nodes need to join the network
if node_role == "target" || node_role == "test"
  puts "[#{node_id}] Joining network via #{bootstrap_address}..."

  # Try to join the network
  success = false
  3.times do |attempt|
    begin
      join_msg = Join.new(node_id)
      network.send_message(bootstrap_address, join_msg)
      puts "[#{node_id}] Join message sent (attempt #{attempt + 1})"

      # Wait for connections to establish
      sleep(5.seconds)

      if !node.active_view.empty?
        active_connections = node.active_view.size
        passive_connections = node.passive_view.size
        puts "[#{node_id}] Successfully joined network with #{active_connections} active and #{passive_connections} passive connections"
        puts "[#{node_id}] Active connections: #{node.active_view.to_a.join(", ")}, Passive connections: #{node.passive_view.to_a.join(", ")}"
        success = true
        break
      end
    rescue ex
      puts "[#{node_id}] Error joining network: #{ex.message}"
    end

    puts "[#{node_id}] Retrying..." if attempt < 2
    sleep(1.seconds)
  end

  if !success
    puts "[#{node_id}] Failed to join network after 3 attempts"
    network.close
    exit(1)
  end
end

# Test node runs the test sequence
if node_role == "test"
  # Let the network stabilize
  sleep(5.seconds)

  # Run the test sequence
  if node.run_test_sequence
    puts "[#{node_id}] All tests passed successfully"
    network.close
    exit(0)
  else
    puts "[#{node_id}] Test sequence failed"
    network.close
    exit(1)
  end
end

# Target nodes just wait for messages
if node_role == "target"
  puts "[#{node_id}] Target node ready. Waiting for messages..."

  # Keep node running until Ctrl+C
  Signal::INT.trap do
    puts "[#{node_id}] Shutting down..."
    network.close
    exit(0)
  end

  # Wait forever
  while true
    if node.active_view.size != active_connections || node.passive_view.size != passive_connections
      active_connections = node.active_view.size
      passive_connections = node.passive_view.size
      puts "[#{node_id}] Active connections: #{active_connections}, Passive connections: #{passive_connections}"
      puts "[#{node_id}] Active connections: #{node.active_view.to_a.join(", ")}, Passive connections: #{node.passive_view.to_a.join(", ")}"
    end
    sleep(1.second)
  end
end
