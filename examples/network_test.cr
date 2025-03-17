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

  # Handle incoming broadcasts
  # Improved handle_broadcast method with fix for sender tracking

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
          log "Extracted original message ID: '#{original_id}'"

          if @sent_messages.has_key?(original_id)
            log "MATCH FOUND! Original message ID: #{original_id} matches sent message"
            @received_acks[original_id] ||= Set(String).new
            @received_acks[original_id] << real_sender
            log "Added ACK from #{real_sender} for message #{original_id}"
            log "Current ACK count for #{original_id}: #{@received_acks[original_id].size}"
            log "ACKs received from: #{@received_acks[original_id].to_a.join(", ")}"
          else
            log "WARNING: No matching sent message found for ID: #{original_id}"
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
    log "Sent messages (#{@sent_messages.size}):"
    @sent_messages.each do |id, content|
      log "  #{id} -> #{content}"
    end

    log "\nReceived ACKs (#{@received_acks.size} message ids):"
    @received_acks.each do |id, senders|
      log "  #{id} -> #{senders.size} ACKs from: #{senders.to_a.join(", ")}"
    end

    log "\nActive view (#{@active_view.size} nodes):"
    log "  #{@active_view.to_a.join(", ")}"

    log "\nPassive view (#{@passive_view.size} nodes):"
    log "  #{@passive_view.to_a.join(", ")}"
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
    # FIXED: Use double pipe (||) as delimiter between ID and content
    full_content = "#{message_id}||#{content}"

    # Store message ID for tracking ACKs
    @sent_messages[message_id] = content
    log "Stored message ID for tracking: #{message_id} -> #{content}"

    log "Sending test message: #{content} (ID: #{message_id})"
    broadcast(full_content)

    return message_id
  end

  # Wait for ACKs with timeout
  def wait_for_acks(message_id : String, expected_count : Int32, timeout_seconds : Int32) : Bool
    log "\n--->  Waiting for #{timeout_seconds} seconds for #{expected_count} ACKs for message #{message_id}...\n"
    start_time = Time.monotonic
    last_log_time = start_time
    last_status_time = start_time

    while Time.monotonic - start_time < timeout_seconds.seconds
      ack_count = (@received_acks[message_id]? || Set(String).new).size
      current_time = Time.monotonic

      # Log progress every second
      if (current_time - last_log_time).total_seconds >= 1.0
        log "Progress: #{ack_count}/#{expected_count} ACKs received"
        last_log_time = current_time
      end

      # Log full status every 5 seconds
      if (current_time - last_status_time).total_seconds >= 5.0
        log_message_status
        last_status_time = current_time
      end

      if ack_count >= expected_count
        log "Successfully received #{ack_count} ACKs"
        log "ACKs from: #{@received_acks[message_id]?.try(&.to_a.join(", ")) || "none"}"
        return true
      end

      sleep(0.1.seconds)
    end

    # Log detailed status on timeout
    actual_count = (@received_acks[message_id]? || Set(String).new).size
    log "Timeout: Only received #{actual_count}/#{expected_count} ACKs"
    log_message_status

    return false
  end

  # Run test sequence
  def run_test_sequence
    log "Network status before test:"
    log "Active connections: #{@active_view.to_a.join(", ")}"
    log "Passive connections: #{@passive_view.to_a.join(", ")}"

    # Expected ACKs = nodes minus ourselves
    expected_acks = @expected_node_count - 1
    log "Expecting #{expected_acks} ACKs for each message"

    # Wait for network to stabilize
    log "Waiting 15 seconds for network to stabilize..."
    sleep(15.seconds)

    # Basic connectivity test
    log "===== Starting basic connectivity test..."
    msg_id = send_test_message("Basic connectivity test")
    log "===== Message ID: #{msg_id}"
    if !wait_for_acks(msg_id, expected_acks, @ack_wait_time)
      log "❌ Basic connectivity test failed"
      return false
    end

    log "✅ Basic connectivity test passed\n"
    sleep(2.seconds)
    log "===== Starting multi-message test..."
    # Multi-message test
    if @test_message_count > 1
      log "===== Sending #{@test_message_count} additional test messages..."
      success = true

      @test_message_count.times do |i|
        msg_id = send_test_message("Test message #{i + 1}")
        log "\n***** Message ID: #{msg_id}\n"
        if !wait_for_acks(msg_id, expected_acks, @ack_wait_time)
          log "❌ Message #{i + 1} failed to receive all ACKs"
          success = false
          break
        end
        sleep(0.2.seconds) # Short delay between messages
      end

      if success
        log "✅ All messages received ACKs successfully\n"
      else
        log "❌ Some messages failed to receive all ACKs\n"
        return false
      end
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
