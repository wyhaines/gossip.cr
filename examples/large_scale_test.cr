#!/usr/bin/env crystal

require "../src/gossip"
require "option_parser"
require "uuid"
require "benchmark"
require "json"

# Configuration settings
class TestConfig
  property node_count : Int32 = 20          # Total number of nodes to launch
  property base_port : Int32 = 8000         # Starting port number
  property bootstrap_port : Int32 = 8000    # Port for the bootstrap node
  property test_node_port : Int32 = 7000    # Port for the test driver node
  property message_count : Int32 = 50       # Number of messages for stress test
  property max_wait_time : Int32 = 30       # Max seconds to wait for responses
  property process_launch_delay : Float64 = 0.1 # Delay between process launches
  property log_folder : String = "logs"     # Folder to store logs
  property debug : Bool = false             # Enable additional debug logging
  property stress_test : Bool = true        # Enable stress testing
  property verify_delivery : Bool = true    # Verify message delivery
  property test_id : String                 # Unique identifier for this test run
  property run_mode : String = "coordinator" # Mode: coordinator or node
  property node_role : String = ""          # Role: bootstrap, test, or target
  property node_id : String = ""            # Node ID
  property bootstrap_id : String = ""       # Bootstrap node ID
  property node_port : Int32 = 0            # Node port
  property network_stabilize_time : Int32 = 10 # Seconds to wait for network to stabilize
  property join_retry_count : Int32 = 3     # Number of retries for joining
  property kill_timeout : Int32 = 5         # Seconds to wait for processes to terminate
  
  def initialize
    # Generate a unique test ID for this run
    @test_id = UUID.random.to_s[0..7]
  end
  
  def to_s
    "Test Configuration: #{node_count} nodes, base port: #{base_port}, test ID: #{test_id}"
  end
  
  def log_file(node_id : String) : String
    # Using double quotes for strings in Crystal (not single quotes like Ruby)
    "#{log_folder}/#{test_id}_#{node_id.gsub("@", "_at_").gsub(":", "_")}.log"
  end
end

# Get the current executable path
EXECUTABLE_PATH = Process.executable_path || PROGRAM_NAME

# Store all PIDs to ensure cleanup on exit
LAUNCHED_PIDS = [] of Int64

# Ensure cleanup on program exit
at_exit do
  puts "Cleaning up any remaining processes..."
  LAUNCHED_PIDS.each do |pid|
    begin
      # Try TERM signal first
      Process.signal(Signal::TERM, pid)
      sleep(0.1.seconds)
      
      # If still running, force kill
      if Process.exists?(pid)
        Process.signal(Signal::KILL, pid)
      end
    rescue ex
      # Ignore errors during cleanup
    end
  end
end

# Parse command line arguments
config = TestConfig.new

# Process arguments with OptionParser
OptionParser.parse do |parser|
  parser.banner = "Usage: #{EXECUTABLE_PATH} [arguments]"
  
  # Common options
  parser.on("-n COUNT", "--nodes=COUNT", "Number of nodes to launch") do |count|
    config.node_count = count.to_i
  end
  
  parser.on("-p PORT", "--port=PORT", "Base port number") do |port|
    config.base_port = port.to_i
    config.bootstrap_port = port.to_i
  end
  
  parser.on("-m COUNT", "--messages=COUNT", "Number of messages for stress test") do |count|
    config.message_count = count.to_i
  end
  
  parser.on("-w SECONDS", "--wait=SECONDS", "Max wait time for responses") do |seconds|
    config.max_wait_time = seconds.to_i
  end
  
  parser.on("--stabilize=SECONDS", "Wait time for network to stabilize") do |seconds|
    config.network_stabilize_time = seconds.to_i
  end
  
  parser.on("-d", "--debug", "Enable debug logging") do
    config.debug = true
  end
  
  parser.on("-t ID", "--test-id=ID", "Set test ID manually") do |id|
    config.test_id = id
  end
  
  # Mode options
  parser.on("--mode=MODE", "Run mode (coordinator or node)") do |mode|
    config.run_mode = mode
  end
  
  # Node-specific options
  parser.on("--role=ROLE", "Node role (bootstrap, test, or target)") do |role|
    config.node_role = role
  end
  
  parser.on("--node-port=PORT", "Node port") do |port|
    config.node_port = port.to_i
  end
  
  parser.on("--id=ID", "Node ID") do |id|
    config.node_id = id
  end
  
  parser.on("--bootstrap-id=ID", "Bootstrap node ID") do |id|
    config.bootstrap_id = id
  end
  
  parser.on("-h", "--help", "Show this help") do
    puts parser
    exit
  end
  
  # Ignore unknown options
  parser.invalid_option do |flag|
    # Just skip
  end
end

# Set up logging directory
Dir.mkdir_p(config.log_folder)

# Role enum for process type
enum Role
  Bootstrap # First node in the network
  TestNode  # Node that drives the test
  TargetNode # Regular node in the network
end

# Node class with acknowledgment behavior
class TestNode < Node
  property received_acks : Hash(String, Set(String)) = Hash(String, Set(String)).new { |h, k| h[k] = Set(String).new }
  property pending_msgs : Set(String) = Set(String).new
  property test_config : TestConfig
  property role : Role
  property start_time = Time.monotonic
  property ack_channel = Channel(Tuple(String, String, String)).new(100) # message_id, sender, content
  property debug : Bool
  
  # Add a method to safely get active view nodes
  def get_active_view : Array(String)
    snapshot = [] of String
    # Direct access to the active_view property, which should be thread-safe
    snapshot = active_view.to_a
    return snapshot
  end
  
  def initialize(id : String, network : NetworkNode, @test_config : TestConfig, @role : Role)
    super(id, network)
    @debug = @test_config.debug
    
    # Start background receiver for ACKs
    if @role == Role::TestNode
      spawn do
        while @network.running
          begin
            msg_id, sender, content = @ack_channel.receive
            @received_acks[msg_id] << sender
            log "Received ACK for #{msg_id} from #{sender}, content: #{content}"
          rescue ex
            log "Error processing ACK: #{ex.message}"
          end
        end
      end
    end
    
    # Debug view status periodically if enabled
    if @debug
      spawn do
        while @network.running
          active_view_now = get_active_view
          log "Current active view (#{active_view_now.size} nodes): #{active_view_now.join(", ")}"
          sleep(5.seconds)
        end
      end
    end
  end
  
  # Override handle_join to ensure proper logging
  def handle_join(message : Join)
    sender = message.sender
    log "Received JOIN from #{sender}"
    
    # Get current views before update
    active_before = active_view.to_a
    passive_before = passive_view.to_a
    
    # Call parent implementation
    super
    
    # Get views after update for logging
    active_after = active_view.to_a
    passive_after = passive_view.to_a
    
    # Log changes
    new_active = active_after - active_before
    removed_active = active_before - active_after
    
    if !new_active.empty?
      log "Added to active view: #{new_active.join(", ")}"
    end
    
    if !removed_active.empty?
      log "Removed from active view: #{removed_active.join(", ")}"
    end
    
    # Log current views
    log "Current active view (#{active_view.size}): #{active_view.to_a.join(", ")}"
    log "Current passive view (#{passive_view.size}): #{passive_view.to_a.join(", ")}"
  end
  
  # Override to handle our custom ACK protocol
  def handle_broadcast(message : BroadcastMessage)
    message_id = message.message_id
    sender = message.sender
    content = message.content
    
    # Process normally first
    already_received = false
    @messages_mutex.synchronize do
      already_received = @received_messages.includes?(message_id)
      unless already_received
        @received_messages << message_id
        @message_contents[message_id] = content
      end
    end
    
    # If this is a new message
    if !already_received
      log "Received broadcast '#{content}' from #{sender}"
      
      # Handle special ACK protocol
      if @role != Role::TestNode && !content.starts_with?("ACK ")
        # This is a target node receiving a test message - send an ACK
        begin
          ack_content = "ACK #{content}"
          ack_id = broadcast(ack_content)
          log "Sent ACK for message: #{content}"
        rescue ex
          log "Error sending ACK: #{ex.message}"
        end
      elsif @role == Role::TestNode && content.starts_with?("ACK ")
        # This is the test node receiving an ACK - record it
        original_content = content[4..]
        msg_parts = original_content.split(":", 2)
        if msg_parts.size == 2
          original_id = msg_parts[0]
          @ack_channel.send({original_id, sender, content})
        end
      end
      
      # Forward to others (standard gossip protocol)
      active_nodes = get_active_view
      
      active_nodes.each do |node|
        next if node == sender
        
        # Check if node is failed
        is_failed = false
        @failures_mutex.synchronize do
          is_failed = @failed_nodes.includes?(node)
        end
        next if is_failed
        
        begin
          if rand < @lazy_push_probability
            lazy_msg = LazyPushMessage.new(@id, message_id)
            send_message(node, lazy_msg)
          else
            forward_msg = BroadcastMessage.new(@id, message_id, content)
            send_message(node, forward_msg)
          end
        rescue ex
          log "Failed to forward message to #{node}: #{ex.message}"
          handle_node_failure(node)
        end
      end
    end
  end
  
  # Method to send a test message and track its ID
  def send_test_message(content : String) : String
    message_id = "#{Time.utc.to_unix_ms}-#{rand(10000)}"
    full_content = "#{message_id}:#{content}"
    
    # Record pending message
    @pending_msgs << message_id
    
    # Clear any previous ACKs for this message
    @received_acks[message_id] = Set(String).new
    
    # Get current active view for logging
    active_nodes = get_active_view
    log "Sending test message with ID #{message_id} to #{active_nodes.size} nodes: #{active_nodes.join(", ")}"
    
    # Send the message
    broadcast(full_content)
    
    # Return the ID for tracking
    message_id
  end
  
  # Wait for ACKs with timeout
  def wait_for_acks(message_id : String, expected_count : Int32, timeout_seconds : Int32) : Bool
    start = Time.monotonic
    
    while Time.monotonic - start < timeout_seconds.seconds
      count = @received_acks[message_id].size
      if count >= expected_count
        return true
      end
      
      # Periodically log progress
      if @debug && (Time.monotonic - start).total_seconds.to_i % 5 == 0
        log "Waiting for ACKs: #{count}/#{expected_count} received"
        log "Current active view: #{get_active_view.join(", ")}"
      end
      
      sleep(0.1.seconds)
    end
    
    false # Timeout
  end
  
  # Log with timestamp
  def log(message : String)
    timestamp = Time.monotonic - @start_time
    puts "[#{@id}] [+#{timestamp.total_seconds.to_i}.#{timestamp.total_milliseconds.to_i % 1000}s] #{message}"
  end
  
  # Statistics and metrics
  def get_stats(message_ids : Array(String))
    stats = {
      "total_messages": message_ids.size,
      "received_acks_count": message_ids.sum { |id| @received_acks[id].size },
      "expected_acks": message_ids.size * (@test_config.node_count - 1),
      "success_rate": message_ids.count { |id| @received_acks[id].size >= (@test_config.node_count - 1) } / message_ids.size.to_f,
      "messages": message_ids.map do |id|
        {
          "id": id,
          "ack_count": @received_acks[id].size,
          "nodes_missing": @test_config.node_count - 1 - @received_acks[id].size
        }
      end
    }
    
    stats
  end
  
  # Helper method to try connecting to a node multiple times
  def try_connect_to_node(target_id : String, retries : Int32 = 3) : Bool
    retries.times do |attempt|
      begin
        log "Attempt #{attempt+1}/#{retries} to connect to #{target_id}"
        join_msg = Join.new(@id)
        send_message(target_id, join_msg)
        
        # Wait a bit for connection to be established
        sleep(1.seconds)
        
        # Check if the node is in our active view
        if active_view.includes?(target_id)
          log "Successfully connected to #{target_id}"
          return true
        end
      rescue ex
        log "Failed to connect to #{target_id} (attempt #{attempt+1}/#{retries}): #{ex.message}"
      end
    end
    
    log "Failed to connect to #{target_id} after #{retries} attempts"
    false
  end
  
  # Attempt to establish connections to other nodes directly
  def establish_connections(bootstrap_id : String, node_count : Int32, base_port : Int32)
    log "Attempting to establish direct connections to #{node_count} nodes..."
    
    # Try connecting to bootstrap node first
    if !active_view.includes?(bootstrap_id)
      try_connect_to_node(bootstrap_id)
    end
    
    # Try connecting to other nodes
    nodes_connected = 0
    node_count.times do |i|
      node_id = "node#{i}@localhost:#{base_port + i}"
      next if node_id == @id # Skip self
      
      # Skip if already in active view
      if active_view.includes?(node_id)
        nodes_connected += 1
        next
      end
      
      # Try to connect
      if try_connect_to_node(node_id)
        nodes_connected += 1
      end
      
      # Stop if we've reached the max active view size
      break if active_view.size >= MAX_ACTIVE
    end
    
    log "Established connections to #{nodes_connected} nodes"
    log "Final active view: #{active_view.to_a.join(", ")}"
  end
end

# Main process launcher and manager
class TestRunner
  getter processes = [] of Process
  getter config : TestConfig
  getter test_node : TestNode? = nil
  getter bootstrap_node_id = ""
  property start_time = Time.monotonic
  
  def initialize(@config)
    @start_time = Time.monotonic
    create_directories
  end
  
  def create_directories
    Dir.mkdir_p(@config.log_folder)
  end
  
  def log(message : String)
    timestamp = Time.monotonic - @start_time
    puts "[TestRunner] [+#{timestamp.total_seconds.to_i}.#{timestamp.total_milliseconds.to_i % 1000}s] #{message}"
  end
  
  # Launch a single node process
  def launch_node(cmd : String, log_file : String) : Process
    log "Launching node: #{cmd}"
    process = Process.new(cmd, shell: true, output: File.open(log_file, "w"), error: :inherit)
    
    # Store PID for cleanup
    LAUNCHED_PIDS << process.pid
    
    # Add to our process list
    @processes << process
    
    return process
  end
  
  # Launch all the node processes
  def launch_network
    log "Launching network with #{@config.node_count} nodes..."
    
    # Start bootstrap node first
    bootstrap_port = @config.bootstrap_port
    @bootstrap_node_id = "node0@localhost:#{bootstrap_port}"
    
    # Use the current executable to launch nodes
    cmd = "#{EXECUTABLE_PATH} --mode=node --role=bootstrap --node-port=#{bootstrap_port} --id=#{@bootstrap_node_id} --test-id=#{@config.test_id}"
    if @config.debug
      cmd += " --debug"
    end
    
    launch_node(cmd, @config.log_file("bootstrap"))
    
    # Allow bootstrap node time to start
    sleep(1.seconds)
    
    # Launch additional target nodes first - we want them ready before the test node joins
    (@config.node_count - 1).times do |i|
      port = @config.base_port + i + 1
      node_id = "node#{i+1}@localhost:#{port}"
      
      cmd = "#{EXECUTABLE_PATH} --mode=node --role=target --node-port=#{port} --id=#{node_id} --bootstrap-id=#{@bootstrap_node_id} --test-id=#{@config.test_id}"
      if @config.debug
        cmd += " --debug"
      end
      
      launch_node(cmd, @config.log_file(node_id))
      
      # Small delay to prevent overwhelming the system
      sleep(@config.process_launch_delay.seconds)
    end
    
    # Allow time for target nodes to join the network
    log "Waiting for target nodes to join..."
    sleep(3.seconds)
    
    # Now launch test node
    test_port = @config.test_node_port
    test_node_id = "test@localhost:#{test_port}"
    
    cmd = "#{EXECUTABLE_PATH} --mode=node --role=test --node-port=#{test_port} --id=#{test_node_id} --bootstrap-id=#{@bootstrap_node_id} --test-id=#{@config.test_id}"
    if @config.debug
      cmd += " --debug"
    end
    
    launch_node(cmd, @config.log_file("test"))
    
    # Allow time for all nodes to join the network
    log "Waiting for network to stabilize (#{@config.network_stabilize_time} seconds)..."
    sleep(@config.network_stabilize_time.seconds)
  end
  
  # Launch the test node and run tests
  def run_test_node
    log "Starting test node..."
    
    # Create test node with debug mode
    address = NodeAddress.new("localhost", @config.test_node_port, "test@localhost:#{@config.test_node_port}")
    network = NetworkNode.new(address)
    @test_node = TestNode.new("test@localhost:#{@config.test_node_port}", network, @config, Role::TestNode)
    test_node = @test_node.not_nil!
    
    # Join the network through bootstrap node with retries
    joined = false
    @config.join_retry_count.times do |attempt|
      begin
        log "Attempt #{attempt+1}/#{@config.join_retry_count} to join network through #{@bootstrap_node_id}"
        join_msg = Join.new(test_node.id)
        network.send_message(@bootstrap_node_id, join_msg)
        log "Join message sent to bootstrap node"
        
        # Wait a bit for connection to be established
        sleep(2.seconds)
        
        # Check if we have any connections
        if !test_node.get_active_view.empty?
          joined = true
          log "Successfully joined network with #{test_node.get_active_view.size} connections"
          break
        end
      rescue ex
        log "Failed to join network: #{ex.message}"
      end
    end
    
    unless joined
      log "Failed to join network after #{@config.join_retry_count} attempts"
      shutdown
      exit 1
    end
    
    # Try to directly establish connections to other nodes
    log "Attempting to establish more connections..."
    test_node.establish_connections(@bootstrap_node_id, @config.node_count, @config.base_port)
    
    # Allow time for connections to stabilize
    log "Waiting for connections to stabilize..."
    sleep(3.seconds)
    
    # Check for any connections
    active_connections = test_node.get_active_view
    if active_connections.empty?
      log "⚠️ Warning: Test node has no active connections!"
      log "Attempting emergency connection establishment..."
      # Try direct connections to all nodes
      @config.node_count.times do |i|
        node_id = "node#{i}@localhost:#{@config.base_port + i}"
        test_node.try_connect_to_node(node_id, 2)
      end
      sleep(2.seconds)
    end
    
    # Run the tests
    run_tests
    
    # Clean shutdown
    network.close
  end
  
  # Run the actual test cases
  def run_tests
    test_node = @test_node.not_nil!
    
    # Log active connections
    active_connections = test_node.get_active_view
    log "Test node active connections: #{active_connections.size} nodes"
    log "Active view: #{active_connections.join(", ")}"
    
    # Skip tests if we have no connections
    if active_connections.empty?
      log "⚠️ Warning: No active connections - tests will likely fail"
    end
    
    # Test 1: Basic connectivity - send a single message and verify all nodes respond
    if @config.verify_delivery
      log "Running Test 1: Basic Connectivity..."
      
      test_start = Time.monotonic
      msg_id = test_node.send_test_message("Hello from test node!")
      log "Sent test message with ID: #{msg_id}"
      
      expected_responses = @config.node_count - 1 # all nodes except test node
      success = test_node.wait_for_acks(msg_id, expected_responses, @config.max_wait_time)
      
      test_duration = Time.monotonic - test_start
      
      if success
        log "✅ Test 1 Passed: Received ACKs from all #{expected_responses} nodes in #{test_duration.total_seconds.round(2)}s"
      else
        recv_count = test_node.received_acks[msg_id].size
        log "❌ Test 1 Failed: Only received #{recv_count}/#{expected_responses} ACKs in #{@config.max_wait_time}s"
      end
      
      # If basic connectivity fails, try to recover by establishing more connections
      if !success && test_node.received_acks[msg_id].size == 0
        log "Attempting to recover connectivity..."
        test_node.establish_connections(@bootstrap_node_id, @config.node_count, @config.base_port)
        sleep(2.seconds)
      end
    end
    
    # Test 2: Stress testing - send many messages rapidly
    if @config.stress_test
      log "Running Test 2: Stress Testing with #{@config.message_count} messages..."
      
      # Prepare messages
      messages = [] of String
      
      test_start = Time.monotonic
      sent_count = 0
      
      # Send messages in rapid succession
      @config.message_count.times do |i|
        begin
          msg_id = test_node.send_test_message("Stress test message #{i+1}")
          messages << msg_id
          sent_count += 1
        rescue ex
          log "Error sending stress test message #{i+1}: #{ex.message}"
        end
        
        # Tiny delay to avoid overwhelming the system
        sleep(0.01.seconds) if i % 10 == 0
      end
      
      log "Sent #{sent_count} messages, waiting for ACKs..."
      
      # Wait for responses with timeout
      start_wait = Time.monotonic
      expected_responses = (@config.node_count - 1) * sent_count
      total_acks = 0
      
      while Time.monotonic - start_wait < @config.max_wait_time.seconds
        total_acks = messages.sum { |id| test_node.received_acks[id].size }
        if total_acks >= expected_responses
          break
        end
        
        # Progress update every 5 seconds
        if (Time.monotonic - start_wait).total_seconds.to_i % 5 == 0
          log "Progress: #{total_acks}/#{expected_responses} ACKs received (#{(total_acks / expected_responses.to_f * 100).round(1)}%)"
        end
        
        sleep(0.5.seconds)
      end
      
      test_duration = Time.monotonic - test_start
      success_rate = expected_responses > 0 ? total_acks / expected_responses.to_f : 0.0
      
      log "Stress Test Results:"
      log "- Time: #{test_duration.total_seconds.round(2)}s"
      log "- Messages sent: #{sent_count}"
      log "- ACKs received: #{total_acks}/#{expected_responses} (#{(success_rate * 100).round(1)}%)"
      log "- Throughput: #{(total_acks / test_duration.total_seconds).round(2)} ACKs/second"
      
      # Detailed report
      stats = test_node.get_stats(messages)
      log "- Messages with 100% delivery: #{stats[:messages].count { |m| m["nodes_missing"] == 0 }}/#{sent_count}"
      log "- Average ACKs per message: #{(stats[:received_acks_count] / sent_count.to_f).round(1)}"
      
      # Success criteria
      if success_rate >= 0.95
        log "✅ Test 2 Passed: Success rate #{(success_rate * 100).round(1)}% (>= 95%)"
      else
        log "❌ Test 2 Failed: Success rate #{(success_rate * 100).round(1)}% (< 95%)"
      end
    end
  end
  
  # Clean shutdown all processes
  def shutdown
    log "Shutting down all processes..."
    
    # First try to gracefully stop all processes
    log "Sending TERM signal to all processes..."
    @processes.each do |process|
      begin
        Process.signal(Signal::TERM, process.pid)
      rescue ex
        log "Error sending TERM to process #{process.pid}: #{ex.message}"
      end
    end
    
    # Wait for all processes to terminate
    log "Waiting up to #{@config.kill_timeout} seconds for processes to terminate..."
    timeout = @config.kill_timeout.seconds
    start = Time.monotonic
    
    # Check if processes are still running
    running_count = 0
    while running_count > 0 && Time.monotonic - start < timeout
      sleep(0.5.seconds)
      
      running_count = 0
      @processes.each do |process|
        running_count += 1 if Process.exists?(process.pid)
      end
      
      log "Still waiting for #{running_count} processes to terminate..." if running_count > 0
    end
    
    # Force kill any remaining processes
    if running_count > 0
      log "Force killing #{running_count} remaining processes..."
      @processes.each do |process|
        if Process.exists?(process.pid)
          begin
            log "Sending KILL signal to process #{process.pid}"
            Process.signal(Signal::KILL, process.pid)
          rescue ex
            log "Error sending KILL to process #{process.pid}: #{ex.message}"
          end
        end
      end
    end
    
    # Verify all processes are terminated
    sleep(0.5.seconds)
    running_count = @processes.count { |p| Process.exists?(p.pid) }
    if running_count > 0
      log "Warning: #{running_count} processes could not be terminated!"
      @processes.each do |process|
        log "Process #{process.pid} still running: #{Process.exists?(process.pid)}"
      end
    else
      log "All processes successfully terminated"
    end
  end
end

# Node implementation for individual processes
class NodeProcess
  getter id : String
  getter role : Role
  getter port : Int32
  getter bootstrap_id : String
  getter test_id : String
  getter debug : Bool
  
  def initialize(@id, @role, @port, @bootstrap_id, @test_id, @debug = false)
    # Nothing to do
  end
  
  def run
    puts "[#{@id}] Starting node with role: #{@role}"
    
    # Create test config
    config = TestConfig.new
    config.test_id = @test_id
    config.debug = @debug
    
    # Initialize network
    address = NodeAddress.new("localhost", @port, @id)
    network = NetworkNode.new(address)
    
    # Create node based on role
    node = TestNode.new(@id, network, config, @role)
    
    # If not bootstrap, join through bootstrap node
    if @role != Role::Bootstrap
      begin
        puts "[#{@id}] Joining network through #{@bootstrap_id}..."
        join_msg = Join.new(@id)
        network.send_message(@bootstrap_id, join_msg)
        puts "[#{@id}] Sent join message to #{@bootstrap_id}"
        
        # Wait a bit and check connections
        sleep(1.seconds)
        if node.active_view.empty?
          puts "[#{@id}] Warning: No active connections after joining"
          
          # Try again with more explicit method
          puts "[#{@id}] Retrying connection to bootstrap node..."
          if node.try_connect_to_node(@bootstrap_id)
            puts "[#{@id}] Successfully connected to bootstrap node"
          else
            puts "[#{@id}] Failed to connect to bootstrap node after retries"
          end
        else
          puts "[#{@id}] Joined network with #{node.active_view.size} connections"
        end
      rescue ex
        puts "[#{@id}] Failed to join network: #{ex.message}"
        network.close
        exit 1
      end
    end
    
    # Handler for signals - ensure proper shutdown
    termination_handler = ->(signal : Signal) do
      puts "[#{@id}] Received #{signal} signal, shutting down..."
      network.close
      exit(0)
    end
    
    # Set up signal handlers
    Signal::INT.trap &termination_handler
    Signal::TERM.trap &termination_handler
    
    # Handle at_exit - make sure network is closed
    at_exit do
      puts "[#{@id}] Shutting down..."
      network.close if network.running
    end
    
    # Keep the process alive
    loop do
      sleep(1.seconds)
      # Check if network is still running, exit if not
      unless network.running
        puts "[#{@id}] Network is no longer running, exiting..."
        exit 0
      end
    end
  end
end

# Main entry point
if config.run_mode == "node"
  # Determine node role
  role = case config.node_role
         when "bootstrap" then Role::Bootstrap
         when "test"      then Role::TestNode
         else                  Role::TargetNode
         end
  
  # Use the correct port
  port = config.node_port
  
  # Create node process
  process = NodeProcess.new(config.node_id, role, port, config.bootstrap_id, config.test_id, config.debug)
  process.run
else
  # Run as test coordinator
  runner = TestRunner.new(config)
  
  begin
    runner.launch_network
    runner.run_test_node
  ensure
    # Always make sure to shut down properly
    runner.shutdown
  end
end
