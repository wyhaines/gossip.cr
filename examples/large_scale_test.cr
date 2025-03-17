#!/usr/bin/env crystal

require "../src/gossip"
require "option_parser"
require "uuid"
require "benchmark"

# Configuration settings
class TestConfig
  property node_count : Int32 = 5          # Total number of nodes to launch
  property base_port : Int32 = 8000        # Starting port number
  property test_node_port : Int32 = 7000   # Port for the test driver node
  property message_count : Int32 = 20      # Number of messages for stress test
  property max_wait_time : Int32 = 30      # Max seconds to wait for responses
  property launch_delay : Float64 = 0.5    # Delay between process launches
  property log_folder : String = "logs"    # Folder to store logs
  property debug : Bool = false            # Enable additional debug logging
  property test_id : String                # Unique identifier for this test run
  property run_mode : String = "coordinator" # Mode: coordinator or node
  property node_role : String = ""         # Role: bootstrap, test, or target
  property node_id : String = ""           # Node ID
  property node_port : Int32 = 0           # Node port
  property bootstrap_port : Int32 = 8000   # Port for bootstrap node
  property bootstrap_id : String = ""      # ID of bootstrap node
  property stabilize_time : Int32 = 10     # Seconds to wait for network to stabilize
  
  def initialize
    # Generate a unique test ID for this run
    @test_id = UUID.random.to_s[0..7]
    @bootstrap_id = "node1@localhost:#{@bootstrap_port}"
  end
  
  def to_s
    "Test Configuration: #{node_count} nodes, base port: #{base_port}, test ID: #{test_id}"
  end
  
  def log_file(node_id : String) : String
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

# Main test runner class
class TestRunner
  getter processes = [] of Process
  getter config : TestConfig
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
  
  # Launch the test network
  def launch_network
    log "Launching network with #{@config.node_count} nodes..."
    
    # Start bootstrap node first (node1)
    bootstrap_port = @config.bootstrap_port
    bootstrap_id = "node1@localhost:#{bootstrap_port}"
    
    cmd = "#{EXECUTABLE_PATH} --mode=node --role=bootstrap --port=#{bootstrap_port} --id=#{bootstrap_id} --test-id=#{@config.test_id}"
    if @config.debug
      cmd += " --debug"
    end
    
    launch_node(cmd, @config.log_file("node1"))
    
    # Allow bootstrap node time to start
    log "Waiting for bootstrap node to initialize..."
    sleep(2.seconds)
    
    # Launch additional nodes
    (2..@config.node_count).each do |i|
      port = @config.base_port + i - 1
      node_id = "node#{i}@localhost:#{port}"
      
      cmd = "#{EXECUTABLE_PATH} --mode=node --role=target --port=#{port} --id=#{node_id} --bootstrap-id=#{bootstrap_id} --test-id=#{@config.test_id}"
      if @config.debug
        cmd += " --debug"
      end
      
      launch_node(cmd, @config.log_file(node_id))
      
      # Small delay to prevent overwhelming the system
      sleep(@config.launch_delay.seconds)
    end
    
    # Allow time for network to stabilize
    log "Waiting for network to stabilize (#{@config.stabilize_time} seconds)..."
    sleep(@config.stabilize_time.seconds)
    
    # Now launch test node
    test_port = @config.test_node_port
    test_node_id = "test@localhost:#{test_port}"
    
    cmd = "#{EXECUTABLE_PATH} --mode=node --role=test --port=#{test_port} --id=#{test_node_id} --bootstrap-id=#{bootstrap_id} --test-id=#{@config.test_id}"
    if @config.debug
      cmd += " --debug"
    end
    
    launch_node(cmd, @config.log_file("test"))
    
    # Allow time for test node to join
    log "Waiting for test node to join..."
    sleep(5.seconds)
  end
  
  # Clean shutdown all processes
  def shutdown
    log "Shutting down all processes..."
    
    # First try to gracefully stop all processes
    @processes.each do |process|
      begin
        Process.signal(Signal::TERM, process.pid)
      rescue ex
        log "Error sending TERM to process #{process.pid}: #{ex.message}"
      end
    end
    
    # Wait for all processes to terminate
    log "Waiting for processes to terminate..."
    timeout = 5.seconds
    start = Time.monotonic
    
    # Check if processes are still running
    running_count = 0
    while running_count > 0 && Time.monotonic - start < timeout
      sleep(0.5.seconds)
      
      running_count = 0
      @processes.each do |process|
        begin
          running_count += 1 if Process.exists?(process.pid)
        rescue
          # Ignore errors checking process existence
        end
      end
      
      log "Still waiting for #{running_count} processes to terminate..." if running_count > 0
    end
    
    # Force kill any remaining processes
    if running_count > 0
      log "Force killing #{running_count} remaining processes..."
      @processes.each do |process|
        begin
          if Process.exists?(process.pid)
            Process.signal(Signal::KILL, process.pid)
          end
        rescue ex
          log "Error sending KILL to process #{process.pid}: #{ex.message}"
        end
      end
    end
    
    log "Cleanup complete"
  end
end

# TestNode class that extends Node with test functionality
class TestNode < Node
  property received_acks = Hash(String, Set(String)).new { |h, k| h[k] = Set(String).new }
  property message_id_to_content = Hash(String, String).new
  property role : String
  property bootstrap_id : String
  property start_time = Time.monotonic
  
  def initialize(id : String, network : NetworkNode, @role : String, @bootstrap_id : String)
    super(id, network)
    
    # Start a periodic job to print our connections if in debug mode
    spawn do
      while @network.running
        debug_log "Node #{@id}: Active connections: #{@active_view.to_a.join(", ")}"
        sleep(5.seconds)
      end
    end
  end
  
  # Send a test message and return its ID
  def send_test_message(content : String) : String
    # Create a unique ID for this message
    message_id = "#{Time.utc.to_unix_ms}-#{rand(1000)}"
    full_content = "#{message_id}:#{content}"
    
    # Store mapping for tracking ACKs
    @message_id_to_content[message_id] = content
    
    # Broadcast to the network
    log "Sending test message: #{content} (ID: #{message_id})"
    broadcast(full_content)
    
    return message_id
  end
  
  # Override broadcast handler to implement ACK protocol
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
    
    # If this is a new message and not from ourselves
    if !already_received
      log "Received broadcast: #{content} from #{sender}"
      
      # Handle ACK protocol
      if content.starts_with?("ACK ")
        # This is an ACK message - if we're the test node, record it
        if @role == "test"
          original_content = content[4..]
          if original_content.includes?(":")
            original_id = original_content.split(":", 2)[0]
            sender_id = sender
            @received_acks[original_id] << sender_id
            log "Recorded ACK for message #{original_id} from #{sender_id}"
          end
        end
      elsif @role != "test" && content.includes?(":")
        # We're a regular node and received a test message - send ACK
        begin
          ack_content = "ACK #{content}"
          log "Sending ACK for: #{content}"
          broadcast(ack_content)
        rescue ex
          log "Error sending ACK: #{ex.message}"
        end
      end
      
      # Forward to others per the gossip protocol
      active_nodes = [] of String
      @views_mutex.synchronize do
        active_nodes = @active_view.to_a
      end
      
      active_nodes.each do |node|
        next if node == sender
        
        failed = false
        @failures_mutex.synchronize do
          failed = @failed_nodes.includes?(node)
        end
        next if failed
        
        begin
          if rand < @lazy_push_probability
            lazy_msg = LazyPushMessage.new(@id, message_id)
            send_message(node, lazy_msg)
          else
            forward_msg = BroadcastMessage.new(@id, message_id, content)
            send_message(node, forward_msg)
          end
        rescue ex
          log "Failed to forward to #{node}: #{ex.message}"
          handle_node_failure(node)
        end
      end
    end
  end
  
  # Wait for acknowledgments with timeout
  def wait_for_acks(message_id : String, expected_count : Int32, timeout_seconds : Int32) : Bool
    log "Waiting for #{expected_count} ACKs for message #{message_id}..."
    start_time = Time.monotonic
    
    while Time.monotonic - start_time < timeout_seconds.seconds
      ack_count = @received_acks[message_id].size
      
      if ack_count >= expected_count
        log "Successfully received #{ack_count} ACKs"
        return true
      end
      
      # Periodically log progress
      if (Time.monotonic - start_time).total_seconds.to_i % 5 == 0
        log "Progress: #{ack_count}/#{expected_count} ACKs received"
      end
      
      sleep(0.1.seconds)
    end
    
    log "Timeout: Only received #{@received_acks[message_id].size}/#{expected_count} ACKs"
    return false
  end
  
  # Join the network through the bootstrap node
  def join_network : Bool
    # Don't join if we're the bootstrap node
    return true if @role == "bootstrap"
    
    log "Joining network through #{@bootstrap_id}..."
    
    3.times do |attempt|
      begin
        # Send a join message
        join_msg = Join.new(@id)
        @network.send_message(@bootstrap_id, join_msg)
        log "Sent join message (attempt #{attempt + 1})"
        
        # Wait for connection to be established
        sleep(2.seconds)
        
        # Check if we have any active connections
        if !@active_view.empty?
          log "Successfully joined network with #{@active_view.size} connections"
          log "Active view: #{@active_view.to_a.join(", ")}"
          return true
        end
      rescue ex
        log "Error joining network: #{ex.message}"
      end
      
      log "No connections established on attempt #{attempt + 1}, retrying..."
      sleep(1.seconds)
    end
    
    log "Failed to join network after 3 attempts"
    return false
  end
  
  # Log with timestamp
  def log(message : String)
    timestamp = Time.monotonic - @start_time
    puts "[#{@id}] [+#{timestamp.total_seconds.to_i}.#{timestamp.total_milliseconds.to_i % 1000}s] #{message}"
  end
  
  # Run connectivity tests
  def run_connectivity_test(node_count : Int32, message_count : Int32, wait_time : Int32) : Bool
    log "Starting connectivity test with #{node_count} nodes, #{message_count} messages..."
    
    # Basic connectivity test
    log "Basic connectivity test - single message..."
    msg_id = send_test_message("Hello from test node!")
    success = wait_for_acks(msg_id, node_count - 1, wait_time)
    
    if !success
      log "⚠️ Basic connectivity test failed!"
      return false
    end
    
    log "✅ Basic connectivity test passed - received ACKs from all nodes"
    
    # Stress test - send multiple messages
    if message_count > 1
      log "Stress test - sending #{message_count} messages..."
      message_ids = [] of String
      
      message_count.times do |i|
        msg_id = send_test_message("Stress test message #{i+1}")
        message_ids << msg_id
        sleep(0.05.seconds) # Small delay to avoid overwhelming the network
      end
      
      # Wait for responses with timeout
      start_wait = Time.monotonic
      expected_acks = (node_count - 1) * message_count
      total_acks = 0
      
      while Time.monotonic - start_wait < wait_time.seconds
        total_acks = message_ids.sum { |id| @received_acks[id].size }
        if total_acks >= expected_acks
          break
        end
        
        # Progress update every 3 seconds
        if (Time.monotonic - start_wait).total_seconds.to_i % 3 == 0
          log "Progress: #{total_acks}/#{expected_acks} ACKs received (#{(total_acks / expected_acks.to_f * 100).round(1)}%)"
        end
        
        sleep(0.5.seconds)
      end
      
      success_rate = total_acks / expected_acks.to_f
      
      log "Stress test results:"
      log "- Expected ACKs: #{expected_acks}"
      log "- Received ACKs: #{total_acks}"
      log "- Success rate: #{(success_rate * 100).round(1)}%"
      
      if success_rate >= 0.95
        log "✅ Stress test passed - success rate >= 95%"
        return true
      else
        log "❌ Stress test failed - success rate < 95%"
        return false
      end
    end
    
    return true
  end
end

# Node process handler
class NodeProcess
  getter id : String
  getter role : String
  getter port : Int32
  getter bootstrap_id : String
  getter test_id : String
  getter debug : Bool
  
  def initialize(@id, @role, @port, @bootstrap_id, @test_id, @debug = false)
  end
  
  def run
    puts "[#{@id}] Starting node with role: #{@role}"
    
    # Initialize network
    address = NodeAddress.new("localhost", @port, @id)
    network = NetworkNode.new(address)
    
    # Create node
    node = TestNode.new(@id, network, @role, @bootstrap_id)
    
    # Join the network
    if @role != "bootstrap"
      if !node.join_network
        puts "[#{@id}] Failed to join network, exiting"
        network.close
        exit(1)
      end
    else
      puts "[#{@id}] Bootstrap node ready"
    end
    
    # If this is the test node, run the connectivity test
    if @role == "test"
      # Wait a moment to ensure connections are stable
      sleep(3.seconds)
      
      # Get node count from the command line arg or use default
      node_count = 5 # Default
      message_count = 20 # Default
      wait_time = 30 # Default
      
      # Run the test
      if node.run_connectivity_test(node_count, message_count, wait_time)
        puts "[#{@id}] All tests passed!"
        exit(0)
      else
        puts "[#{@id}] Tests failed"
        exit(1)
      end
    end
    
    # Set up signal handlers
    Signal::INT.trap do
      puts "[#{@id}] Shutting down..."
      network.close
      exit(0)
    end
    
    Signal::TERM.trap do
      puts "[#{@id}] Received termination signal, shutting down..."
      network.close
      exit(0)
    end
    
    # Keep the process alive
    loop do
      sleep(1.seconds)
      # Check if network is still running
      break unless network.running
    end
    
    puts "[#{@id}] Network closed, exiting"
  end
end

# Parse command line arguments
config = TestConfig.new

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
  
  parser.on("-s SECONDS", "--stabilize=SECONDS", "Network stabilization time") do |seconds|
    config.stabilize_time = seconds.to_i
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
  
  parser.on("--port=PORT", "Node port") do |port|
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

# Main entry point
if config.run_mode == "node"
  # Register process ID for cleanup
  LAUNCHED_PIDS << Process.pid
  
  # Create and run node process
  process = NodeProcess.new(
    config.node_id, 
    config.node_role, 
    config.node_port, 
    config.bootstrap_id, 
    config.test_id, 
    config.debug
  )
  process.run
else
  # Run as coordinator
  runner = TestRunner.new(config)
  
  begin
    runner.launch_network
    # Test node will run the tests and exit
    
    # Wait for all processes to complete
    runner.processes.each(&.wait)
  ensure
    runner.shutdown
  end
end
