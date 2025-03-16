require "../src/gossip"
require "../src/debug"  # Import the debug macro

# Demo node class with enhanced message display and status information
class DemoNode < Node
  @input_channel = Channel(String).new(10)
  @operation_in_progress = false
  @operation_mutex = Mutex.new
  
  def handle_broadcast(message : BroadcastMessage)
    if !@received_messages.includes?(message.message_id)
      @messages_mutex.synchronize do
        @received_messages << message.message_id
        @message_contents[message.message_id] = message.content
      end

      # Print received message with clear formatting - this is user-facing output
      # so we keep it on STDOUT
      puts "\n\033[32m[#{Time.utc}] Message from #{message.sender}:\033[0m"
      puts "\033[34m#{message.content}\033[0m"
      print_prompt

      # Forward to others as per normal gossip protocol
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
          if rand < @lazy_push_probability
            lazy_msg = LazyPushMessage.new(@id, message.message_id)
            send_message(node, lazy_msg)
          else
            forward_msg = BroadcastMessage.new(@id, message.message_id, message.content)
            send_message(node, forward_msg)
          end
        rescue ex
          debug_log "Node #{@id}: Failed to forward message to #{node}: #{ex.message}"
          handle_node_failure(node)
        end
      end
    end
  end

  def handle_join(message : Join)
    super
    print_status
    print_prompt
  end

  def print_status
    # Print status is user-facing output so keep on STDOUT
    puts "\n\033[33mNetwork Status:\033[0m"
    @views_mutex.synchronize do
      puts "  Active connections: #{active_view.size}/#{MAX_ACTIVE}"
      puts "  Active nodes: #{active_view.to_a.join(", ")}"
      puts "  Passive nodes: #{passive_view.to_a.join(", ")}"
    end
    
    @failures_mutex.synchronize do
      puts "  Failed nodes: #{@failed_nodes.to_a.join(", ")}" unless @failed_nodes.empty?
    end
    
    @messages_mutex.synchronize do
      puts "  Messages received: #{@received_messages.size}"
      puts "  Missing messages: #{@missing_messages.keys.size}"
    end
  end

  def print_prompt
    @operation_mutex.synchronize do
      unless @operation_in_progress
        print "\n\033[36mEnter message (or commands: /status, /help, /nodes, /quit):\033[0m "
        STDOUT.flush
      end
    end
  end
  
  # Asynchronous broadcast with timeout
  def async_broadcast(message : String)
    @operation_mutex.synchronize do
      @operation_in_progress = true
    end
    print "\n\033[33mSending message...\033[0m"
    
    # Create a channel to wait for completion
    done_channel = Channel(Bool).new(1)
    
    spawn do
      begin
        message_id = broadcast(message)
        puts "\n\033[32mMessage sent successfully (ID: #{message_id})!\033[0m"
        done_channel.send(true)
      rescue ex
        puts "\n\033[31mError sending message: #{ex.message}\033[0m"
        done_channel.send(false)
      end
    end
    
    # Set timeout to avoid blocking indefinitely
    spawn do
      sleep 5.seconds
      done_channel.send(false) rescue nil
    end
    
    # Wait for completion or timeout
    success = done_channel.receive
    
    @operation_mutex.synchronize do
      @operation_in_progress = false
    end
    print_prompt
    success
  end
  
  # Start input processing fiber
  def start_input_processing
    spawn do
      while @network.running
        input = @input_channel.receive
        process_input(input)
      end
    end
  end
  
  # Queue input for processing
  def queue_input(input : String)
    @input_channel.send(input) rescue nil
  end
  
  # Process input commands
  private def process_input(input : String)
    case input.strip
    when "/status"
      print_status
      print_prompt
    when "/help"
      print_help
      print_prompt
    when "/nodes"
      print_nodes_detail
      print_prompt
    when "/quit"
      puts "\nShutting down..."
      @network.close
    else
      return if input.strip.empty?
      async_broadcast(input)
    end
  end
  
  # More detailed node information
  private def print_nodes_detail
    puts "\n\033[33mActive Nodes:\033[0m"
    @views_mutex.synchronize do
      if @active_view.empty?
        puts "  No active connections"
      else
        @active_view.each do |node|
          status = "\033[32mConnected\033[0m"
          begin
            unless @network.test_connection(node)
              status = "\033[33mUnreachable\033[0m"
            end
          rescue
            status = "\033[31mFailed\033[0m"
          end
          puts "  #{node} - #{status}"
        end
      end
    end
    
    puts "\n\033[33mPassive Nodes:\033[0m"
    @views_mutex.synchronize do
      if @passive_view.empty?
        puts "  No passive connections"
      else
        puts "  #{@passive_view.to_a.join(", ")}"
      end
    end
  end
end

def print_help
  puts <<-HELP
  
  \033[33mAvailable commands:\033[0m
    /status  - Show current network status
    /nodes   - Show detailed node connection status
    /help    - Show this help message
    /quit    - Exit the program
    
  Any other input will be broadcast as a message to all connected nodes.
  HELP
end

# Asynchronous input handler
def handle_input_async(node : DemoNode)
  spawn do
    while node.network.running
      if input = gets
        node.queue_input(input)
      else
        break
      end
    end
  end
end

# Command line argument parsing
case ARGV[0]?
when "bootstrap"
  # Start the bootstrap node
  address = NodeAddress.new("localhost", 7001, "node1@localhost:7001")
  network = NetworkNode.new(address)
  node = DemoNode.new("node1@localhost:7001", network)
  puts "\n\033[32mBootstrap node started at localhost:7001\033[0m"
  print_help
  node.print_status

  # Start asynchronous input processing
  node.start_input_processing
  handle_input_async(node)
  
  # Keep main fiber alive
  while node.network.running
    sleep 1
  end

  # Clean shutdown
  network.close
when "join"
  if ARGV.size < 3
    puts "Usage: crystal run examples/demo.cr join <port> <node_id>"
    exit 1
  end

  port = ARGV[1].to_i
  node_id = ARGV[2]
  full_id = "#{node_id}@localhost:#{port}"
  address = NodeAddress.new("localhost", port, full_id)
  network = NetworkNode.new(address)
  node = DemoNode.new(full_id, network)

  # Join the network through the bootstrap node
  puts "\n\033[32mJoining network as #{full_id}...\033[0m"
  begin
    join_msg = Join.new(full_id)
    network.send_message("node1@localhost:7001", join_msg)
    print_help
    node.print_status
  rescue ex
    puts "\n\033[31mFailed to join network: #{ex.message}\033[0m"
    puts "Is the bootstrap node running?"
    network.close
    exit 1
  end

  # Start asynchronous input processing
  node.start_input_processing
  handle_input_async(node)
  
  # Keep main fiber alive
  while node.network.running
    sleep 1
  end

  # Clean shutdown
  network.close
when "help", "-h", "--help"
  puts <<-USAGE
  Usage:
    crystal run examples/demo.cr bootstrap                  # Start bootstrap node
    crystal run examples/demo.cr join <port> <node_id>      # Join network
    crystal run examples/demo.cr help                       # Show this help

  Example:
    # In terminal 1:
    crystal run examples/demo.cr bootstrap

    # In terminal 2:
    crystal run examples/demo.cr join 7002 node2

    # In terminal 3:
    crystal run examples/demo.cr join 7003 node3
  USAGE
else
  puts "Unknown command. Use 'crystal run examples/demo.cr help' for usage information."
  exit 1
end
