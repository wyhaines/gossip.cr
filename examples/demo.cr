require "../src/gossip"

# Demo node class with enhanced message display and status information
class DemoNode < Node
  def handle_broadcast(message : BroadcastMessage)
    if !@received_messages.includes?(message.message_id)
      @received_messages << message.message_id
      @message_contents[message.message_id] = message.content

      # Print received message with clear formatting
      puts "\n\033[32m[#{Time.utc}] Message from #{message.sender}:\033[0m"
      puts "\033[34m#{message.content}\033[0m"
      print_prompt

      # Forward to others as per normal gossip protocol
      @active_view.each do |node|
        next if node == message.sender
        if rand < @lazy_push_probability
          lazy_msg = LazyPushMessage.new(@id, message.message_id)
          send_message(node, lazy_msg)
        else
          forward_msg = BroadcastMessage.new(@id, message.message_id, message.content)
          send_message(node, forward_msg)
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
    puts "\n\033[33mNetwork Status:\033[0m"
    puts "  Active connections: #{active_view.size}/#{MAX_ACTIVE}"
    puts "  Active nodes: #{active_view.to_a.join(", ")}"
    puts "  Passive nodes: #{passive_view.to_a.join(", ")}"
  end

  def print_prompt
    print "\n\033[36mEnter message (or commands: /status, /help, /quit):\033[0m "
    STDOUT.flush
  end
end

def print_help
  puts <<-HELP
  Available commands:
    /status - Show current network status
    /help   - Show this help message
    /quit   - Exit the program
    
  Any other input will be broadcast as a message to all connected nodes.
  HELP
end

def handle_input(node : DemoNode, input : String)
  case input.strip
  when "/status"
    node.print_status
  when "/help"
    print_help
  when "/quit"
    puts "\nShutting down..."
    return false
  else
    return false if input.strip.empty?
    node.broadcast(input)
  end
  true
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

  # Start REPL for sending messages
  while node.network.running
    node.print_prompt
    if input = gets
      next if input.empty?
      break unless handle_input(node, input)
    else
      break
    end
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
  join_msg = Join.new(full_id)
  network.send_message("node1@localhost:7001", join_msg)

  print_help
  node.print_status

  # Start REPL for sending messages
  while node.network.running
    node.print_prompt
    if input = gets
      next if input.empty?
      break unless handle_input(node, input)
    else
      break
    end
  end

  # Clean shutdown
  network.close
when "help", "-h", "--help"
  puts <<-USAGE
  Usage:
    crystal run examples/demo.cr bootstrap                  # Start bootstrap node
    crystal run examples/demo.cr join <port> <node_id>     # Join network
    crystal run examples/demo.cr help                      # Show this help

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
