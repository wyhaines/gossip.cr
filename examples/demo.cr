require "../src/gossip"

# Demo node class with enhanced message display
class DemoNode < Node
  def handle_broadcast(message : BroadcastMessage)
    if !@received_messages.includes?(message.message_id)
      @received_messages << message.message_id
      @message_contents[message.message_id] = message.content

      puts "\nReceived broadcast from #{message.sender}: #{message.content}"
      print "\nEnter message to broadcast (or press Enter to continue): "

      @active_view.each do |node|
        if node != message.sender
          if rand < @lazy_push_probability
            lazy_msg = LazyPushMessage.new(@id, message.message_id)
            send_message(node, lazy_msg)
          else
            send_message(node, message)
          end
        end
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
  puts "Bootstrap node started at localhost:7001"

  # Start REPL for sending messages
  puts "\nEnter messages to broadcast (Ctrl+C to exit):"
  while message = gets
    next if message.empty?
    node.broadcast(message)
  end
when "join"
  if ARGV.size < 3
    puts "Usage: crystal run examples/demo.cr join <port> <node_id>"
    exit 1
  end

  port = ARGV[1].to_i
  node_id = ARGV[2]
  address = NodeAddress.new("localhost", port, "#{node_id}@localhost:#{port}")
  network = NetworkNode.new(address)
  node = DemoNode.new("#{node_id}@localhost:#{port}", network)

  # Join the network through the bootstrap node
  join_msg = Join.new(address.to_s)
  network.send_message("node1@localhost:7001", join_msg)
  puts "Node #{node_id} joined network through bootstrap node"

  # Start REPL for sending messages
  puts "\nEnter messages to broadcast (Ctrl+C to exit):"
  while message = gets
    next if message.empty?
    node.broadcast(message)
  end
when "help", "-h", "--help"
  puts <<-HELP
  Usage:
    crystal run examples/demo.cr bootstrap                  # Start bootstrap node
    crystal run examples/demo.cr join <port> <node_id>     # Join network and start REPL
    crystal run examples/demo.cr help                      # Show this help
  HELP
else
  puts "Unknown command. Use 'crystal run examples/demo.cr help' for usage information."
  exit 1
end
