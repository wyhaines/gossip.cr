require "./spec_helper"
require "../src/gossip"

# Helper to get a random available port
def get_random_port : Int32
  server = TCPServer.new("localhost", 0)
  port = server.local_address.port
  server.close
  port
end

# Extended test node for debugging
class DebugTestNode < Gossip::Protocol::Node
  property handled_messages = [] of Gossip::Messages::Base::Message
  property sent_messages = [] of {String, Gossip::Messages::Base::Message}
  
  def handle_message(message : Gossip::Messages::Base::Message)
    puts "[#{@id}] Handling #{message.type} from #{message.sender}"
    @handled_messages << message
    super(message)
  end
  
  def send_message(to : String, message : Gossip::Messages::Base::Message)
    puts "[#{@id}] Sending #{message.type} to #{to}"
    @sent_messages << {to, message}
    super(to, message)
  end
end

describe "Debug Protocol Issues" do
  it "traces message flow in join process" do
    # Create two nodes with debug logging
    port1 = get_random_port
    node1_id = "node1@localhost:#{port1}"
    addr1 = Gossip::NodeAddress.new("localhost", port1, node1_id)
    network1 = Gossip::Network::NetworkNode.new(addr1)
    node1 = DebugTestNode.new(node1_id, network1)
    
    port2 = get_random_port
    node2_id = "node2@localhost:#{port2}"
    addr2 = Gossip::NodeAddress.new("localhost", port2, node2_id)
    network2 = Gossip::Network::NetworkNode.new(addr2)
    node2 = DebugTestNode.new(node2_id, network2)
    
    # Wait for nodes to start
    sleep(0.5.seconds)
    
    puts "\n=== Starting Join Process ==="
    puts "Node1 ID: #{node1_id}"
    puts "Node2 ID: #{node2_id}"
    
    # Node2 joins via node1
    join_msg = Gossip::Messages::Membership::Join.new(node2_id)
    puts "\nNode2 sending Join message to Node1..."
    network2.send_message(node1_id, join_msg)
    
    # Give a tiny bit of time for the connection to be established
    sleep(0.1.seconds)
    
    # Wait and check periodically
    5.times do |i|
      sleep(0.5.seconds)
      puts "\n=== After #{(i+1)*0.5} seconds ==="
      puts "Node1 handled messages: #{node1.handled_messages.map(&.type)}"
      puts "Node1 sent messages: #{node1.sent_messages.map { |to, msg| "#{msg.type} to #{to}" }}"
      puts "Node1 active view: #{node1.active_view}"
      puts "Node1 passive view: #{node1.passive_view}"
      
      puts "\nNode2 handled messages: #{node2.handled_messages.map(&.type)}"
      puts "Node2 sent messages: #{node2.sent_messages.map { |to, msg| "#{msg.type} to #{to}" }}"
      puts "Node2 active view: #{node2.active_view}"
      puts "Node2 passive view: #{node2.passive_view}"
      
      # Check if InitViews was received
      if node2.handled_messages.any? { |m| m.is_a?(Gossip::Messages::Membership::InitViews) }
        puts "\n✓ InitViews message was received by Node2"
        break
      end
    end
    
    # Final verification
    puts "\n=== Final State ==="
    node1.active_view.should contain(node2_id)
    
    # Check that InitViews was sent
    init_views_sent = node1.sent_messages.any? { |to, msg| 
      to == node2_id && msg.is_a?(Gossip::Messages::Membership::InitViews)
    }
    init_views_sent.should be_true
    
    # Check that InitViews was received
    init_views_received = node2.handled_messages.any? { |m| 
      m.is_a?(Gossip::Messages::Membership::InitViews)
    }
    init_views_received.should be_true
    
    # Clean up
    network1.close
    network2.close
  end
end