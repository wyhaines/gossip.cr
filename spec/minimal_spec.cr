require "./spec_helper"
require "../src/gossip"

describe "Minimal Gossip Test" do
  it "establishes basic connectivity" do
    # Create nodes directly without helpers
    addr1 = Gossip::NodeAddress.new("localhost", 8001, "node1@localhost:8001")
    network1 = Gossip::Network::NetworkNode.new(addr1)
    node1 = Gossip::Protocol::Node.new("node1@localhost:8001", network1)
    
    addr2 = Gossip::NodeAddress.new("localhost", 8002, "node2@localhost:8002")
    network2 = Gossip::Network::NetworkNode.new(addr2)
    node2 = Gossip::Protocol::Node.new("node2@localhost:8002", network2)
    
    # Give time to start
    sleep(2.seconds)
    
    # Send join message
    puts "Node2 joining node1..."
    join_msg = Gossip::Messages::Membership::Join.new("node2@localhost:8002")
    begin
      network2.send_message("node1@localhost:8001", join_msg)
      puts "Join message sent successfully"
    rescue ex
      puts "Failed to send join: #{ex.message}"
    end
    
    # Wait for processing
    sleep(3.seconds)
    
    # Check views
    puts "Node1 active view: #{node1.active_view.to_a}"
    puts "Node1 passive view: #{node1.passive_view.to_a}"
    puts "Node2 active view: #{node2.active_view.to_a}"
    puts "Node2 passive view: #{node2.passive_view.to_a}"
    
    # Basic assertion - at least one should have a connection
    (node1.active_view.size + node2.active_view.size).should be > 0
    
    # Cleanup
    network1.close
    network2.close
    sleep(0.5.seconds)
  end
end