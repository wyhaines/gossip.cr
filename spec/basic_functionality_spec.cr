require "./spec_helper"
require "../src/gossip"

# Helper to get a random available port
def get_random_port : Int32
  server = TCPServer.new("localhost", 0)
  port = server.local_address.port
  server.close
  port
end

# Simple test to verify basic functionality
describe "Basic Gossip Functionality" do
  it "creates and starts a node successfully" do
    port = get_random_port
    node_id = "test@localhost:#{port}"
    addr = Gossip::NodeAddress.new("localhost", port, node_id)
    network = Gossip::Network::NetworkNode.new(addr)
    node = Gossip::Protocol::Node.new(node_id, network)
    
    # Verify node is initialized
    node.id.should eq(node_id)
    node.active_view.empty?.should be_true
    node.passive_view.empty?.should be_true
    
    # Verify network is running
    network.running.should be_true
    
    # Clean up
    network.close
  end
  
  it "establishes connection between two nodes" do
    # Create first node
    port1 = get_random_port
    node1_id = "node1@localhost:#{port1}"
    addr1 = Gossip::NodeAddress.new("localhost", port1, node1_id)
    network1 = Gossip::Network::NetworkNode.new(addr1)
    node1 = Gossip::Protocol::Node.new(node1_id, network1)
    
    # Create second node
    port2 = get_random_port
    node2_id = "node2@localhost:#{port2}"
    addr2 = Gossip::NodeAddress.new("localhost", port2, node2_id)
    network2 = Gossip::Network::NetworkNode.new(addr2)
    node2 = Gossip::Protocol::Node.new(node2_id, network2)
    
    # Give nodes time to start up
    sleep(0.5.seconds)
    
    # Node2 joins via node1
    puts "Node2 sending join to node1..."
    join_msg = Gossip::Messages::Membership::Join.new(node2_id)
    network2.send_message(node1_id, join_msg)
    
    # Wait for message processing with timeout
    timeout = 5.seconds
    start = Time.monotonic
    
    while (Time.monotonic - start) < timeout
      if node1.active_view.includes?(node2_id)
        break
      end
      sleep(0.1.seconds)
    end
    
    # Check if connection was established
    puts "Node1 active view: #{node1.active_view}"
    puts "Node2 active view: #{node2.active_view}"
    
    node1.active_view.should contain(node2_id)
    
    # Clean up
    network1.close
    network2.close
  end
  
  it "propagates a simple message" do
    # Create 3 nodes
    nodes = [] of {Gossip::Protocol::Node, Gossip::Network::NetworkNode}
    base_port = 10000 + Random.rand(1000)
    
    3.times do |i|
      port = base_port + i
      node_id = "node#{i+1}@localhost:#{port}"
      addr = Gossip::NodeAddress.new("localhost", port, node_id)
      network = Gossip::Network::NetworkNode.new(addr)
      node = Gossip::Protocol::Node.new(node_id, network)
      nodes << {node, network}
    end
    
    # Give nodes time to start
    sleep(2.seconds)
    
    # Connect all nodes to node1 for better connectivity
    # Node2 joins node1
    join_msg = Gossip::Messages::Membership::Join.new(nodes[1][0].id)
    nodes[1][1].send_message(nodes[0][0].id, join_msg)
    
    sleep(2.seconds)
    
    # Node3 joins node1 too
    join_msg = Gossip::Messages::Membership::Join.new(nodes[2][0].id)
    nodes[2][1].send_message(nodes[0][0].id, join_msg)
    
    sleep(3.seconds)
    
    # Verify connections
    puts "Node1 active view: #{nodes[0][0].active_view}"
    puts "Node2 active view: #{nodes[1][0].active_view}"
    puts "Node3 active view: #{nodes[2][0].active_view}"
    
    # Node1 should have both connections
    nodes[0][0].active_view.size.should eq(2)
    
    # Broadcast from node1
    message_id = nodes[0][0].broadcast("Test message")
    
    # Wait for propagation
    sleep(10.seconds)
    
    # Check which nodes received the message
    received_count = 0
    nodes.each_with_index do |(node, _), i|
      received = node.received_messages.includes?(message_id)
      puts "Node#{i+1} received: #{received}"
      received_count += 1 if received
    end
    
    # At least 2 nodes should receive (broadcaster + at least one other)
    received_count.should be >= 2
    
    # Node1 (broadcaster) should always have it
    nodes[0][0].received_messages.should contain(message_id)
    
    # Clean up
    nodes.each { |_, network| network.close }
  end
end