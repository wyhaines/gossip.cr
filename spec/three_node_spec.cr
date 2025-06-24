require "./spec_helper"
require "../src/gossip"

describe "Three Node Gossip Test" do
  it "propagates messages across three nodes" do
    # Use fixed ports to avoid conflicts
    nodes = [] of {Gossip::Protocol::Node, Gossip::Network::NetworkNode}
    base_port = 9000 + Random.rand(1000)
    
    # Create 3 nodes
    3.times do |i|
      port = base_port + i
      node_id = "node#{i+1}@localhost:#{port}"
      addr = Gossip::NodeAddress.new("localhost", port, node_id)
      network = Gossip::Network::NetworkNode.new(addr)
      node = Gossip::Protocol::Node.new(node_id, network)
      nodes << {node, network}
    end
    
    # Give time to initialize
    sleep(2.seconds)
    
    # Connect node2 to node1
    puts "\n=== Connecting node2 to node1 ==="
    join_msg = Gossip::Messages::Membership::Join.new(nodes[1][0].id)
    nodes[1][1].send_message(nodes[0][0].id, join_msg)
    
    # Wait for connection
    sleep(3.seconds)
    
    # Connect node3 to node1
    puts "\n=== Connecting node3 to node1 ==="
    join_msg = Gossip::Messages::Membership::Join.new(nodes[2][0].id)
    nodes[2][1].send_message(nodes[0][0].id, join_msg)
    
    # Wait for connection
    sleep(3.seconds)
    
    # Check connections
    puts "\n=== Connection Status ==="
    nodes.each_with_index do |(node, _), i|
      puts "Node#{i+1} active: #{node.active_view.to_a}"
      puts "Node#{i+1} passive: #{node.passive_view.to_a}"
    end
    
    # Node1 should have both connections
    nodes[0][0].active_view.size.should eq(2)
    
    # Broadcast from node1
    puts "\n=== Broadcasting from node1 ==="
    message_id = nodes[0][0].broadcast("Test message from node1")
    puts "Broadcast message ID: #{message_id}"
    
    # Wait for propagation
    sleep(5.seconds)
    
    # Check message receipt
    puts "\n=== Message Receipt Status ==="
    nodes.each_with_index do |(node, _), i|
      puts "Node#{i+1} received: #{node.received_messages.includes?(message_id)}"
      puts "Node#{i+1} messages: #{node.received_messages.to_a}"
    end
    
    # All nodes should receive
    nodes.each do |node, _|
      node.received_messages.should contain(message_id)
    end
    
    # Cleanup
    nodes.each { |_, network| network.close }
    sleep(0.5.seconds)
  end
end