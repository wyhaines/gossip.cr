require "./spec_helper"
require "../src/gossip"

# Helper to get a random available port
def get_random_port : Int32
  server = TCPServer.new("localhost", 0)
  port = server.local_address.port
  server.close
  port
end

describe "Gossip Protocol Basic Operations" do
  it "allows nodes to join the network" do
    # Create bootstrap node
    port1 = get_random_port
    node1_id = "node1@localhost:#{port1}"
    addr1 = Gossip::NodeAddress.new("localhost", port1, node1_id)
    network1 = Gossip::Network::NetworkNode.new(addr1)
    node1 = Gossip::Protocol::Node.new(node1_id, network1)
    
    # Create joining node
    port2 = get_random_port
    node2_id = "node2@localhost:#{port2}"
    addr2 = Gossip::NodeAddress.new("localhost", port2, node2_id)
    network2 = Gossip::Network::NetworkNode.new(addr2)
    node2 = Gossip::Protocol::Node.new(node2_id, network2)
    
    # Wait for nodes to initialize
    sleep(1.seconds)
    
    # Node2 joins via node1
    join_msg = Gossip::Messages::Membership::Join.new(node2_id)
    network2.send_message(node1_id, join_msg)
    
    # Wait for join to process
    sleep(2.seconds)
    
    # Verify connection established
    node1.active_view.should contain(node2_id)
    
    # Clean up
    network1.close
    network2.close
  end
  
  it "broadcasts messages between connected nodes" do
    # Create two nodes
    port1 = get_random_port
    node1_id = "node1@localhost:#{port1}"
    addr1 = Gossip::NodeAddress.new("localhost", port1, node1_id)
    network1 = Gossip::Network::NetworkNode.new(addr1)
    node1 = Gossip::Protocol::Node.new(node1_id, network1)
    
    port2 = get_random_port
    node2_id = "node2@localhost:#{port2}"
    addr2 = Gossip::NodeAddress.new("localhost", port2, node2_id)
    network2 = Gossip::Network::NetworkNode.new(addr2)
    node2 = Gossip::Protocol::Node.new(node2_id, network2)
    
    # Wait for initialization
    sleep(1.seconds)
    
    # Connect nodes
    join_msg = Gossip::Messages::Membership::Join.new(node2_id)
    network2.send_message(node1_id, join_msg)
    
    # Wait for connection
    sleep(3.seconds)
    
    # Verify connection before broadcasting
    node1.active_view.should contain(node2_id)
    
    # Broadcast from node1
    message_id = node1.broadcast("Hello from node1")
    
    # Wait for propagation
    sleep(5.seconds)
    
    # Both nodes should have the message
    node1.received_messages.should contain(message_id)
    node2.received_messages.should contain(message_id)
    
    # Clean up
    network1.close
    network2.close
  end
  
  it "handles multiple node joins" do
    nodes = [] of {Gossip::Protocol::Node, Gossip::Network::NetworkNode}
    
    # Create bootstrap node
    port1 = get_random_port
    node1_id = "node1@localhost:#{port1}"
    addr1 = Gossip::NodeAddress.new("localhost", port1, node1_id)
    network1 = Gossip::Network::NetworkNode.new(addr1)
    node1 = Gossip::Protocol::Node.new(node1_id, network1)
    nodes << {node1, network1}
    
    sleep(1.seconds)
    
    # Create and join 3 more nodes
    3.times do |i|
      port = get_random_port
      node_id = "node#{i+2}@localhost:#{port}"
      addr = Gossip::NodeAddress.new("localhost", port, node_id)
      network = Gossip::Network::NetworkNode.new(addr)
      node = Gossip::Protocol::Node.new(node_id, network)
      nodes << {node, network}
      
      # Join via bootstrap
      join_msg = Gossip::Messages::Membership::Join.new(node_id)
      network.send_message(node1_id, join_msg)
      
      sleep(1.seconds)
    end
    
    # Wait for network to stabilize
    sleep(2.seconds)
    
    # Bootstrap should have connections
    node1.active_view.size.should be > 0
    node1.active_view.size.should be <= Gossip::Protocol::Config::MAX_ACTIVE
    
    # Bootstrap should have seen most joins
    # Some might be displaced to passive view
    total_known = node1.active_view.size + node1.passive_view.size
    total_known.should be >= 2 # knows about at least 2 other nodes
    
    # Clean up
    nodes.each { |_, network| network.close }
  end
end