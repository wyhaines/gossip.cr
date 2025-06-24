require "./spec_helper"
require "../src/gossip"

# Helper to get a random available port
def get_random_port : Int32
  server = TCPServer.new("localhost", 0)
  port = server.local_address.port
  server.close
  port
end

describe "Connection Establishment" do
  it "establishes bidirectional connection via Join/InitViews" do
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
    
    # Wait for initialization
    sleep(1.seconds)
    
    # Node2 joins via node1 (simulating what network_test does)
    success = false
    3.times do |attempt|
      begin
        join_msg = Gossip::Messages::Membership::Join.new(node2_id)
        network2.send_message(node1_id, join_msg)
        puts "Join message sent (attempt #{attempt + 1})"
        
        # Wait for connection establishment
        timeout = 3.seconds
        start = Time.monotonic
        
        while (Time.monotonic - start) < timeout
          if node2.active_view.includes?(node1_id)
            success = true
            break
          end
          sleep(0.1.seconds)
        end
        
        break if success
      rescue ex
        puts "Attempt #{attempt + 1} failed: #{ex.message}"
      end
      
      sleep(1.seconds) # Wait before retry
    end
    
    # Check final state
    puts "Node1 active view: #{node1.active_view.to_a}"
    puts "Node2 active view: #{node2.active_view.to_a}"
    
    # Verify connections
    success.should be_true
    node1.active_view.should contain(node2_id)
    node2.active_view.should contain(node1_id)
    
    # Clean up
    network1.close
    network2.close
  end
  
  it "propagates messages after connection" do
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
    
    # Establish connection with retries
    success = false
    3.times do
      begin
        join_msg = Gossip::Messages::Membership::Join.new(node2_id)
        network2.send_message(node1_id, join_msg)
        
        # Wait for bidirectional connection
        timeout = 3.seconds
        start = Time.monotonic
        
        while (Time.monotonic - start) < timeout
          if node1.active_view.includes?(node2_id) && node2.active_view.includes?(node1_id)
            success = true
            break
          end
          sleep(0.1.seconds)
        end
        
        break if success
      rescue ex
        # Retry
      end
      
      sleep(0.5.seconds)
    end
    
    success.should be_true
    
    # Now test message propagation
    message_id = node1.broadcast("Test message")
    
    # Wait for propagation
    received = false
    timeout = 5.seconds
    start = Time.monotonic
    
    while (Time.monotonic - start) < timeout
      if node2.received_messages.includes?(message_id)
        received = true
        break
      end
      sleep(0.1.seconds)
    end
    
    received.should be_true
    
    # Clean up
    network1.close
    network2.close
  end
end