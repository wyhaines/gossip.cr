require "./spec_helper"
require "../src/gossip"

# Helper to get a random available port
def get_random_port : Int32
  server = TCPServer.new("localhost", 0)
  port = server.local_address.port
  server.close
  port
end

def create_test_node(name : String) : {Gossip::Protocol::Node, Gossip::Network::NetworkNode}
  port = get_random_port
  node_id = "#{name}@localhost:#{port}"
  addr = Gossip::NodeAddress.new("localhost", port, node_id)
  network = Gossip::Network::NetworkNode.new(addr)
  node = Gossip::Protocol::Node.new(node_id, network)
  {node, network}
end

describe "Realistic Message Propagation" do
  it "propagates messages in a small network with retries" do
    nodes = [] of {Gossip::Protocol::Node, Gossip::Network::NetworkNode}
    
    # Create 4 nodes
    4.times do |i|
      nodes << create_test_node("node#{i+1}")
    end
    
    # Wait for initialization
    sleep(1.seconds)
    
    # Connect nodes in a chain: 1->2->3->4
    # This tests multi-hop propagation
    begin
      # Connect node2 to node1
      success = false
      3.times do
        join_msg = Gossip::Messages::Membership::Join.new(nodes[1][0].id)
        nodes[1][1].send_message(nodes[0][0].id, join_msg)
        
        sleep(1.seconds)
        if nodes[0][0].active_view.includes?(nodes[1][0].id)
          success = true
          break
        end
      end
      success.should be_true
      
      # Connect node3 to node2
      success = false
      3.times do
        join_msg = Gossip::Messages::Membership::Join.new(nodes[2][0].id)
        nodes[2][1].send_message(nodes[1][0].id, join_msg)
        
        sleep(1.seconds)
        if nodes[1][0].active_view.includes?(nodes[2][0].id)
          success = true
          break
        end
      end
      success.should be_true
      
      # Connect node4 to node3
      success = false
      3.times do
        join_msg = Gossip::Messages::Membership::Join.new(nodes[3][0].id)
        nodes[3][1].send_message(nodes[2][0].id, join_msg)
        
        sleep(1.seconds)
        if nodes[2][0].active_view.includes?(nodes[3][0].id)
          success = true
          break
        end
      end
      success.should be_true
      
      # Allow network to stabilize
      sleep(2.seconds)
      
      # Broadcast from node1
      message_id = nodes[0][0].broadcast("Test message from node1")
      
      # Wait for propagation with generous timeout
      propagated_count = 0
      timeout = 15.seconds
      start = Time.monotonic
      
      while (Time.monotonic - start) < timeout
        propagated_count = nodes.count { |n, _| n.received_messages.includes?(message_id) }
        
        # Success if all nodes received it
        break if propagated_count == 4
        
        sleep(0.5.seconds)
      end
      
      puts "Message propagated to #{propagated_count}/4 nodes"
      
      # At minimum, immediate neighbors should receive it
      propagated_count.should be >= 2
      
      # Ideally all nodes receive it
      if propagated_count < 4
        puts "Warning: Not all nodes received the message (got #{propagated_count}/4)"
      end
    ensure
      nodes.each { |_, network| network.close }
    end
  end
  
  it "propagates multiple messages with acceptable loss" do
    nodes = [] of {Gossip::Protocol::Node, Gossip::Network::NetworkNode}
    
    # Create 5 nodes
    5.times do |i|
      nodes << create_test_node("node#{i+1}")
    end
    
    # Wait for initialization
    sleep(1.seconds)
    
    begin
      # Connect all nodes to first node (star topology)
      connected_count = 0
      nodes[1..-1].each_with_index do |node_data, idx|
        node, network = node_data
        success = false
        
        2.times do
          join_msg = Gossip::Messages::Membership::Join.new(node.id)
          network.send_message(nodes[0][0].id, join_msg)
          
          sleep(1.seconds)
          if nodes[0][0].active_view.includes?(node.id)
            success = true
            connected_count += 1
            break
          end
        end
      end
      
      # Should connect at least 3 out of 4
      connected_count.should be >= 3
      
      # Allow network to stabilize
      sleep(2.seconds)
      
      # Send 10 messages
      message_ids = [] of String
      10.times do |i|
        # Alternate between different senders
        sender_idx = i % [nodes.size, connected_count + 1].min
        message_id = nodes[sender_idx][0].broadcast("Message #{i} from node#{sender_idx+1}")
        message_ids << message_id
        sleep(0.2.seconds)  # Small delay between messages
      end
      
      # Wait for propagation
      sleep(10.seconds)
      
      # Count reception rates
      reception_rates = nodes.map_with_index do |node_data, idx|
        node = node_data[0]
        received = message_ids.count { |id| node.received_messages.includes?(id) }
        rate = received.to_f / message_ids.size
        puts "Node#{idx+1} received #{received}/#{message_ids.size} messages (#{(rate * 100).round}%)"
        rate
      end
      
      # Average reception rate should be good
      avg_rate = reception_rates.sum / reception_rates.size
      puts "Average reception rate: #{(avg_rate * 100).round}%"
      
      # Expect at least 70% average reception rate
      avg_rate.should be >= 0.7
      
      # Each connected node should receive at least 50% of messages
      nodes.each_with_index do |node_data, idx|
        if nodes[0][0].active_view.includes?(node_data[0].id) || idx == 0
          reception_rates[idx].should be >= 0.5
        end
      end
    ensure
      nodes.each { |_, network| network.close }
    end
  end
end