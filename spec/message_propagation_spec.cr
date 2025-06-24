require "./spec_helper"
require "../src/gossip"

# Test node that tracks message propagation details
class PropagationTestNode < Gossip::Protocol::Node
  # Track message propagation timing
  property message_receive_times = Hash(String, Time).new
  property forwarded_messages = Set(String).new
  property lazy_push_sent = Set(String).new
  property message_requests = Hash(String, Array(String)).new
  
  getter views_mutex : Mutex
  getter messages_mutex : Mutex
  getter failures_mutex : Mutex
  
  # Override to track timing
  def handle_broadcast(message : Gossip::Messages::Broadcast::BroadcastMessage)
    @messages_mutex.synchronize do
      unless @received_messages.includes?(message.message_id)
        @message_receive_times[message.message_id] = Time.utc
      end
    end
    super(message)
  end
  
  # Override send_message to track what we forward
  def send_message(to : String, message : Gossip::Messages::Base::Message)
    case message
    when Gossip::Messages::Broadcast::BroadcastMessage
      @forwarded_messages << message.message_id
    when Gossip::Messages::Broadcast::LazyPushMessage
      @lazy_push_sent << message.message_id
    when Gossip::Messages::Broadcast::MessageRequest
      @message_requests[message.message_id] ||= [] of String
      @message_requests[message.message_id] << to
    end
    
    super(to, message)
  end
  
  # Helper to check if message was received
  def has_message?(message_id : String) : Bool
    @messages_mutex.synchronize do
      @received_messages.includes?(message_id)
    end
  end
  
  # Helper to get propagation delay
  def propagation_delay(message_id : String, start_time : Time) : Float64?
    @messages_mutex.synchronize do
      if time = @message_receive_times[message_id]?
        (time - start_time).total_seconds
      end
    end
  end
end

# Helper to get a random available port
def get_random_port : Int32
  server = TCPServer.new("localhost", 0)
  port = server.local_address.port
  server.close
  port
end

def create_propagation_test_node(base_id : String, port : Int32? = nil) : {PropagationTestNode, Gossip::Network::NetworkNode}
  port ||= get_random_port
  node_id = "#{base_id}@localhost:#{port}"
  addr = Gossip::NodeAddress.new("localhost", port, node_id)
  network = Gossip::Network::NetworkNode.new(addr)
  node = PropagationTestNode.new(node_id, network)
  {node, network}
end

describe "Message Propagation" do
  describe "Basic Broadcast" do
    it "propagates message to all nodes in small network" do
      nodes_data = [] of {PropagationTestNode, Gossip::Network::NetworkNode}
      
      begin
        # Create 5 nodes
        5.times do |i|
          node, network = create_propagation_test_node("node#{i + 1}")
          nodes_data << {node, network}
        end
        
        # Connect all nodes to first node
        (1...nodes_data.size).each do |i|
          node, network = nodes_data[i]
          join_msg = Gossip::Messages::Membership::Join.new(node.id)
          network.send_message(nodes_data[0][0].id, join_msg)
          sleep(0.05.seconds)
        end
        
        # Wait for network to stabilize
        sleep(0.5.seconds)
        
        # Broadcast from first node
        start_time = Time.utc
        message_id = nodes_data[0][0].broadcast("Test message")
        
        # Wait for propagation
        timeout = 2.seconds
        start = Time.monotonic
        while (Time.monotonic - start) < timeout
          if nodes_data.all? { |n, _| n.has_message?(message_id) }
            break
          end
          sleep(0.01.seconds)
        end
        
        # All nodes should receive the message
        nodes_data.each do |node, _|
          node.has_message?(message_id).should be_true
        end
        
        # Check propagation times (should be fast for small network)
        nodes_data[1..].each do |node, _|
          delay = node.propagation_delay(message_id, start_time)
          delay.should_not be_nil
          delay.not_nil!.should be < 1.0  # Should propagate in under 1 second
        end
      ensure
        nodes_data.each { |_, network| network.close rescue nil }
      end
    end
    
    it "handles high message volume without loss" do
      nodes_data = [] of {PropagationTestNode, Gossip::Network::NetworkNode}
      message_count = 50
      
      begin
        # Create 5 nodes
        5.times do |i|
          node, network = create_propagation_test_node("node#{i + 1}")
          nodes_data << {node, network}
        end
        
        # Connect in a chain
        (1...nodes_data.size).each do |i|
          node, network = nodes_data[i]
          join_msg = Gossip::Messages::Membership::Join.new(node.id)
          network.send_message(nodes_data[i-1][0].id, join_msg)
          sleep(0.05.seconds)
        end
        
        sleep(0.5.seconds)
        
        # Send many messages rapidly
        message_ids = [] of String
        message_count.times do |i|
          # Alternate between different source nodes
          source_idx = i % nodes_data.size
          message_id = nodes_data[source_idx][0].broadcast("Message #{i}")
          message_ids << message_id
          sleep(0.001.seconds)  # Very short delay between messages
        end
        
        # Wait for propagation with extended timeout for high volume
        timeout = 10.seconds
        start = Time.monotonic
        all_received = false
        
        while (Time.monotonic - start) < timeout
          received_counts = nodes_data.map { |n, _| 
            message_ids.count { |id| n.has_message?(id) }
          }
          
          if received_counts.all? { |count| count == message_count }
            all_received = true
            break
          end
          
          sleep(0.1.seconds)
        end
        
        # Check results
        all_received.should be_true
        
        # Verify each node received all messages
        nodes_data.each_with_index do |(node, _), idx|
          received = message_ids.count { |id| node.has_message?(id) }
          received.should eq(message_count)
        end
      ensure
        nodes_data.each { |_, network| network.close rescue nil }
      end
    end
  end
  
  describe "Lazy Push Recovery" do
    it "recovers missing messages via lazy push" do
      nodes_data = [] of {PropagationTestNode, Gossip::Network::NetworkNode}
      
      begin
        # Create 3 nodes
        3.times do |i|
          node, network = create_propagation_test_node("node#{i + 1}")
          nodes_data << {node, network}
        end
        
        # Connect them
        join_msg = Gossip::Messages::Membership::Join.new(nodes_data[1][0].id)
        nodes_data[1][1].send_message(nodes_data[0][0].id, join_msg)
        sleep(0.1.seconds)
        
        join_msg = Gossip::Messages::Membership::Join.new(nodes_data[2][0].id)
        nodes_data[2][1].send_message(nodes_data[1][0].id, join_msg)
        sleep(0.1.seconds)
        
        # Set high lazy push probability
        nodes_data.each { |n, _| n.lazy_push_probability = 0.9 }
        
        # Broadcast from node1
        message_id = nodes_data[0][0].broadcast("Lazy push test")
        
        # Wait for lazy push and recovery
        timeout = 5.seconds
        start = Time.monotonic
        while (Time.monotonic - start) < timeout
          if nodes_data.all? { |n, _| n.has_message?(message_id) }
            break
          end
          sleep(0.1.seconds)
        end
        
        # All nodes should eventually receive the message
        nodes_data.each do |node, _|
          node.has_message?(message_id).should be_true
        end
        
        # Check that lazy push was used
        lazy_push_used = nodes_data.any? { |n, _| n.lazy_push_sent.includes?(message_id) }
        lazy_push_used.should be_true
        
        # Check that message requests were sent
        request_sent = nodes_data.any? { |n, _| n.message_requests.has_key?(message_id) }
        request_sent.should be_true
      ensure
        nodes_data.each { |_, network| network.close rescue nil }
      end
    end
  end
  
  describe "Propagation with Failures" do
    it "continues propagation despite node failures" do
      nodes_data = [] of {PropagationTestNode, Gossip::Network::NetworkNode}
      
      begin
        # Create 6 nodes in a more complex topology
        6.times do |i|
          node, network = create_propagation_test_node("node#{i + 1}")
          nodes_data << {node, network}
        end
        
        # Create a mesh topology
        # 0 connects to 1, 2
        # 1 connects to 0, 3, 4
        # 2 connects to 0, 5
        # etc.
        connections = [
          [1, 2],     # node 0
          [0, 3, 4],  # node 1
          [0, 5],     # node 2
          [1],        # node 3
          [1],        # node 4
          [2]         # node 5
        ]
        
        connections.each_with_index do |targets, source_idx|
          targets.each do |target_idx|
            next if source_idx >= target_idx  # Avoid duplicate connections
            
            join_msg = Gossip::Messages::Membership::Join.new(nodes_data[target_idx][0].id)
            nodes_data[target_idx][1].send_message(nodes_data[source_idx][0].id, join_msg)
            sleep(0.05.seconds)
          end
        end
        
        sleep(0.5.seconds)
        
        # Broadcast from node 0
        message_id = nodes_data[0][0].broadcast("Failure test message")
        
        # Wait a bit for initial propagation
        sleep(0.2.seconds)
        
        # Fail node 1 (critical bridge node)
        nodes_data[1][1].close
        
        # Mark node 1 as failed in other nodes
        [0, 3, 4].each do |idx|
          nodes_data[idx][0].handle_node_failure(nodes_data[1][0].id)
        end
        
        # Wait for message to still reach all alive nodes
        timeout = 5.seconds
        start = Time.monotonic
        alive_nodes = [0, 2, 3, 4, 5]
        
        while (Time.monotonic - start) < timeout
          if alive_nodes.all? { |idx| nodes_data[idx][0].has_message?(message_id) }
            break
          end
          sleep(0.1.seconds)
        end
        
        # All alive nodes should receive the message
        alive_nodes.each do |idx|
          nodes_data[idx][0].has_message?(message_id).should be_true
        end
      ensure
        nodes_data.each { |_, network| network.close rescue nil }
      end
    end
  end
  
  describe "Concurrent Broadcasts" do
    it "handles concurrent broadcasts from multiple nodes" do
      nodes_data = [] of {PropagationTestNode, Gossip::Network::NetworkNode}
      
      begin
        # Create 8 nodes
        8.times do |i|
          node, network = create_propagation_test_node("node#{i + 1}")
          nodes_data << {node, network}
        end
        
        # Connect in a ring with cross-connections
        nodes_data.each_with_index do |(node, network), i|
          # Connect to next node in ring
          next_idx = (i + 1) % nodes_data.size
          join_msg = Gossip::Messages::Membership::Join.new(node.id)
          network.send_message(nodes_data[next_idx][0].id, join_msg)
          
          # Connect to node across the ring
          across_idx = ((i + nodes_data.size // 2) % nodes_data.size).to_i
          if across_idx != i
            join_msg = Gossip::Messages::Membership::Join.new(node.id)
            network.send_message(nodes_data[across_idx][0].id, join_msg)
          end
          
          sleep(0.05.seconds)
        end
        
        sleep(0.5.seconds)
        
        # All nodes broadcast simultaneously
        message_ids = [] of String
        channel = Channel(String).new(nodes_data.size)
        
        nodes_data.each_with_index do |(node, _), i|
          spawn do
            msg_id = node.broadcast("Concurrent message from node#{i}")
            channel.send(msg_id)
          end
        end
        
        # Collect all message IDs
        nodes_data.size.times do
          message_ids << channel.receive
        end
        
        # Wait for all messages to propagate
        timeout = 10.seconds
        start = Time.monotonic
        all_propagated = false
        
        while (Time.monotonic - start) < timeout
          if nodes_data.all? { |n, _| 
            message_ids.all? { |msg_id| n.has_message?(msg_id) }
          }
            all_propagated = true
            break
          end
          sleep(0.1.seconds)
        end
        
        # All nodes should receive all messages
        all_propagated.should be_true
        
        nodes_data.each_with_index do |(node, _), node_idx|
          message_ids.each_with_index do |msg_id, msg_idx|
            node.has_message?(msg_id).should be_true
          end
        end
      ensure
        nodes_data.each { |_, network| network.close rescue nil }
      end
    end
  end
  
  describe "Message Recovery" do
    it "recovers missing messages through periodic requests" do
      nodes_data = [] of {PropagationTestNode, Gossip::Network::NetworkNode}
      
      begin
        # Create 4 nodes
        4.times do |i|
          node, network = create_propagation_test_node("node#{i + 1}")
          nodes_data << {node, network}
        end
        
        # Connect first 3 nodes
        (1..2).each do |i|
          join_msg = Gossip::Messages::Membership::Join.new(nodes_data[i][0].id)
          nodes_data[i][1].send_message(nodes_data[0][0].id, join_msg)
          sleep(0.05.seconds)
        end
        
        sleep(0.3.seconds)
        
        # Broadcast while node 4 is disconnected
        message_id = nodes_data[0][0].broadcast("Message before join")
        
        sleep(0.2.seconds)
        
        # Now connect node 4
        join_msg = Gossip::Messages::Membership::Join.new(nodes_data[3][0].id)
        nodes_data[3][1].send_message(nodes_data[2][0].id, join_msg)
        
        # Send a lazy push to node 4 for the old message
        lazy_msg = Gossip::Messages::Broadcast::LazyPushMessage.new(nodes_data[2][0].id, message_id)
        nodes_data[2][0].send_message(nodes_data[3][0].id, lazy_msg)
        
        # Wait for recovery (should happen via message request/response)
        timeout = Gossip::Protocol::Config::REQUEST_RETRY_INTERVAL + 2
        start = Time.monotonic
        
        while (Time.monotonic - start) < timeout.seconds
          if nodes_data[3][0].has_message?(message_id)
            break
          end
          sleep(0.1.seconds)
        end
        
        # Node 4 should eventually receive the message
        nodes_data[3][0].has_message?(message_id).should be_true
      ensure
        nodes_data.each { |_, network| network.close rescue nil }
      end
    end
  end
end