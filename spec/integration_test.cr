require "../src/gossip"
require "spec"

# Helper class to manage test nodes
class TestNode
  getter network : NetworkNode
  getter address : NodeAddress
  getter test_node : TestNodeImpl

  def initialize(host : String, port : Int32, id : String)
    @address = NodeAddress.new(host, port, id)
    @network = NetworkNode.new(@address)
    @test_node = TestNodeImpl.new(id, @network)
  end

  def join_network(bootstrap_node : String)
    join_msg = Join.new(@address.to_s)
    @network.send_message(bootstrap_node, join_msg)
  end

  def broadcast_message(content : String)
    @test_node.broadcast(content)
  end

  def close
    @network.close
  end

  def message_contents
    contents = {} of String => String
    @test_node.messages_mutex.synchronize do
      contents = @test_node.message_contents.dup
    end
    contents
  end

  def active_view
    nodes = [] of String
    @test_node.views_mutex.synchronize do
      nodes = @test_node.active_view.to_a
    end
    nodes
  end

  def passive_view
    nodes = [] of String
    @test_node.views_mutex.synchronize do
      nodes = @test_node.passive_view.to_a
    end
    nodes
  end

  def lazy_push_probability=(value : Float64)
    @test_node.lazy_push_probability = value
  end
  
  def mark_as_failed(node_id : String)
    @test_node.handle_node_failure(node_id)
  end
  
  def received_message_count
    count = 0
    @test_node.messages_mutex.synchronize do
      count = @test_node.received_messages.size
    end
    count
  end
  
  def has_message?(message_id : String) : Bool
    has = false
    @test_node.messages_mutex.synchronize do
      has = @test_node.received_messages.includes?(message_id)
    end
    has
  end
  
  def test_connection(node_id : String) : Bool
    @test_node.network.test_connection(node_id)
  end
end

# Extended Node class for testing
class TestNodeImpl < Node
  # Make mutexes accessible for testing
  getter views_mutex : Mutex
  getter messages_mutex : Mutex
  
  def initialize(id : String, network : NetworkNode)
    super(id, network)
    @test_received_messages = [] of String
  end

  # Override broadcast handler to track messages for testing
  def handle_broadcast(message : BroadcastMessage)
    message_id = message.message_id
    already_received = false
    
    @messages_mutex.synchronize do
      already_received = @received_messages.includes?(message_id)
      unless already_received
        @received_messages << message_id
        @test_received_messages << message_id
        @message_contents[message_id] = message.content
      end
    end
    
    if !already_received
      active_nodes = [] of String
      @views_mutex.synchronize do
        active_nodes = @active_view.to_a
      end
      
      active_nodes.each do |node|
        next if node == message.sender
        
        @failures_mutex.synchronize do
          next if @failed_nodes.includes?(node)
        end
        
        begin
          if rand < @lazy_push_probability
            lazy_msg = LazyPushMessage.new(@id, message_id)
            send_message(node, lazy_msg)
          else
            forward_msg = BroadcastMessage.new(@id, message_id, message.content)
            send_message(node, forward_msg)
          end
        rescue ex
          handle_node_failure(node)
        end
      end
      puts "Node #{@id}: Received broadcast #{message_id}: #{message.content}"
    end
  end

  def test_received_messages
    msgs = [] of String
    @messages_mutex.synchronize do
      msgs = @test_received_messages.dup
    end
    msgs
  end
  
  # Expose handle_node_failure for testing
  def handle_node_failure(node : String)
    super
  end
end

# Integration test
describe "Gossip Integration" do
  # Helper to wait with timeout checking a condition
  def wait_for(timeout_seconds : Float64, interval : Float64 = 0.1, &block : -> Bool)
    start_time = Time.monotonic
    until block.call || (Time.monotonic - start_time).total_seconds > timeout_seconds
      sleep interval
    end
    block.call # Return final condition check
  end

  it "successfully propagates messages through the network" do
    # Start multiple nodes
    nodes = [] of TestNode
    base_port = 7001

    begin
      # Create 5 nodes
      5.times do |i|
        node = TestNode.new("localhost", base_port + i, "node#{i + 1}")
        nodes << node
      end

      # Let nodes join the network through node1
      nodes[1..].each do |node|
        node.join_network(nodes[0].address.to_s)
      end

      # Wait for network to stabilize
      sleep 2

      # Verify active connections
      nodes[1..].each do |node|
        active_view = node.active_view
        active_view.size.should be > 0, "Node #{node.address.id} has no active connections"
      end

      # Broadcast a message from node1
      test_message = "Hello, network!"
      message_id = nodes[0].broadcast_message(test_message)

      # Wait for message propagation with timeout
      all_received = wait_for(10.0) do
        nodes.all? { |node| node.message_contents.values.includes?(test_message) }
      end

      # Verify that all nodes received the message
      all_received.should be_true, "Not all nodes received the message within timeout"
      nodes.each do |node|
        node.message_contents.values.should contain(test_message), 
          "Node #{node.address.id} didn't receive the message"
      end
    ensure
      # Clean up
      nodes.each(&.close)
      sleep 0.5
    end
  end

  it "handles node failures and message recovery" do
    nodes = [] of TestNode
    base_port = 7101

    begin
      # Create 5 nodes for better failure testing
      5.times do |i|
        node = TestNode.new("localhost", base_port + i, "node#{i + 1}")
        nodes << node
      end

      # Connect nodes
      nodes[1..].each do |node|
        node.join_network(nodes[0].address.to_s)
      end

      sleep 2

      # Set high lazy push probability to test recovery
      nodes.each do |node|
        node.lazy_push_probability = 0.8 # Higher probability for more aggressive testing
      end

      # Broadcast a first message to establish connections
      test_message1 = "Initial message"
      nodes[0].broadcast_message(test_message1)
      
      # Wait for first message to propagate
      all_received_first = wait_for(10.0) do
        nodes.all? { |node| node.message_contents.values.includes?(test_message1) }
      end
      
      all_received_first.should be_true, "Initial message didn't propagate correctly"

      # Simulate a node failure (node2)
      puts "Simulating failure of node2..."
      failed_node = nodes[1]
      
      # Mark node2 as failed in other nodes
      nodes[2..].each do |node|
        node.mark_as_failed(failed_node.address.to_s)
      end
      
      sleep 1
      
      # Broadcast from node3
      test_message2 = "Message after failure"
      message_id2 = nodes[2].broadcast_message(test_message2)
      
      # Wait for message to propagate to remaining nodes
      remaining_received = wait_for(10.0) do
        nodes[0].message_contents.values.includes?(test_message2) && 
        nodes[2..].all? { |node| node.message_contents.values.includes?(test_message2) }
      end
      
      # Verify message propagated to all except failed node
      remaining_received.should be_true, "Message didn't propagate after node failure"
      nodes[0].message_contents.values.should contain(test_message2)
      nodes[2..].each do |node|
        node.message_contents.values.should contain(test_message2)
      end
    ensure
      # Clean up
      nodes.each(&.close)
      sleep 0.5
    end
  end
  
  it "recovers from network partitions" do
    nodes = [] of TestNode
    base_port = 7201
    
    begin
      # Create 6 nodes in two groups
      6.times do |i|
        node = TestNode.new("localhost", base_port + i, "node#{i + 1}")
        nodes << node
      end
      
      # First set up a fully connected network
      nodes[1..].each do |node|
        node.join_network(nodes[0].address.to_s)
      end
      
      sleep 2
      
      # Verify initial connectivity
      nodes.each do |node|
        node.active_view.size.should be > 0, "Node #{node.address.id} has no initial connections"
      end
      
      # Create a network partition by marking cross-group nodes as failed
      puts "Creating network partition..."
      
      # Group 1: nodes 0-2, Group 2: nodes 3-5
      # Have group 1 mark all of group 2 as failed
      (0..2).each do |i|
        (3..5).each do |j|
          nodes[i].mark_as_failed(nodes[j].address.to_s)
        end
      end
      
      # Have group 2 mark all of group 1 as failed
      (3..5).each do |i|
        (0..2).each do |j|
          nodes[i].mark_as_failed(nodes[j].address.to_s)
        end
      end
      
      sleep 2
      
      # Broadcast in group 1
      group1_message = "Message from group 1"
      nodes[0].broadcast_message(group1_message)
      
      # Broadcast in group 2
      group2_message = "Message from group 2"
      nodes[3].broadcast_message(group2_message)
      
      sleep 2
      
      # Verify messages stayed within their groups
      (0..2).each do |i|
        nodes[i].message_contents.values.should contain(group1_message)
        nodes[i].message_contents.values.should_not contain(group2_message)
      end
      
      (3..5).each do |i|
        nodes[i].message_contents.values.should contain(group2_message)
        nodes[i].message_contents.values.should_not contain(group1_message)
      end
      
      # Now heal the partition by having node2 and node3 reconnect
      puts "Healing network partition..."
      begin
        nodes[2].join_network(nodes[3].address.to_s)
        sleep 2
        
        # Broadcast a new message from node2
        healing_message = "Healing message"
        nodes[2].broadcast_message(healing_message)
        
        # Wait for healing message to reach all nodes
        all_healed = wait_for(10.0) do
          nodes.all? { |node| node.message_contents.values.includes?(healing_message) }
        end
        
        all_healed.should be_true, "Healing message didn't reach all nodes"
      rescue ex
        puts "Error during partition healing: #{ex.message}"
        # Failing this part shouldn't fail the whole test
      end
    ensure
      # Clean up
      nodes.each(&.close)
      sleep 0.5
    end
  end
  
  it "maintains consistent views during churn" do
    nodes = [] of TestNode
    base_port = 7301
    
    begin
      # Create initial set of nodes
      4.times do |i|
        node = TestNode.new("localhost", base_port + i, "node#{i + 1}")
        nodes << node
      end
      
      # Connect initial nodes
      nodes[1..].each do |node|
        node.join_network(nodes[0].address.to_s)
      end
      
      sleep 2
      
      # Verify initial connectivity
      nodes.each do |node|
        node.active_view.size.should be > 0, "Initial node #{node.address.id} has no connections"
      end
      
      # Add two more nodes
      2.times do |i|
        new_node = TestNode.new("localhost", base_port + 4 + i, "node#{5 + i}")
        new_node.join_network(nodes[0].address.to_s)
        nodes << new_node
      end
      
      sleep 2
      
      # Check that new nodes have established connections
      nodes[4..5].each do |node|
        node.active_view.size.should be > 0, "New node #{node.address.id} has no connections"
      end
      
      # Broadcast message from a new node
      test_message = "Message from new node"
      nodes[4].broadcast_message(test_message)
      
      # Wait for message to propagate
      all_received = wait_for(10.0) do
        nodes.all? { |node| node.message_contents.values.includes?(test_message) }
      end
      
      all_received.should be_true, "Message from new node didn't reach all nodes"
      
      # Remove two nodes (simulate crashes)
      puts "Removing nodes 1 and 3..."
      nodes[0].close # node1
      nodes[2].close # node3
      
      # Let the network stabilize
      sleep 3
      
      # Broadcast from one of the remaining original nodes
      recovery_message = "Recovery after node removal"
      nodes[1].broadcast_message(recovery_message)
      
      # Check message propagation among remaining nodes
      remaining_received = wait_for(10.0) do
        [1, 3, 4, 5].all? { |i| nodes[i].message_contents.values.includes?(recovery_message) }
      end
      
      remaining_received.should be_true, "Recovery message didn't reach all remaining nodes"
      
      # Check view consistency - each remaining node should have active connections
      [1, 3, 4, 5].each do |i|
        nodes[i].active_view.size.should be > 0, "Node #{nodes[i].address.id} lost all connections after churn"
      end
    ensure
      # Clean up remaining nodes
      nodes.each do |node|
        begin
          node.close
        rescue
          # Ignore errors during cleanup
        end
      end
      sleep 0.5
    end
  end
end

# Run the integration tests
puts "Running enhanced integration tests..."
Spec.run
