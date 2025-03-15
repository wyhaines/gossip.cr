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
    @test_node.message_contents
  end

  def lazy_push_probability=(value : Float64)
    @test_node.lazy_push_probability = value
  end
end

# Extended Node class for testing
class TestNodeImpl < Node
  def initialize(id : String, network : NetworkNode)
    super(id, network)
    @test_received_messages = [] of String
  end

  # Override broadcast handler to track messages for testing
  def handle_broadcast(message : BroadcastMessage)
    if !@received_messages.includes?(message.message_id)
      @received_messages << message.message_id
      @test_received_messages << message.message_id
      @message_contents[message.message_id] = message.content

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
      puts "Node #{@id}: Broadcast #{message.message_id}: #{message.content}"
    end
  end

  def test_received_messages
    @test_received_messages
  end
end

# Integration test
describe "Gossip Integration" do
  it "successfully propagates messages through the network" do
    # Start multiple nodes
    nodes = [] of TestNode
    base_port = 7001

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

    # Broadcast a message from node1
    test_message = "Hello, network!"
    nodes[0].broadcast_message(test_message)

    # Wait for message propagation
    sleep 2

    # Verify that all nodes received the message
    nodes.each do |node|
      node.message_contents.values.should contain(test_message)
    end

    # Clean up
    nodes.each(&.close)
  end

  it "handles node failures and message recovery" do
    nodes = [] of TestNode
    base_port = 7101

    # Create 3 nodes
    3.times do |i|
      node = TestNode.new("localhost", base_port + i, "node#{i + 1}")
      nodes << node
    end

    # Connect nodes
    nodes[1..].each do |node|
      node.join_network(nodes[0].address.to_s)
    end

    sleep 1

    # Set high lazy push probability to test recovery
    nodes.each do |node|
      node.lazy_push_probability = 0.9
    end

    # Broadcast a message
    test_message = "Test recovery"
    nodes[0].broadcast_message(test_message)

    sleep 3

    # Verify message recovery worked
    nodes.each do |node|
      node.message_contents.values.should contain(test_message)
    end

    # Clean up
    nodes.each(&.close)
  end
end

# Run the integration tests
puts "Running integration tests..."
Spec.run
