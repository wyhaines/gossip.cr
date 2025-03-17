require "./spec_helper"
require "../src/gossip"

# Type aliases for clarity
alias NodeTuple = {Node, NetworkNode}

# Helper to get a random available port
def get_random_port : Int32
  server = TCPServer.new("localhost", 0)
  port = server.local_address.port
  server.close
  port
end

# Helper to create a node ID string
def create_node_id(base_id : String, port : Int32) : String
  String.build do |str|
    str << base_id
    str << "@localhost:"
    str << port
  end
end

# Helper to create a node with proper address
def create_test_node(base_id : String) : NodeTuple
  port : Int32 = get_random_port
  node_id : String = create_node_id(base_id, port)
  addr : NodeAddress = NodeAddress.new("localhost", port, node_id)
  network : NetworkNode = NetworkNode.new(addr)
  node : Node = Node.new(node_id, network)
  {node, network}
end

# Helper to wait for network operations to complete
def wait_for_network
  sleep(Time::Span.new(seconds: 0, nanoseconds: 100_000_000))
end

describe "Gossip Protocol" do
  describe "Message Serialization" do
    it "serializes and deserializes Join message" do
      port : Int32 = get_random_port
      node_id : String = create_node_id("node1", port)
      msg : Join = Join.new(node_id)
      json : String = msg.to_json
      parsed : Join = Join.from_json(json)
      parsed.sender.should eq(node_id)
      parsed.type.should eq("Join")
    end

    it "serializes and deserializes ForwardJoin message" do
      port1 : Int32 = get_random_port
      port2 : Int32 = get_random_port
      node1_id : String = create_node_id("node1", port1)
      node2_id : String = create_node_id("node2", port2)
      msg : ForwardJoin = ForwardJoin.new(node1_id, node2_id, 2)
      json : String = msg.to_json
      parsed : ForwardJoin = ForwardJoin.from_json(json)
      parsed.sender.should eq(node1_id)
      parsed.new_node.should eq(node2_id)
      parsed.ttl.should eq(2)
    end

    it "serializes and deserializes BroadcastMessage" do
      port : Int32 = get_random_port
      node_id : String = create_node_id("node1", port)
      msg : BroadcastMessage = BroadcastMessage.new(node_id, "msg123", "hello")
      json : String = msg.to_json
      parsed : BroadcastMessage = BroadcastMessage.from_json(json)
      parsed.sender.should eq(node_id)
      parsed.message_id.should eq("msg123")
      parsed.content.should eq("hello")
    end
  end

  describe "Node" do
    it "initializes with empty views" do
      result : NodeTuple = create_test_node("node1")
      node = result[0]
      network = result[1]

      node.active_view.empty?.should be_true
      node.passive_view.empty?.should be_true
      node.received_messages.empty?.should be_true

      network.close
      wait_for_network
    end

    it "handles join messages" do
      result1 : NodeTuple = create_test_node("node1")
      result2 : NodeTuple = create_test_node("node2")
      node1 = result1[0]
      node2 = result2[0]
      network1 = result1[1]
      network2 = result2[1]

      join_msg : Join = Join.new(node2.id)
      node1.handle_message(join_msg)
      wait_for_network

      node1.active_view.includes?(node2.id).should be_true

      network1.close
      network2.close
      wait_for_network
    end

    it "respects maximum active view size" do
      result1 : NodeTuple = create_test_node("node1")
      node1 = result1[0]
      network1 = result1[1]
      networks : Array(NetworkNode) = [network1]
      nodes : Array(Node) = [] of Node

      begin
        # Create MAX_ACTIVE + 1 nodes and have them join
        (2..Node::MAX_ACTIVE + 2).each do |i|
          result : NodeTuple = create_test_node("node#{i}")
          node = result[0]
          network = result[1]
          nodes << node
          networks << network

          join_msg : Join = Join.new(node.id)
          node1.handle_message(join_msg)
          wait_for_network
        end

        # Verify active view size constraint
        node1.active_view.size.should eq(Node::MAX_ACTIVE)
        
        # The passive view should not be empty - it contains nodes from:
        # 1. Active view displacement when the view becomes full
        # 2. ForwardJoin messages with TTL=0 (part of the join propagation)
        # Both are correct protocol behaviors
        node1.passive_view.empty?.should be_false
      ensure
        # Close networks in reverse order to avoid connection errors
        networks.reverse_each(&.close)
        wait_for_network
      end
    end
  end
end
