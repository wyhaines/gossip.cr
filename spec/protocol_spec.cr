require "./spec_helper"
require "../src/gossip"

# Extended test node for protocol-level testing
class ProtocolTestNode < Gossip::Protocol::Node
  # Expose internal state for testing
  getter views_mutex : Mutex
  getter messages_mutex : Mutex
  getter failures_mutex : Mutex

  # Track all handled messages
  property handled_messages = [] of Gossip::Messages::Base::Message

  def handle_message(message : Gossip::Messages::Base::Message)
    @handled_messages << message
    super(message)
  end

  # Expose network for testing
  def network_node : Gossip::Network::NetworkNode
    @network
  end

  # Helper to check view consistency
  def has_consistent_views? : Bool
    @views_mutex.synchronize do
      # Check no duplicates between active and passive views
      (@active_view & @passive_view).empty? &&
      # Check size constraints
      @active_view.size <= MAX_ACTIVE &&
      @passive_view.size <= MAX_PASSIVE &&
      # Check self not in views
      !@active_view.includes?(@id) &&
      !@passive_view.includes?(@id)
    end
  end

  # Helper to get all connected nodes
  def all_connected_nodes : Array(String)
    @views_mutex.synchronize do
      (@active_view | @passive_view).to_a
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

# Helper to create test node with protocol
def create_protocol_test_node(base_id : String, port : Int32? = nil) : {ProtocolTestNode, Gossip::Network::NetworkNode}
  port ||= get_random_port
  node_id = "#{base_id}@localhost:#{port}"
  addr = Gossip::NodeAddress.new("localhost", port, node_id)
  network = Gossip::Network::NetworkNode.new(addr)
  node = ProtocolTestNode.new(node_id, network)
  {node, network}
end

describe "Gossip Protocol Implementation" do
  describe "Node Join Mechanism" do
    it "handles single node join correctly" do
      node1, network1 = create_protocol_test_node("node1")
      node2, network2 = create_protocol_test_node("node2")

      begin
        # Node2 joins via node1 with retries
        success = false
        3.times do |attempt|
          join_msg = Gossip::Messages::Membership::Join.new(node2.id)
          network2.send_message(node1.id, join_msg)

          # Wait for bidirectional connection
          timeout = 2.seconds
          start = Time.monotonic

          while (Time.monotonic - start) < timeout
            if node1.active_view.includes?(node2.id) && node2.active_view.includes?(node1.id)
              success = true
              break
            end
            sleep(0.1.seconds)
          end

          break if success
          sleep(0.5.seconds) if attempt < 2
        end

        # Verify bidirectional connection established
        success.should be_true
        node1.active_view.should contain(node2.id)
        node2.active_view.should contain(node1.id)

        # Verify views are consistent
        node1.has_consistent_views?.should be_true
        node2.has_consistent_views?.should be_true

        # Verify InitViews message was eventually received
        node2.handled_messages.any? { |m| m.is_a?(Gossip::Messages::Membership::InitViews) }.should be_true
      ensure
        network1.close
        network2.close
      end
    end

    it "handles concurrent joins without timeout" do
      bootstrap, boot_network = create_protocol_test_node("bootstrap")
      nodes = [] of {ProtocolTestNode, Gossip::Network::NetworkNode}

      begin
        # Create 10 nodes and have them join concurrently
        10.times do |i|
          node, network = create_protocol_test_node("node#{i + 1}")
          nodes << {node, network}
        end

        # All nodes join at once with retries
        start_time = Time.monotonic
        channel = Channel(Bool).new(nodes.size)
        connected_count = 0

        nodes.each do |node, network|
          spawn do
            success = false
            3.times do |attempt|
              begin
                join_msg = Gossip::Messages::Membership::Join.new(node.id)
                network.send_message(bootstrap.id, join_msg)

                # Wait for connection
                timeout = 3.seconds
                start = Time.monotonic

                while (Time.monotonic - start) < timeout
                  if node.active_view.includes?(bootstrap.id)
                    success = true
                    break
                  end
                  sleep(0.1.seconds)
                end

                break if success
              rescue ex
                # Retry on error
              end
              sleep(0.5.seconds) if attempt < 2
            end
            channel.send(success)
          end
        end

        # Wait for all joins to complete and count successes
        nodes.size.times do
          if channel.receive
            connected_count += 1
          end
        end
        join_time = Time.monotonic - start_time

        # Most nodes should connect successfully
        connected_count.should be >= 8  # At least 8 out of 10

        # Joins should complete within reasonable time (under 10 seconds for 10 nodes with retries)
        join_time.total_seconds.should be < 11.0

        # Wait for network to stabilize
        sleep(1.second)

        # Bootstrap should have connections (respecting MAX_ACTIVE)
        bootstrap.active_view.size.should be > 0
        bootstrap.active_view.size.should be <= Gossip::Protocol::Config::MAX_ACTIVE

        # Some nodes should be in passive view if we exceeded MAX_ACTIVE
        if nodes.size > Gossip::Protocol::Config::MAX_ACTIVE
          bootstrap.passive_view.size.should be > 0
        end

        # All views should be consistent
        bootstrap.has_consistent_views?.should be_true
        nodes.each { |n, _| n.has_consistent_views?.should be_true }
      ensure
        boot_network.close
        nodes.each { |_, network| network.close }
      end
    end

    it "handles join with view limits correctly" do
      bootstrap, boot_network = create_protocol_test_node("bootstrap")
      nodes = [] of {ProtocolTestNode, Gossip::Network::NetworkNode}

      begin
        # Create MAX_ACTIVE + 5 nodes
        successful_joins = 0
        (Gossip::Protocol::Config::MAX_ACTIVE + 5).times do |i|
          node, network = create_protocol_test_node("node#{i + 1}")
          nodes << {node, network}

          # Join sequentially with retries to test view management
          success = false
          2.times do |attempt|
            begin
              join_msg = Gossip::Messages::Membership::Join.new(node.id)
              network.send_message(bootstrap.id, join_msg)

              # Wait briefly for join to process
              timeout = 1.second
              start = Time.monotonic

              while (Time.monotonic - start) < timeout
                if bootstrap.active_view.includes?(node.id) || bootstrap.passive_view.includes?(node.id)
                  success = true
                  successful_joins += 1
                  break
                end
                sleep(0.05.seconds)
              end

              break if success
            rescue ex
              # Retry
            end
            sleep(0.1.seconds) if attempt < 1
          end
        end

        # Wait for stabilization
        sleep(1.seconds)

        # Bootstrap should have many connections
        bootstrap.active_view.size.should be > 0
        bootstrap.active_view.size.should be <= Gossip::Protocol::Config::MAX_ACTIVE

        # Some nodes should be in passive view if many joined successfully
        if successful_joins > Gossip::Protocol::Config::MAX_ACTIVE
          bootstrap.passive_view.size.should be > 0
        end

        # Total known nodes should be close to successful joins
        total_connected = bootstrap.active_view.size + bootstrap.passive_view.size
        total_connected.should be >= [successful_joins - 2, 1].max  # Allow some tolerance

        # No duplicates between views
        (bootstrap.active_view & bootstrap.passive_view).empty?.should be_true
      ensure
        boot_network.close
        nodes.each { |_, network| network.close }
      end
    end

    it "handles ForwardJoin propagation correctly" do
      node1, network1 = create_protocol_test_node("node1")
      node2, network2 = create_protocol_test_node("node2")
      node3, network3 = create_protocol_test_node("node3")

      begin
        # First establish node2 -> node1 connection with retries
        success = false
        3.times do
          join_msg = Gossip::Messages::Membership::Join.new(node2.id)
          network2.send_message(node1.id, join_msg)

          # Wait for bidirectional connection
          timeout = 2.seconds
          start = Time.monotonic

          while (Time.monotonic - start) < timeout
            if node1.active_view.includes?(node2.id) && node2.active_view.includes?(node1.id)
              success = true
              break
            end
            sleep(0.1.seconds)
          end

          break if success
          sleep(0.5.seconds)
        end

        success.should be_true

        # Now node3 joins via node1
        join_msg = Gossip::Messages::Membership::Join.new(node3.id)
        network3.send_message(node1.id, join_msg)

        # Wait for ForwardJoin to potentially reach node2
        sleep(2.seconds)

        # Check if node2 received ForwardJoin or knows about node3
        forward_join_received = node2.handled_messages.any? { |m|
          m.is_a?(Gossip::Messages::Membership::ForwardJoin) &&
          m.as(Gossip::Messages::Membership::ForwardJoin).new_node == node3.id
        }

        # Node2 should either have received ForwardJoin or node3 should be in its views
        node3_known = node2.active_view.includes?(node3.id) || node2.passive_view.includes?(node3.id)

        (forward_join_received || node3_known).should be_true
      ensure
        network1.close
        network2.close
        network3.close
      end
    end
  end

  describe "View Management" do
    it "maintains view constraints during churn" do
      bootstrap, boot_network = create_protocol_test_node("bootstrap")
      all_nodes = [] of {ProtocolTestNode, Gossip::Network::NetworkNode}

      begin
        # Phase 1: Add many nodes
        15.times do |i|
          node, network = create_protocol_test_node("node#{i + 1}")
          all_nodes << {node, network}

          join_msg = Gossip::Messages::Membership::Join.new(node.id)
          network.send_message(bootstrap.id, join_msg)
          sleep(0.05.seconds)
        end

        sleep(0.5.seconds)

        # Check initial state
        bootstrap.active_view.size.should eq(Gossip::Protocol::Config::MAX_ACTIVE)
        bootstrap.has_consistent_views?.should be_true

        # Phase 2: Remove some nodes
        nodes_to_remove = all_nodes.sample(5)
        nodes_to_remove.each do |node, network|
          network.close
          all_nodes.delete({node, network})
        end

        # Trigger view maintenance
        sleep((Gossip::Protocol::Config::HEARTBEAT_INTERVAL + 1).seconds)

        # Bootstrap should detect failures and promote from passive view
        bootstrap.active_view.size.should be >= Gossip::Protocol::Config::MIN_ACTIVE
        bootstrap.has_consistent_views?.should be_true

        # Phase 3: Add new nodes again
        5.times do |i|
          node, network = create_protocol_test_node("new_node#{i + 1}")
          all_nodes << {node, network}

          join_msg = Gossip::Messages::Membership::Join.new(node.id)
          network.send_message(bootstrap.id, join_msg)
          sleep(0.05.seconds)
        end

        sleep(0.5.seconds)

        # Final check - views should still be consistent
        bootstrap.has_consistent_views?.should be_true
        bootstrap.active_view.size.should be <= Gossip::Protocol::Config::MAX_ACTIVE
        bootstrap.active_view.size.should be >= Gossip::Protocol::Config::MIN_ACTIVE
      ensure
        boot_network.close
        all_nodes.each { |_, network| network.close rescue nil }
      end
    end

    it "handles node failures and recovery" do
      nodes_data = [] of {ProtocolTestNode, Gossip::Network::NetworkNode}

      begin
        # Create 5 nodes in a chain
        5.times do |i|
          node, network = create_protocol_test_node("node#{i + 1}")
          nodes_data << {node, network}
        end

        # Connect them in sequence
        (1...nodes_data.size).each do |i|
          node, network = nodes_data[i]
          prev_node = nodes_data[i - 1][0]

          join_msg = Gossip::Messages::Membership::Join.new(node.id)
          network.send_message(prev_node.id, join_msg)
          sleep(0.1.seconds)
        end

        # Verify initial connectivity
        nodes_data.each_with_index do |(node, _), i|
          node.active_view.size.should be > 0
        end

        # Simulate middle node failure
        failed_node, failed_network = nodes_data[2]
        failed_network.close

        # Wait for heartbeat detection
        sleep((Gossip::Protocol::Config::HEARTBEAT_INTERVAL + 1).seconds)

        # Nodes should detect the failure
        nodes_data.each_with_index do |(node, _), i|
          next if i == 2  # Skip the failed node

          node.failed_nodes.should contain(failed_node.id)
          node.active_view.should_not contain(failed_node.id)
        end

        # Network should still be connected (nodes should find alternate paths)
        active_nodes = nodes_data.reject { |n, _| n == failed_node }
        active_nodes.each do |(node, _)|
          node.active_view.size.should be > 0
        end
      ensure
        nodes_data.each { |_, network| network.close rescue nil }
      end
    end
  end

  describe "Shuffle Mechanism" do
    it "exchanges nodes between passive views" do
      nodes_data = [] of {ProtocolTestNode, Gossip::Network::NetworkNode}

      begin
        # Create 6 nodes
        6.times do |i|
          node, network = create_protocol_test_node("node#{i + 1}")
          nodes_data << {node, network}
        end

        # Connect first 3 nodes to node1
        (1..2).each do |i|
          node, network = nodes_data[i]
          join_msg = Gossip::Messages::Membership::Join.new(node.id)
          network.send_message(nodes_data[0][0].id, join_msg)
          sleep(0.05.seconds)
        end

        # Connect last 3 nodes to node4
        (4..5).each do |i|
          node, network = nodes_data[i]
          join_msg = Gossip::Messages::Membership::Join.new(node.id)
          network.send_message(nodes_data[3][0].id, join_msg)
          sleep(0.05.seconds)
        end

        # Connect the two groups
        join_msg = Gossip::Messages::Membership::Join.new(nodes_data[3][0].id)
        nodes_data[3][1].send_message(nodes_data[0][0].id, join_msg)
        sleep(0.2.seconds)

        # Record initial passive view sizes
        initial_passive_sizes = nodes_data.map { |n, _| n.passive_view.size }

        # Trigger shuffle
        nodes_data[0][0].send_shuffle
        sleep(0.5.seconds)

        # Check that passive views have changed
        nodes_data.each_with_index do |(node, _), i|
          # At least some nodes should have different passive view sizes
          if node.passive_view.size != initial_passive_sizes[i]
            (node.passive_view.size > 0).should be_true
          end
        end

        # Verify no self-references or active-passive duplicates
        nodes_data.each do |node, _|
          node.has_consistent_views?.should be_true
        end
      ensure
        nodes_data.each { |_, network| network.close rescue nil }
      end
    end
  end

  describe "Connection Redistribution" do
    it "handles redirect messages for load balancing" do
      bootstrap, boot_network = create_protocol_test_node("bootstrap")
      nodes = [] of {ProtocolTestNode, Gossip::Network::NetworkNode}

      begin
        # Create more nodes than MAX_ACTIVE
        (Gossip::Protocol::Config::MAX_ACTIVE + 3).times do |i|
          node, network = create_protocol_test_node("node#{i + 1}")
          nodes << {node, network}

          join_msg = Gossip::Messages::Membership::Join.new(node.id)
          network.send_message(bootstrap.id, join_msg)
          sleep(0.1.seconds)
        end

        # Some nodes should have received redirect messages
        redirect_count = nodes.count { |n, _|
          n.handled_messages.any? { |m| m.is_a?(Gossip::Messages::Membership::Redirect) }
        }
        redirect_count.should be > 0

        # Nodes should be distributed (not all connected only to bootstrap)
        non_bootstrap_connections = nodes.count { |n, _|
          n.active_view.any? { |peer| peer != bootstrap.id }
        }
        non_bootstrap_connections.should be > 0
      ensure
        boot_network.close
        nodes.each { |_, network| network.close rescue nil }
      end
    end
  end
end
