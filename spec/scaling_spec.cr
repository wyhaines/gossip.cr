require "./spec_helper"
require "../src/gossip"
require "wait_group"

# Test node for scaling tests with performance metrics
class ScalingTestNode < Gossip::Protocol::Node
  property join_start_time : Time?
  property join_complete_time : Time?
  property messages_sent = 0
  property messages_received = 0
  property bytes_sent = 0
  property bytes_received = 0
  
  getter views_mutex : Mutex
  getter messages_mutex : Mutex
  
  def send_message(to : String, message : Gossip::Messages::Base::Message)
    @messages_sent += 1
    @bytes_sent += message.to_json.bytesize
    super(to, message)
  end
  
  def handle_message(message : Gossip::Messages::Base::Message)
    @messages_received += 1
    @bytes_received += message.to_json.bytesize
    super(message)
  end
  
  def join_duration : Float64?
    if start = @join_start_time
      if complete = @join_complete_time
        (complete - start).total_seconds
      end
    end
  end
  
  def message_count : Int32
    @messages_mutex.synchronize do
      @received_messages.size
    end
  end
  
  def connection_count : Int32
    @views_mutex.synchronize do
      @active_view.size
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

def create_scaling_test_node(base_id : String, port : Int32? = nil) : {ScalingTestNode, Gossip::Network::NetworkNode}
  port ||= get_random_port
  node_id = "#{base_id}@localhost:#{port}"
  addr = Gossip::NodeAddress.new("localhost", port, node_id)
  network = Gossip::Network::NetworkNode.new(addr)
  node = ScalingTestNode.new(node_id, network)
  {node, network}
end

describe "Gossip Protocol Scaling" do
  describe "Large Network Join" do
    it "handles 10 nodes joining without timeout" do
      bootstrap, boot_network = create_scaling_test_node("bootstrap")
      nodes_data = [] of {ScalingTestNode, Gossip::Network::NetworkNode}
      
      begin
        # Create 10 nodes
        10.times do |i|
          node, network = create_scaling_test_node("node#{i + 1}")
          nodes_data << {node, network}
        end
        
        # Track join times
        join_durations = [] of Float64
        successful_joins = 0
        
        # All nodes join concurrently
        wg = WaitGroup.new(nodes_data.size)
        
        nodes_data.each do |node, network|
          spawn do
            begin
              node.join_start_time = Time.utc
              join_msg = Gossip::Messages::Membership::Join.new(node.id)
              network.send_message(bootstrap.id, join_msg)
              
              # Wait for connection to be established
              start = Time.monotonic
              timeout = 5.seconds
              connected = false
              
              while (Time.monotonic - start) < timeout
                if node.connection_count > 0
                  connected = true
                  break
                end
                sleep(0.01.seconds)
              end
              
              if connected
                node.join_complete_time = Time.utc
                successful_joins += 1
                if duration = node.join_duration
                  join_durations << duration
                end
              end
            ensure
              wg.done
            end
          end
        end
        
        wg.wait
        
        # All nodes should join successfully
        successful_joins.should eq(10)
        
        # Join times should be reasonable (under 2 seconds average)
        avg_join_time = join_durations.sum / join_durations.size
        avg_join_time.should be < 2.0
        
        # Max join time should be under 5 seconds
        max_join_time = join_durations.max
        max_join_time.should be < 5.0
        
        # Bootstrap should have proper view management
        bootstrap.active_view.size.should be <= Gossip::Protocol::Config::MAX_ACTIVE
        
        # All nodes should be connected somewhere
        total_in_views = bootstrap.active_view.size + bootstrap.passive_view.size
        total_in_views.should eq(10)
      ensure
        boot_network.close
        nodes_data.each { |_, network| network.close rescue nil }
      end
    end
    
    it "handles 20 nodes with proper load distribution" do
      bootstrap, boot_network = create_scaling_test_node("bootstrap")
      nodes_data = [] of {ScalingTestNode, Gossip::Network::NetworkNode}
      
      begin
        # Create 20 nodes
        20.times do |i|
          node, network = create_scaling_test_node("node#{i + 1}")
          nodes_data << {node, network}
        end
        
        # Join in waves to avoid overwhelming bootstrap
        nodes_data.each_slice(5) do |slice|
          wg = WaitGroup.new(slice.size)
          
          slice.each do |node, network|
            spawn do
              begin
                join_msg = Gossip::Messages::Membership::Join.new(node.id)
                network.send_message(bootstrap.id, join_msg)
              ensure
                wg.done
              end
            end
          end
          
          wg.wait
          sleep(0.2.seconds)  # Brief pause between waves
        end
        
        # Wait for network to stabilize
        sleep(2.seconds)
        
        # Check load distribution
        nodes_with_multiple_connections = nodes_data.count { |n, _| n.connection_count > 1 }
        nodes_with_multiple_connections.should be > 5  # At least some nodes should have multiple connections
        
        # No node should be overloaded
        nodes_data.each do |node, _|
          node.connection_count.should be <= Gossip::Protocol::Config::MAX_ACTIVE
        end
        
        # Bootstrap should not have all connections
        bootstrap.active_view.size.should eq(Gossip::Protocol::Config::MAX_ACTIVE)
      ensure
        boot_network.close
        nodes_data.each { |_, network| network.close rescue nil }
      end
    end
  end
  
  describe "High Message Volume" do
    it "propagates 50 messages across 10 nodes" do
      nodes_data = [] of {ScalingTestNode, Gossip::Network::NetworkNode}
      message_count = 50
      node_count = 10
      
      begin
        # Create nodes
        node_count.times do |i|
          node, network = create_scaling_test_node("node#{i + 1}")
          nodes_data << {node, network}
        end
        
        # Connect nodes in a mesh-like topology
        # Each node connects to 2-3 others
        nodes_data.each_with_index do |(node, network), i|
          targets = [
            (i + 1) % node_count,
            (i + 2) % node_count,
            (i + node_count // 2) % node_count
          ].uniq.reject { |t| t == i }
          
          targets.first(2).each do |target_idx|
            join_msg = Gossip::Messages::Membership::Join.new(node.id)
            network.send_message(nodes_data[target_idx][0].id, join_msg)
            sleep(0.02.seconds)
          end
        end
        
        # Wait for network to stabilize
        sleep(1.second)
        
        # Verify connectivity
        disconnected = nodes_data.select { |n, _| n.connection_count == 0 }
        disconnected.size.should eq(0)
        
        # Send messages from different nodes
        message_ids = [] of String
        start_time = Time.utc
        
        message_count.times do |i|
          source_idx = i % node_count
          msg_id = nodes_data[source_idx][0].broadcast("Message #{i} from node #{source_idx}")
          message_ids << msg_id
          sleep(0.01.seconds)  # Small delay between messages
        end
        
        # Wait for propagation with progress tracking
        timeout = 30.seconds
        start = Time.monotonic
        last_progress = 0
        no_progress_count = 0
        
        while (Time.monotonic - start) < timeout
          # Count messages received by each node
          received_counts = nodes_data.map { |n, _| 
            message_ids.count { |id| n.message_count > 0 && n.received_messages.includes?(id) }
          }
          
          min_received = received_counts.min
          avg_received = received_counts.sum.to_f / received_counts.size
          
          # Check if we're making progress
          if min_received > last_progress
            last_progress = min_received
            no_progress_count = 0
          else
            no_progress_count += 1
          end
          
          # Log progress every second
          if (Time.monotonic - start).total_seconds.to_i % 1 == 0
            puts "Progress: min=#{min_received}/#{message_count}, avg=#{avg_received.round(1)}/#{message_count}"
          end
          
          # Success condition
          if min_received == message_count
            break
          end
          
          # If no progress for too long, we might be stuck
          if no_progress_count > 50  # 5 seconds of no progress
            puts "Warning: No progress for 5 seconds"
            break
          end
          
          sleep(0.1.seconds)
        end
        
        # Verify results
        nodes_data.each_with_index do |(node, _), idx|
          received = message_ids.count { |id| node.received_messages.includes?(id) }
          received.should eq(message_count)
        end
        
        # Check propagation time
        total_time = (Time.utc - start_time).total_seconds
        puts "Total propagation time: #{total_time} seconds"
        total_time.should be < 30.0
      ensure
        nodes_data.each { |_, network| network.close rescue nil }
      end
    end
    
    it "handles 100 messages with 5 nodes efficiently" do
      nodes_data = [] of {ScalingTestNode, Gossip::Network::NetworkNode}
      message_count = 100
      
      begin
        # Create 5 nodes
        5.times do |i|
          node, network = create_scaling_test_node("node#{i + 1}")
          nodes_data << {node, network}
        end
        
        # Full mesh topology for 5 nodes
        nodes_data.each_with_index do |(node, network), i|
          nodes_data.each_with_index do |(target_node, _), j|
            next if i >= j  # Avoid duplicate connections
            
            join_msg = Gossip::Messages::Membership::Join.new(node.id)
            network.send_message(target_node.id, join_msg)
            sleep(0.05.seconds)
          end
        end
        
        sleep(0.5.seconds)
        
        # Send messages in batches
        message_ids = [] of String
        batch_size = 10
        
        (message_count // batch_size).times do |batch|
          batch_ids = [] of String
          
          # Send batch concurrently from different nodes
          wg = WaitGroup.new(batch_size)
          batch_size.times do |i|
            spawn do
              begin
                source_idx = (batch * batch_size + i) % nodes_data.size
                msg_id = nodes_data[source_idx][0].broadcast("Batch #{batch} Message #{i}")
                batch_ids << msg_id
              ensure
                wg.done
              end
            end
          end
          
          wg.wait
          message_ids.concat(batch_ids)
          
          # Small delay between batches
          sleep(0.05.seconds)
        end
        
        # Wait for propagation
        timeout = 20.seconds
        start = Time.monotonic
        
        while (Time.monotonic - start) < timeout
          if nodes_data.all? { |n, _| 
            message_ids.all? { |id| n.received_messages.includes?(id) }
          }
            break
          end
          sleep(0.1.seconds)
        end
        
        # All nodes should receive all messages
        nodes_data.each do |node, _|
          received = message_ids.count { |id| node.received_messages.includes?(id) }
          received.should eq(message_count)
        end
        
        # Check network traffic
        total_messages_sent = nodes_data.sum { |n, _| n.messages_sent }
        total_bytes_sent = nodes_data.sum { |n, _| n.bytes_sent }
        
        puts "Network stats: #{total_messages_sent} messages, #{total_bytes_sent} bytes"
        
        # Efficiency check - shouldn't need too many retransmissions
        # Each message should be sent roughly O(n) times in a good gossip protocol
        avg_transmissions_per_message = total_messages_sent.to_f / message_count
        avg_transmissions_per_message.should be < 50  # Conservative upper bound
      ensure
        nodes_data.each { |_, network| network.close rescue nil }
      end
    end
  end
  
  describe "Network Stress Test" do
    it "maintains stability under continuous load" do
      nodes_data = [] of {ScalingTestNode, Gossip::Network::NetworkNode}
      
      begin
        # Create 8 nodes
        8.times do |i|
          node, network = create_scaling_test_node("node#{i + 1}")
          nodes_data << {node, network}
        end
        
        # Create random topology
        nodes_data.each_with_index do |(node, network), i|
          # Each node connects to 2-3 random others
          num_connections = rand(2..3)
          targets = (0...nodes_data.size).to_a.reject { |j| j == i }.sample(num_connections)
          
          targets.each do |target_idx|
            join_msg = Gossip::Messages::Membership::Join.new(node.id)
            network.send_message(nodes_data[target_idx][0].id, join_msg)
            sleep(0.03.seconds)
          end
        end
        
        sleep(1.second)
        
        # Continuous message sending for 5 seconds
        stop_sending = false
        message_ids = [] of String
        sender_fibers = [] of Fiber
        
        # Start continuous senders
        nodes_data.each_with_index do |(node, _), i|
          fiber = spawn do
            count = 0
            while !stop_sending
              msg_id = node.broadcast("Stress test #{count} from node #{i}")
              message_ids << msg_id
              count += 1
              sleep((0.1 + rand(0.1)).seconds)  # Variable rate
            end
          end
          sender_fibers << fiber
        end
        
        # Run for 5 seconds
        sleep(5.seconds)
        stop_sending = true
        
        # Wait for senders to stop
        sleep(0.5.seconds)
        
        # Let messages propagate
        sleep(3.seconds)
        
        # Check results
        puts "Total messages sent: #{message_ids.size}"
        
        # Each node should have received most messages (allow 5% loss under stress)
        min_acceptable = (message_ids.size * 0.95).to_i
        
        nodes_data.each_with_index do |(node, _), i|
          received = message_ids.count { |id| node.received_messages.includes?(id) }
          puts "Node #{i} received: #{received}/#{message_ids.size}"
          received.should be >= min_acceptable
        end
        
        # Network should still be functional
        test_msg_id = nodes_data[0][0].broadcast("Final test message")
        sleep(1.second)
        
        nodes_data.each do |node, _|
          node.received_messages.includes?(test_msg_id).should be_true
        end
      ensure
        nodes_data.each { |_, network| network.close rescue nil }
      end
    end
  end
end