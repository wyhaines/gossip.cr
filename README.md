# gossip.cr

[![License: MIT](https://img.shields.io/badge/License-Apache2-blue.svg)](https://opensource.org/license/apache-2-0)
[![Crystal CI](https://github.com/wyhaines/gossip.cr/actions/workflows/crystal.yml/badge.svg)](https://github.com/wyhaines/gossip.cr/actions/workflows/crystal.yml)
[![API Documentation](https://img.shields.io/badge/API-Documentation-green.svg)](https://wyhaines.github.io/gossip.cr)

A robust, efficient, and fault-tolerant gossip protocol implementation for Crystal.

## Overview

`gossip.cr` provides a modular, reliable implementation of gossip protocols for distributed systems. It enables peer-to-peer communication between nodes in a network with the following features:

- **Decentralized**: No single point of failure or central coordination
- **Fault-tolerant**: Resilient against node failures and network partitions
- **Scalable**: Efficiently handles adding and removing nodes dynamically
- **Self-healing**: Automatically recovers from failures
- **Guaranteed Delivery**: Ensures messages reach all nodes despite failures

## Algorithms & Design

This library implements several key protocols:

### HyParView Membership Protocol

The foundation of the network is built on the HyParView protocol, which maintains two views per node:

- **Active View**: A small set of nodes with direct connections
- **Passive View**: A larger set of nodes kept as backups

This approach provides both efficiency and resilience. When nodes in the active view fail, they are replaced with nodes from the passive view, ensuring the network remains connected.

### Plumtree Broadcast Protocol

For reliable broadcast, `gossip.cr` implements the Plumtree (Push-Lazy-Push Multicast Tree) algorithm which:

1. Constructs an efficient spanning tree for eager push dissemination
2. Uses lazy push for redundancy to recover from failures
3. Adaptively optimizes the tree as the network evolves

### Message Acknowledgment & Recovery

To ensure reliability, the system:

- Tracks missing messages through lazy push notifications
- Requests full message content from peers when missing messages are detected
- Implements heartbeats to detect node failures

## Installation

Add this to your application's `shard.yml`:

```yaml
dependencies:
  gossip:
    github: wyhaines/gossip.cr
```

Then run:

```bash
shards install
```

## Basic Usage

### Creating a Node

```crystal
require "gossip"

# Create a node
address = NodeAddress.new("localhost", 7001, "node1@localhost:7001")
network = NetworkNode.new(address)
node = Node.new("node1@localhost:7001", network)

# Start accepting connections
# Network operations are handled asynchronously in fibers
```

### Joining an Existing Network

```crystal
# Join an existing network through a bootstrap node
join_msg = Join.new(my_node_id)
network.send_message("bootstrap_node@hostname:port", join_msg)
```

### Broadcasting Messages

```crystal
# Send a message to all nodes in the network
message_id = node.broadcast("Hello, network!")
```

### Extending Node Behavior

You can extend the `Node` class to customize message handling:

```crystal
class MyCustomNode < Node
  def handle_broadcast(message : BroadcastMessage)
    # Custom message handling logic
    puts "Received message: #{message.content} from #{message.sender}"
    
    # Call super to maintain the gossip protocol's default behavior
    super
  end
end
```

## Configuration

Key configuration parameters can be adjusted in `src/protocol/config.cr`:

```crystal
module Config
  # View sizes
  MAX_ACTIVE = 5       # Max connections in active view
  MAX_PASSIVE = 10     # Max nodes in passive view
  
  # Message propagation
  LAZY_PUSH_PROB = 0.05  # Probability of using lazy push
  
  # Timing settings
  SHUFFLE_INTERVAL = 5.0  # Seconds between shuffles
  HEARTBEAT_INTERVAL = 2.0  # Seconds between heartbeats
end
```

## Examples

The repository includes several examples:

- **demo.cr**: Interactive demo with terminal UI
- **network_test.cr**: Basic network connectivity testing
- **integration_test.cr**: Comprehensive integration tests

To run the demo:

```bash
# Terminal 1: Start bootstrap node
crystal run examples/demo.cr bootstrap

# Terminal 2: Join network with second node
crystal run examples/demo.cr join 7002 node2
```

## Project Structure

```
src/
├── gossip.cr               # Main entry point
├── messages/               # Message type definitions
│   ├── base.cr
│   ├── broadcast.cr
│   ├── membership.cr
│   └── heartbeat.cr
├── network/                # Network communication
│   ├── address.cr
│   └── node.cr
├── protocol/               # Protocol implementation
│   ├── config.cr
│   ├── node.cr
│   └── handlers.cr
└── debug.cr                # Debug utilities
```

## Testing

Run the test suite:

```bash
crystal spec
```

Integration tests demonstrate reliable message delivery, recovery from node failures, and handling of network partitions.

## Performance Considerations

- **Message Size**: Keep broadcast messages reasonably sized for efficient propagation
- **Network Size**: The protocol is most efficient with 5-50 nodes; for larger networks, consider hierarchical structures
- **Timing Parameters**: Adjust shuffle and heartbeat intervals based on network stability and message frequency

## Contributing

1. Fork it (<https://github.com/wyhaines/gossip.cr/fork>)
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Add some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create a new Pull Request

## License

This project is available under the Apache 2.0 License. See the LICENSE file for details.
