# gossip.cr

An efficient, fault-tolerant gossip protocol implementation for Crystal.

## Project Structure

The codebase has been refactored into a modular structure:

```
src/
├── gossip.cr               # Main entry point that requires all components
├── messages/
│   ├── base.cr             # Base Message struct and common functionality
│   ├── broadcast.cr        # Broadcast-related message types
│   ├── membership.cr       # Membership-related message types (Join, etc.)
│   └── heartbeat.cr        # Heartbeat-related message types
├── network/
│   ├── address.cr          # NodeAddress struct
│   └── node.cr             # NetworkNode class for TCP communication
├── protocol/
│   ├── config.cr           # Protocol configuration constants
│   ├── node.cr             # Node class with core protocol logic
│   └── handlers.cr         # Message handler implementations
└── debug.cr                # Debug logging
```

## Key Components

### Messages

- **Base Message**: Abstract base struct for all messages
- **Membership Messages**: Join, ForwardJoin, Shuffle, etc.
- **Broadcast Messages**: BroadcastMessage, LazyPushMessage, etc.
- **Heartbeat Messages**: Heartbeat and HeartbeatAck

### Network

- **NodeAddress**: Represents a network location (host, port, ID)
- **NetworkNode**: Handles TCP communication between nodes

### Protocol

- **Config**: Protocol configuration constants
- **Node**: Core gossip protocol implementation
- **Handlers**: Message handling logic

## Usage

The main entry point (`gossip.cr`) re-exports all types for backward compatibility, so existing code should continue to work without modification.

```crystal
require "./gossip"

# Create nodes
address = NodeAddress.new("localhost", 7001, "node1@localhost:7001")
network = NetworkNode.new(address)
node = Node.new("node1@localhost:7001", network)

# Join network, send messages, etc.
```

## Namespace Design

The code uses namespaces to organize functionality while preserving backward compatibility:

```crystal
module Gossip
  module Messages
    module Base
      # Base message definitions
    end
    
    module Membership
      # Membership message types
    end
    
    # ...
  end
  
  module Network
    # Network-related code
  end
  
  module Protocol
    # Protocol implementation
  end
end

# Re-export for backward compatibility
include Gossip
```
