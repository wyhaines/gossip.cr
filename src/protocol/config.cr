module Gossip
  module Protocol
    # Protocol configuration constants
    module Config
      # View sizes
      MAX_ACTIVE  =  8 # Increased from 5 to allow more direct connections
      MAX_PASSIVE = 15 # Increased from 10 for better network resilience
      MIN_ACTIVE  =  3 # Increased from 2 to promote more connectivity
      MIN_PASSIVE =  5 # Increased from 3 for better passive view coverage

      # Protocol parameters - tuned for faster propagation
      TTL            =    3  # Increased from 2 for better reach in larger networks
      LAZY_PUSH_PROB = 0.0   # Set to 0 for fully eager push (no lazy push)

      # Timing settings - adjusted for better responsiveness
      SHUFFLE_INTERVAL       = 10.0 # Increased from 5.0 to reduce overhead
      SHUFFLE_SIZE           =   4  # Increased from 3 to exchange more nodes
      HEARTBEAT_INTERVAL     = 3.0  # Increased from 2.0 to reduce overhead
      REQUEST_RETRY_INTERVAL = 0.5  # Decreased from 1.0 for faster recovery

      # Recovery parameters
      MAX_REQUEST_ATTEMPTS = 5 # Increased from 3 for more robust recovery
    end
  end
end
