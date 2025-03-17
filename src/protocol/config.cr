module Gossip
  module Protocol
    # Protocol configuration constants
    module Config
      # View sizes
      MAX_ACTIVE            =  5 # Max size of active view
      MAX_PASSIVE           = 10 # Max size of passive view
      MIN_ACTIVE            =  2 # Min nodes to maintain in active view
      MIN_PASSIVE           =  3 # Min nodes to send for new node's passive view
      
      # Protocol parameters
      TTL                   =  2 # Time-to-live for ForwardJoin
      LAZY_PUSH_PROB        = 0.05 # Probability for lazy push to improve propagation speed
      
      # Timing settings
      SHUFFLE_INTERVAL      = 5.0 # Seconds between shuffles
      SHUFFLE_SIZE          =  3 # Number of nodes to exchange in shuffle
      HEARTBEAT_INTERVAL    = 2.0 # Seconds between heartbeats
      REQUEST_RETRY_INTERVAL = 1.0 # Seconds between retries for message requests
      
      # Recovery parameters
      MAX_REQUEST_ATTEMPTS  = 3 # Maximum number of times to request a missing message
    end
  end
end
