require "./base"

module Gossip
  module Messages
    module Membership
      # Message for a new node joining the network
      struct Join < Base::Message
      end

      # Message to propagate join information
      struct ForwardJoin < Base::Message
        property new_node : String
        property ttl : Int32

        def initialize(@sender, @new_node, @ttl)
          super(@sender)
        end
      end

      # Message for view maintenance via shuffling
      struct Shuffle < Base::Message
        property nodes : Array(String)

        def initialize(@sender, nodes : Array(String))
          super(@sender)
          @nodes = nodes
        end
      end

      # Response to a shuffle message
      struct ShuffleReply < Base::Message
        property nodes : Array(String)

        def initialize(@sender, nodes : Array(String))
          super(@sender)
          @nodes = nodes
        end
      end

      # Message to initialize a new node's views
      struct InitViews < Base::Message
        property active_nodes : Array(String)
        property passive_nodes : Array(String)

        def initialize(@sender, @active_nodes, @passive_nodes)
          super(@sender)
        end
      end
    end
  end
end
