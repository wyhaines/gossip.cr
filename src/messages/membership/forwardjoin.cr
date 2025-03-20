module Gossip
  module Messages
    module Membership
      # Message to propagate join information
      struct ForwardJoin < Base::Message
        property new_node : String
        property ttl : Int32

        def initialize(@sender, @new_node, @ttl)
          super(@sender)
        end
      end
    end
  end
end
