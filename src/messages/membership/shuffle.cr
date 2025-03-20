module Gossip
  module Messages
    module Membership
      # Message for view maintenance via shuffling
      struct Shuffle < Base::Message
        property nodes : Array(String)

        def initialize(@sender, nodes : Array(String))
          super(@sender)
          @nodes = nodes
        end
      end
    end
  end
end
