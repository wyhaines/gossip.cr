module Gossip
  module Messages
    module Membership
      # Response to a shuffle message
      struct ShuffleReply < Base::Message
        property nodes : Array(String)

        def initialize(@sender, nodes : Array(String))
          super(@sender)
          @nodes = nodes
        end
      end
    end
  end
end
