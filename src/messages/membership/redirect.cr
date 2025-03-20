module Gossip
  module Messages
    module Membership
      # Message to suggest connection redistribution
      struct Redirect < Base::Message
        property target_nodes : Array(String)

        def initialize(@sender, @target_nodes : Array(String))
          super(@sender)
        end
      end
    end
  end
end

