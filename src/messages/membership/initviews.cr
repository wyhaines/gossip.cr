module Gossip
  module Messages
    module Membership
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
