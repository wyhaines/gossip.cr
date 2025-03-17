require "./base"

module Gossip
  module Messages
    module Broadcast
      # Message for broadcasting content (Plumtree)
      struct BroadcastMessage < Base::Message
        property message_id : String
        property content : String

        def initialize(@sender, @message_id, @content)
          super(@sender)
        end
      end

      # Message for lazy push notification (Plumtree)
      struct LazyPushMessage < Base::Message
        property message_id : String

        def initialize(@sender, @message_id)
          super(@sender)
        end
      end

      # Message to request missing content (Plumtree)
      struct MessageRequest < Base::Message
        property message_id : String
        property from_lazy_push : Bool

        def initialize(@sender, @message_id, @from_lazy_push = true)
          super(@sender)
        end
      end

      # Message to respond with missing content (Plumtree)
      struct MessageResponse < Base::Message
        property message_id : String
        property content : String

        def initialize(@sender, @message_id, @content)
          super(@sender)
        end
      end
    end
  end
end
