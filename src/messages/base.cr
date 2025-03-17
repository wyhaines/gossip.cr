module Gossip
  module Messages
    module Base
      # Abstract base struct for all messages
      abstract struct Message
        include JSON::Serializable
        property sender : String
        property type : String

        def initialize(@sender)
          @type = self.class.name
        end
      end
    end
  end
end
