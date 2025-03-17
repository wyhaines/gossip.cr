module Gossip
  module Messages
    module Base
      # Abstract base struct for all messages
      abstract struct Message
        include JSON::Serializable
        property sender : String
        property type : String

        def initialize(@sender)
          # Extract just the class name without the namespace
          full_name = self.class.name
          @type = full_name.split("::").last
        end
      end
    end
  end
end
