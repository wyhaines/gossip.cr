module Gossip
  module Network
    # NodeAddress represents a network location
    struct NodeAddress
      include JSON::Serializable
      property host : String
      property port : Int32
      property id : String

      def initialize(@host, @port, @id)
      end

      def to_s
        @id
      end
    end
  end
end
