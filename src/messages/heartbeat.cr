require "./base"

module Gossip
  module Messages
    module Heartbeat
      # Message for heartbeats
      struct Heartbeat < Base::Message
      end

      # Message for heartbeat acknowledgments
      struct HeartbeatAck < Base::Message
      end
    end
  end
end
