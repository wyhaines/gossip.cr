# Main entry point that requires all components
require "socket"
require "json"
require "mutex"
require "set"

# Load all components
require "./debug"
require "./messages/base"
require "./messages/membership"
require "./messages/broadcast"
require "./messages/heartbeat"
require "./messages/anti_entropy"
require "./network/address"
require "./network/node"
require "./protocol/config"
require "./protocol/reliable_broadcast"
require "./protocol/anti_entropy"
require "./protocol/node"
require "./protocol/handlers"

# Re-export main types for backward compatibility
module Gossip
  # Export types from each module
  alias Message = Messages::Base::Message
  alias Join = Messages::Membership::Join
  alias ForwardJoin = Messages::Membership::ForwardJoin
  alias Shuffle = Messages::Membership::Shuffle
  alias ShuffleReply = Messages::Membership::ShuffleReply
  alias InitViews = Messages::Membership::InitViews
  alias Redirect = Messages::Membership::Redirect # Add this line for the new message type
  alias BroadcastMessage = Messages::Broadcast::BroadcastMessage
  alias LazyPushMessage = Messages::Broadcast::LazyPushMessage
  alias MessageRequest = Messages::Broadcast::MessageRequest
  alias MessageResponse = Messages::Broadcast::MessageResponse
  alias Heartbeat = Messages::Heartbeat::Heartbeat
  alias HeartbeatAck = Messages::Heartbeat::HeartbeatAck
  alias DigestRequest = Messages::AntiEntropy::DigestRequest
  alias DigestReply = Messages::AntiEntropy::DigestReply
  alias MessagesRequest = Messages::AntiEntropy::MessagesRequest
  alias MessagesResponse = Messages::AntiEntropy::MessagesResponse

  alias NodeAddress = Network::NodeAddress
  alias NetworkNode = Network::NetworkNode
  alias Node = Protocol::Node
end

# Include everything in the global namespace for backward compatibility
include Gossip
