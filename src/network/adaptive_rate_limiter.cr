module Gossip
  module Network
    # AdaptiveRateLimiter monitors channel capacity and applies dynamic rate limiting
    class AdaptiveRateLimiter
      # Configuration parameters
      property capacity : Int32         # Total channel capacity
      property threshold_pct : Float64  # Percentage threshold before limiting starts (0.0-1.0)
      property max_delay_ms : Int32     # Maximum delay in milliseconds
      property base_delay_ms : Int32    # Base delay when threshold is just exceeded
      property adaptive_curve : Float64 # Exponent for the delay curve (higher = more aggressive)

      # Runtime state
      property current_count : Atomic(Int32) = Atomic.new(0)
      property last_delay : Atomic(Int32) = Atomic.new(0)
      property metrics : MetricsCollector

      def initialize(@capacity, @threshold_pct = 0.7, @base_delay_ms = 5, @max_delay_ms = 200, @adaptive_curve = 2.0)
        @metrics = MetricsCollector.new(@capacity)
      end

      # Called before adding to the channel
      def pre_send
        @current_count.add(1)
        count = @current_count.get
        @metrics.update_queue_size(count)

        # Record if the channel is at capacity
        if count >= @capacity
          @metrics.record_queue_full
        end
      end

      # Called after successfully sending to the channel
      def post_send
        @metrics.record_message_sent
      end

      # Called after receiving from the channel
      def post_receive
        @current_count.sub(1)
        @metrics.update_queue_size(@current_count.get)
        @metrics.record_message_received
      end

      # Calculate and apply delay based on current channel load
      def apply_rate_limit
        # Get current count atomically
        count = @current_count.get

        # Calculate fill percentage
        fill_percentage = count.to_f / @capacity

        # Determine if we need to apply rate limiting
        if fill_percentage >= @threshold_pct
          # Calculate how far beyond threshold we are (0.0-1.0 scale)
          severity = (fill_percentage - @threshold_pct) / (1.0 - @threshold_pct)
          severity = 1.0 if severity > 1.0

          # Calculate delay with progressive curve (can be tuned)
          # Using exponential growth for more aggressive limiting as we get closer to capacity
          delay_ms = (@base_delay_ms + (@max_delay_ms - @base_delay_ms) * (severity ** @adaptive_curve)).to_i

          # Store current delay for monitoring
          @last_delay.set(delay_ms)

          # Update metrics
          @metrics.record_rate_limiting(delay_ms)

          # Apply the delay
          sleep(delay_ms.milliseconds)

          return delay_ms
        else
          @last_delay.set(0)
          return 0
        end
      end

      # Get current status for logging/debugging
      def status
        {
          capacity:         @capacity,
          current:          @current_count.get,
          fill_percentage:  (@current_count.get.to_f / @capacity * 100).round(1),
          current_delay_ms: @last_delay.get,
          threshold_pct:    (@threshold_pct * 100).round(1),
        }
      end
    end
  end
end
