module Gossip
  module Network
    # Metrics tracker for collecting and reporting system performance data
    class MetricsCollector
      # Core metrics
      property total_messages_sent : Int64 = 0
      property total_messages_received : Int64 = 0
      property rate_limited_count : Int64 = 0
      property queue_full_count : Int64 = 0

      # Timing metrics
      property start_time : Time::Span
      property total_delay_ms : Int64 = 0

      # Queue metrics
      property peak_queue_size : Int32 = 0
      property current_queue_size : Int32 = 0
      property queue_capacity : Int32

      # Performance monitoring
      property last_throughput_check : Time::Span
      property messages_since_last_check : Int32 = 0
      property current_throughput : Float64 = 0.0
      property peak_throughput : Float64 = 0.0

      # Historical data (circular buffer of last N minutes, 1 sample/second)
      property history_size : Int32 = 300 # 5 minutes of history
      property queue_history : Array(Int32)
      property throughput_history : Array(Float64)
      property delay_history : Array(Int32)
      property last_history_update : Time::Span

      def initialize(@queue_capacity : Int32)
        @start_time = Time.monotonic
        @last_throughput_check = @start_time
        @last_history_update = @start_time
        @queue_history = Array(Int32).new(@history_size, 0)
        @throughput_history = Array(Float64).new(@history_size, 0.0)
        @delay_history = Array(Int32).new(@history_size, 0)
      end

      # Record a message being sent
      def record_message_sent
        @total_messages_sent += 1
        @messages_since_last_check += 1
        update_throughput
      end

      # Record a message being received/processed
      def record_message_received
        @total_messages_received += 1
        @messages_since_last_check += 1
        update_throughput
      end

      # Record rate limiting being applied
      def record_rate_limiting(delay_ms : Int32)
        @rate_limited_count += 1
        @total_delay_ms += delay_ms
      end

      # Record queue full event
      def record_queue_full
        @queue_full_count += 1
      end

      # Update queue size metrics
      def update_queue_size(size : Int32)
        @current_queue_size = size
        @peak_queue_size = size if size > @peak_queue_size
      end

      # Get average delay when rate limiting is applied
      def average_delay_ms : Float64
        return 0.0 if @rate_limited_count == 0
        @total_delay_ms.to_f / @rate_limited_count
      end

      # Calculate current message throughput (msgs/sec)
      def update_throughput
        now = Time.monotonic
        elapsed = now - @last_throughput_check

        # Update throughput every second
        if elapsed.total_seconds >= 1.0
          @current_throughput = @messages_since_last_check.to_f / elapsed.total_seconds
          @peak_throughput = @current_throughput if @current_throughput > @peak_throughput
          @messages_since_last_check = 0
          @last_throughput_check = now
        end

        # Update history every second
        history_elapsed = now - @last_history_update
        if history_elapsed.total_seconds >= 1.0
          # Shift histories and add new values
          @queue_history.shift
          @queue_history << @current_queue_size

          @throughput_history.shift
          @throughput_history << @current_throughput

          @delay_history.shift
          @delay_history << (@rate_limited_count > 0 ? (@total_delay_ms / @rate_limited_count).to_i : 0)

          @last_history_update = now
        end
      end

      # Calculate uptime in seconds
      def uptime_seconds : Float64
        (Time.monotonic - @start_time).total_seconds
      end

      # Get current queue utilization percentage
      def queue_utilization : Float64
        (@current_queue_size.to_f / @queue_capacity) * 100.0
      end

      # Get peak queue utilization percentage
      def peak_queue_utilization : Float64
        (@peak_queue_size.to_f / @queue_capacity) * 100.0
      end

      # Get formatted summary of metrics
      def summary : String
        String.build do |str|
          str << "=== Network Metrics Summary ===\n"
          str << "Uptime: #{format_duration(uptime_seconds)}\n"
          str << "Total Messages: #{@total_messages_received + @total_messages_sent} (#{@total_messages_received} received, #{@total_messages_sent} sent)\n"
          str << "Current Throughput: #{@current_throughput.round(2)} msgs/sec (peak: #{@peak_throughput.round(2)})\n"
          str << "Queue: #{@current_queue_size}/#{@queue_capacity} (#{queue_utilization.round(1)}% utilized, peak: #{peak_queue_utilization.round(1)}%)\n"
          str << "Rate Limiting: #{@rate_limited_count} events, avg delay: #{average_delay_ms.round(2)}ms\n"
          str << "Queue Full Events: #{@queue_full_count}\n"

          # Include recent queue trend (last minute)
          recent_trend = @queue_history[(@queue_history.size - 60)..] || @queue_history
          if !recent_trend.empty?
            avg_recent_util = (recent_trend.sum / recent_trend.size.to_f / @queue_capacity * 100.0).round(1)
            str << "Recent Queue Trend (1 min): #{avg_recent_util}% avg utilization\n"
          end
        end
      end

      # Get detailed metrics report as JSON-compatible Hash
      def detailed_metrics : Hash
        {
          "uptime_seconds" => uptime_seconds,
          "messages"       => {
            "total"    => @total_messages_received + @total_messages_sent,
            "received" => @total_messages_received,
            "sent"     => @total_messages_sent,
          },
          "throughput" => {
            "current" => @current_throughput,
            "peak"    => @peak_throughput,
          },
          "queue" => {
            "current"          => @current_queue_size,
            "peak"             => @peak_queue_size,
            "capacity"         => @queue_capacity,
            "utilization"      => queue_utilization,
            "peak_utilization" => peak_queue_utilization,
          },
          "rate_limiting" => {
            "events"           => @rate_limited_count,
            "average_delay_ms" => average_delay_ms,
            "queue_full_count" => @queue_full_count,
          },
          "history" => {
            "queue_utilization" => @queue_history.map { |size| (size.to_f / @queue_capacity * 100.0).round(1) },
            "throughput"        => @throughput_history.map { |tp| tp.round(1) },
            "delay_ms"          => @delay_history,
          },
        }
      end

      # Helper to format duration in human-readable form
      private def format_duration(seconds : Float64) : String
        hours = (seconds / 3600).to_i
        minutes = ((seconds % 3600) / 60).to_i
        secs = (seconds % 60).to_i

        if hours > 0
          "#{hours}h #{minutes}m #{secs}s"
        elsif minutes > 0
          "#{minutes}m #{secs}s"
        else
          "#{secs}s"
        end
      end
    end
  end
end
