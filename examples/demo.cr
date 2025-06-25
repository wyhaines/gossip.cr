require "../src/gossip"
require "../src/debug" # Import the debug macro

# Terminal UI Helper class
class TerminalUI
  # ANSI Escape codes
  CLEAR_SCREEN    = "\033[2J"
  CLEAR_LINE      = "\033[2K"
  CURSOR_UP       = "\033[1A"
  CURSOR_DOWN     = "\033[1B"
  CURSOR_TO_START = "\033[0G"
  SAVE_CURSOR     = "\033[s"
  RESTORE_CURSOR  = "\033[u"
  HIDE_CURSOR     = "\033[?25l"
  SHOW_CURSOR     = "\033[?25h"

  # Colors
  RESET  = "\033[0m"
  RED    = "\033[31m"
  GREEN  = "\033[32m"
  YELLOW = "\033[33m"
  BLUE   = "\033[34m"
  CYAN   = "\033[36m"
  BOLD   = "\033[1m"

  # Properties
  property status_lines : Int32 = 6
  property input_buffer : String = ""
  property messages : Array(String) = [] of String
  property max_messages : Int32 = 20
  property terminal_height : Int32 = 0
  property terminal_width : Int32 = 0
  property status_text : String = ""

  # Mutex to prevent UI corruption
  @ui_mutex = Mutex.new

  def initialize
    # Get terminal size
    update_terminal_size

    # Setup terminal and cleanup on exit
    setup_terminal
    at_exit { restore_terminal }
  end

  def setup_terminal
    # Hide cursor during operation
    print HIDE_CURSOR
    clear_screen
  end

  def restore_terminal
    # Restore cursor and terminal state
    print "#{CLEAR_SCREEN}#{CURSOR_TO_START}#{SHOW_CURSOR}"
  end

  def update_terminal_size
    # Get terminal size using tput if available
    width_output = `tput cols`.strip rescue "80"
    height_output = `tput lines`.strip rescue "24"

    @terminal_width = width_output.to_i
    @terminal_height = height_output.to_i
  end

  def add_message(message : String)
    @ui_mutex.synchronize do
      @messages << message
      # Keep only the last N messages
      if @messages.size > @max_messages
        @messages.shift
      end
      redraw
    end
  end

  def update_status(status_text : String)
    @ui_mutex.synchronize do
      @status_text = status_text
      redraw
    end
  end

  def handle_keypress(key : Char)
    @ui_mutex.synchronize do
      case key
      when '\u007F' # Backspace
        @input_buffer = @input_buffer[0...-1] if @input_buffer.size > 0
      when '\r', '\n' # Enter
        command = @input_buffer
        @input_buffer = ""
        redraw
        return command
      else
        @input_buffer += key if key.ord >= 32 && key.ord < 127
      end
      redraw
    end
    return nil
  end

  def clear_screen
    print "#{CLEAR_SCREEN}#{CURSOR_TO_START}"
  end

  def redraw
    # Reset the terminal state completely
    print CLEAR_SCREEN # Clear the entire screen
    print "\033[H"     # Move cursor to home position (0,0)

    # Draw header
    puts "#{BOLD}#{BLUE}=== Gossip Protocol Demo ===#{RESET}"
    puts "#{YELLOW}Messages:#{RESET}"

    # Calculate available height for the message area
    message_area_height = @terminal_height - @status_lines - 5
    message_area_height = 5 if message_area_height < 5 # Ensure minimum height

    # Select messages to display
    displayed_messages = @messages.size > message_area_height ? @messages[-message_area_height..-1] : @messages

    # Draw message area
    if displayed_messages.empty?
      puts "  No messages yet"
    else
      displayed_messages.each do |msg|
        # Process and wrap each message
        lines = wrap_text(msg, @terminal_width - 4) # -4 for indent and safety margin
        lines.each do |line|
          print "  " # Indent
          print line
          print "\r\n" # Explicit carriage return + new line
        end
      end
    end

    # Draw status area
    print "\r\n" # Ensure we start on a new line
    print "#{YELLOW}#{BOLD}=== Network Status ===#{RESET}\r\n"
    if @status_text
      @status_text.lines.each do |line|
        print line.chomp # Remove any existing line endings
        print "\r\n"     # Add explicit CR+LF
      end
    end

    # Draw input area at the bottom
    print "\r\n" # Ensure we start on a new line
    print "#{CYAN}Enter message (or commands: /status, /help, /nodes, /quit):#{RESET}\r\n"
    print "> #{@input_buffer}"

    # Flush output to ensure everything is displayed
    STDOUT.flush
  end

  # Helper to wrap text to fit the terminal width
  private def wrap_text(text : String, width : Int32) : Array(String)
    result = [] of String

    text.lines.each do |line|
      # Process each line of the message
      remaining = line.chomp # Remove existing line endings

      while remaining.size > width
        # Find a good break point
        break_at = width
        while break_at > 0 && !remaining[break_at].ascii_whitespace?
          break_at -= 1
        end

        # If no good break found, force a break at width
        break_at = width if break_at == 0

        result << remaining[0...break_at]
        remaining = remaining[break_at..-1].lstrip # Remove leading whitespace from remainder
      end

      result << remaining unless remaining.empty?
    end

    result
  end
end

# Demo node class with enhanced message display and status information
class DemoNode < Node
  @input_channel = Channel(String).new(10)
  @operation_in_progress = false
  @operation_mutex = Mutex.new
  @ui : TerminalUI

  def initialize(id : String, network : NetworkNode, initial_network_size : Int32 = 10)
    super(id, network, initial_network_size)
    @ui = TerminalUI.new
    update_status
  end

  def handle_broadcast(message : BroadcastMessage)
    if !@received_messages.includes?(message.message_id)
      @messages_mutex.synchronize do
        @received_messages << message.message_id
        @message_contents[message.message_id] = message.content
      end

      # Format and display received message
      formatted_message = "#{TerminalUI::GREEN}[#{Time.utc}] Message from #{message.sender}:#{TerminalUI::RESET}\n  #{TerminalUI::BLUE}#{message.content}#{TerminalUI::RESET}"
      @ui.add_message(formatted_message)

      # Forward to others as per normal gossip protocol
      active_nodes = [] of String
      @views_mutex.synchronize do
        active_nodes = @active_view
      end

      active_nodes.each do |node|
        next if node == message.sender

        @failures_mutex.synchronize do
          next if @failed_nodes.includes?(node)
        end

        begin
          if rand < @lazy_push_probability
            lazy_msg = LazyPushMessage.new(@id, message.message_id)
            send_message(node, lazy_msg)
          else
            forward_msg = BroadcastMessage.new(@id, message.message_id, message.content)
            send_message(node, forward_msg)
          end
        rescue ex
          debug_log "Node #{@id}: Failed to forward message to #{node}: #{ex.message}"
          handle_node_failure(node)
        end
      end
    end
  end

  def handle_join(message : Join)
    super
    update_status
  end

  def update_status
    status = String.build do |str|
      @views_mutex.synchronize do
        str << "  Active connections: #{active_view.size}/#{max_active_view_size}\n"
        str << "  Active nodes: #{active_view.empty? ? "None" : active_view.join(", ")}\n"
        str << "  Passive nodes: #{passive_view.empty? ? "None" : passive_view.join(", ")}\n"
      end

      @failures_mutex.synchronize do
        unless @failed_nodes.empty?
          str << "  Failed nodes: #{@failed_nodes.join(", ")}\n"
        end
      end

      @messages_mutex.synchronize do
        str << "  Messages received: #{@received_messages.size}\n"
        str << "  Missing messages: #{@missing_messages.keys.size}\n"
      end
    end

    @ui.update_status(status)
  end

  # Asynchronous broadcast with timeout
  def async_broadcast(message : String)
    @operation_mutex.synchronize do
      @operation_in_progress = true
    end

    @ui.add_message("#{TerminalUI::YELLOW}Sending message...#{TerminalUI::RESET}")

    # Create a channel to wait for completion
    done_channel = Channel(Bool).new(1)

    spawn do
      begin
        message_id = broadcast(message)
        @ui.add_message("#{TerminalUI::GREEN}Message sent successfully (ID: #{message_id})!#{TerminalUI::RESET}")
        done_channel.send(true)
      rescue ex
        @ui.add_message("#{TerminalUI::RED}Error sending message: #{ex.message}#{TerminalUI::RESET}")
        done_channel.send(false)
      end
    end

    # Set timeout to avoid blocking indefinitely
    spawn do
      sleep 5.seconds
      done_channel.send(false) rescue nil
    end

    # Wait for completion or timeout
    success = done_channel.receive

    @operation_mutex.synchronize do
      @operation_in_progress = false
    end
    update_status
    success
  end

  # Start input processing fiber
  def start_input_processing
    spawn do
      while @network.running
        input = @input_channel.receive
        process_input(input)
      end
    end
  end

  # Queue input for processing
  def queue_input(input : String)
    @input_channel.send(input) rescue nil
  end

  # Process input commands
  private def process_input(input : String)
    case input.strip
    when "/status"
      update_status
    when "/help"
      print_help
    when "/nodes"
      print_nodes_detail
    when "/quit"
      @ui.add_message("#{TerminalUI::YELLOW}Shutting down...#{TerminalUI::RESET}")
      @network.close
    else
      return if input.strip.empty?
      async_broadcast(input)
    end
  end

  # More detailed node information
  private def print_nodes_detail
    status = String.build do |str|
      str << "#{TerminalUI::YELLOW}Active Nodes:#{TerminalUI::RESET}\n"
      @views_mutex.synchronize do
        if @active_view.empty?
          str << "  No active connections\n"
        else
          @active_view.each do |node|
            conn_status = "#{TerminalUI::GREEN}Connected#{TerminalUI::RESET}"
            begin
              unless @network.test_connection(node)
                conn_status = "#{TerminalUI::YELLOW}Unreachable#{TerminalUI::RESET}"
              end
            rescue
              conn_status = "#{TerminalUI::RED}Failed#{TerminalUI::RESET}"
            end
            str << "  #{node} - #{conn_status}\n"
          end
        end
      end

      str << "\n#{TerminalUI::YELLOW}Passive Nodes:#{TerminalUI::RESET}\n"
      @views_mutex.synchronize do
        if @passive_view.empty?
          str << "  No passive connections\n"
        else
          str << "  #{@passive_view.join(", ")}\n"
        end
      end
    end

    @ui.add_message(status)
  end

  def print_help
    help_text = <<-HELP
    #{TerminalUI::YELLOW}Available commands:#{TerminalUI::RESET}
      /status  - Show current network status
      /nodes   - Show detailed node connection status
      /help    - Show this help message
      /quit    - Exit the program
      
    Any other input will be broadcast as a message to all connected nodes.
    HELP

    @ui.add_message(help_text)
  end
end

# Advanced input handler for raw key input
def handle_raw_input(node : DemoNode)
  # Save current terminal settings
  saved_term_attrs = `stty -g`.strip

  begin
    # Put terminal in raw mode
    system("stty raw -echo")

    while node.network.running
      if char = STDIN.raw &.read_char
        case char
        when 3.chr # Ctrl+C
          node.queue_input("/quit")
          break
        when '\r', '\n'
          # Get the current input buffer
          input = node.@ui.input_buffer
          if !input.empty?
            node.queue_input(input)
            node.@ui.input_buffer = ""
            node.@ui.redraw
          end
        else
          if command = node.@ui.handle_keypress(char)
            node.queue_input(command)
          end
        end
      end
    end
  ensure
    # Restore terminal settings
    system("stty #{saved_term_attrs}")
  end
end

# Command line argument parsing
case ARGV[0]?
when "bootstrap"
  # Start the bootstrap node
  address = NodeAddress.new("localhost", 7001, "node1@localhost:7001")
  network = NetworkNode.new(address)
  node = DemoNode.new("node1@localhost:7001", network)

  # Start asynchronous input processing
  node.start_input_processing
  handle_raw_input(node)

  # Clean shutdown
  network.close
when "join"
  if ARGV.size < 3
    puts "Usage: crystal run examples/demo.cr join <port> <node_id>"
    exit 1
  end

  port = ARGV[1].to_i
  node_id = ARGV[2]
  full_id = "#{node_id}@localhost:#{port}"
  address = NodeAddress.new("localhost", port, full_id)
  network = NetworkNode.new(address)
  node = DemoNode.new(full_id, network)

  # Join the network through the bootstrap node
  node.@ui.add_message("#{TerminalUI::GREEN}Joining network as #{full_id}...#{TerminalUI::RESET}")
  begin
    join_msg = Join.new(full_id)
    network.send_message("node1@localhost:7001", join_msg)
    node.print_help
  rescue ex
    node.@ui.add_message("#{TerminalUI::RED}Failed to join network: #{ex.message}#{TerminalUI::RESET}")
    node.@ui.add_message("Is the bootstrap node running?")
    network.close
    exit 1
  end

  # Start asynchronous input processing
  node.start_input_processing
  handle_raw_input(node)

  # Clean shutdown
  network.close
when "help", "-h", "--help"
  puts <<-USAGE
  Usage:
    crystal run examples/demo.cr bootstrap                  # Start bootstrap node
    crystal run examples/demo.cr join <port> <node_id>      # Join network
    crystal run examples/demo.cr help                       # Show this help

  Example:
    # In terminal 1:
    crystal run examples/demo.cr bootstrap

    # In terminal 2:
    crystal run examples/demo.cr join 7002 node2

    # In terminal 3:
    crystal run examples/demo.cr join 7003 node3
  USAGE
else
  puts "Unknown command. Use 'crystal run examples/demo.cr help' for usage information."
  exit 1
end
