# Define debug logging macro based on compile-time flag
{% if flag?(:DEBUG) %}
  macro debug_log(message)
    STDERR.puts \{{message}}
  end
{% else %}
  macro debug_log(message)
    # Does nothing when DEBUG is not set
  end
{% end %}
