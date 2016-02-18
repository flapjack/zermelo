require 'rspec/core/formatters/base_formatter'

# Outputs how log each spec takes to run
class ProfileAllFormatter < RSpec::Core::Formatters::BaseFormatter
  RSpec::Core::Formatters.register self,
                                   :example_started, :example_passed,
                                   :start_dump

  def initialize(output)
    super(output)
    @example_times = []
  end

  def start(notification)
    super(notification)
    @output.puts 'Profiling enabled.'
  end

  def example_started(_notification)
    @time = (Time.respond_to?(:zone) && Time.zone) ? Time.zone.now : Time.now
  end

  def example_passed(notification)
    now = (Time.respond_to?(:zone) && Time.zone) ? Time.zone.now : Time.now
    @example_times << [
      notification.example.example_group.description,
      notification.example.description,
      now - @time
    ]
  end

  def start_dump(_notification)
    @output.puts "\n\nExample times:\n"

    @example_times = @example_times.sort_by do |_description, _example, time|
      time
    end.reverse

    @example_times.each do |description, example, time|
      @output.print format('%.7f', time)
      @output.puts " #{description} #{example}"
    end
    @output.flush
  end
end
