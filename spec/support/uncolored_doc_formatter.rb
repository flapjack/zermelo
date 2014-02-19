require 'rspec/core/formatters/documentation_formatter'

class UncoloredDocFormatter < RSpec::Core::Formatters::DocumentationFormatter

   RSpec::Core::Formatters.register self, :example_group_started, :example_group_finished,
                                          :example_passed, :example_pending, :example_failed

  def color(text, color_code)
    text
  end

end