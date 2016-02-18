# Store logger messages in specs for possible later output
class MockLogger
  attr_accessor :messages, :errors

  def self.configure_log(name, _config = {})
    @name = name
  end

  def initialize
    @messages = []
    @errors   = []
  end

  def clear
    @messages.clear
    @errors.clear
  end

  %w(debug info warn).each do |level|
    class_eval <<-RUBY
      def #{level}(msg = nil, &block)
        msg = yield if msg.nil? && block_given?
        @messages << '[#{level.upcase}] :: ' +
          (self.class.instance_variable_get('@name') || 'flapjack') + ' :: ' +
          msg
      end
    RUBY
  end

  %w(error fatal).each do |level|
    class_eval <<-ERRORS
      def #{level}(msg = nil, &block)
        msg = yield if msg.nil? && block_given?
        @messages << '[#{level.upcase}] :: ' +
          (self.class.instance_variable_get('@name') || 'flapjack') + ' :: ' +
          msg
        @errors << '[#{level.upcase}] :: ' +
          (self.class.instance_variable_get('@name') || 'flapjack') + ' :: ' +
          msg
      end
    ERRORS
  end

  %w(debug info warn error fatal).each do |level|
    class_eval <<-LEVELS
      def #{level}?
        true
      end
    LEVELS
  end
end
