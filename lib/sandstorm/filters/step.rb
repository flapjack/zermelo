module Sandstorm

  module Filters

    class Step

      attr_reader :action, :options, :attributes

      def initialize(act, opts, attrs)
        @action     = act
        @options    = opts
        @attributes = attrs
      end

    end

  end

end
