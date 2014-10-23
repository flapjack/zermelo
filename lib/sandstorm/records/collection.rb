module Sandstorm
  module Records
    # high-level abstraction for a set or list of record ids
    class Collection
      attr_reader :klass, :name, :type

      def initialize(opts = {})
        @klass  = opts[:class]
        @name   = opts[:name]
        @type   = opts[:type]
      end
    end
  end
end