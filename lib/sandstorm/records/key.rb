
module Sandstorm

  module Records

    class Key

      attr_reader :klass, :id, :name, :type

      def initialize(opts = {})
        @klass = opts[:class]
        @id    = opts[:id]      # if id.nil?, it's a class variable
        @name  = opts[:name]
        @type  = opts[:type]
      end

    end

  end

end