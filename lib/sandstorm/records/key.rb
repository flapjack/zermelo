
module Sandstorm

  module Records

    class Key

      attr_reader :klass, :id, :name, :type, :object

      def initialize(opts = {})
        @klass  = opts[:class]
        @id     = opts[:id]      # if id.nil?, it's a class variable
        @name   = opts[:name]
        @type   = opts[:type]
        @object = opts[:object]  # :association, :attribute or :index
      end

    end

  end

end