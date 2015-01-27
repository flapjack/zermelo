module Zermelo
  module Filters
    class Steps
      class BaseStep
        def self.accepted_types
          raise "Must be implemented in subclass"
        end

        def self.returns_type
          raise "Must be implemented in subclass"
        end

        attr_reader :options, :attributes

        def initialize(opts, attrs)
          @options    = opts
          @attributes = attrs
        end
      end
    end
  end
end
