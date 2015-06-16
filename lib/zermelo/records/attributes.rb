require 'zermelo'

module Zermelo
  module Records
    module Attributes

      def attribute_types
        ret = nil
        @lock.synchronize do
          ret = (@attribute_types ||= {}).dup
        end
        ret
      end

      protected

      def define_attributes(options = {})
        options.each_pair do |key, value|
          raise "Unknown attribute type ':#{value}' for ':#{key}'" unless
            Zermelo.valid_type?(value)
          self.define_attribute_methods([key])
        end
        @lock.synchronize do
          (@attribute_types ||= {}).update(options)
        end
      end

    end

  end

end