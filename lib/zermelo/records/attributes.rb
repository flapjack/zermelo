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
        options.each_pair do |key, att_type|
          raise "Unknown attribute type ':#{att_type}' for ':#{key}'" unless
            Zermelo.valid_type?(att_type)
          self.define_attribute_methods([key])

          if key.to_s == 'id'
            define_method("id=".to_sym) do |v|
              raise "Cannot reassign id" unless @attributes['id'].nil?
              send("id_will_change!")
              @attributes['id'] = v.to_s
            end
          else
            define_method("#{key}=".to_sym) do |v|
              send("#{key}_will_change!")
              if (self.class.attribute_types[key.to_sym] == :set) && !v.is_a?(Set)
                @attributes[key.to_s] = Set.new(v)
              else
                @attributes[key.to_s] = v
              end
            end
          end

          define_method(key.to_sym) do
            value = @attributes[key.to_s]
            return value unless (self.class.attribute_types[key.to_sym] == :timestamp)
            value.is_a?(Integer) ? Time.at(value) : value
          end
        end
        @lock.synchronize do
          (@attribute_types ||= {}).update(options)
        end
      end

    end

  end

end