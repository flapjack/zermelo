
module Sandstorm

  module Records

    class TypeValidator < ActiveModel::Validator
      def validate(record)
        attr_types = record.class.attribute_types

        attr_types.each_pair do |name, type|
          value = record.send(name)
          next if value.nil?
          valid_type = Sandstorm::ALL_TYPES[type]
          unless valid_type.any? {|type| value.is_a?(type) }
            count = (valid_type.size > 1) ? 'one of ' : ''
            type_str = valid_type.collect {|type| type.name }.join(", ")
            record.errors.add(name, "should be #{count}#{type_str} but is #{value.class.name}")
          end
        end
      end
    end

  end

end