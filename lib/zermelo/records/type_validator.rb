module Zermelo
  module Records
    class TypeValidator < ActiveModel::Validator
      def validate(record)
        record.class.attribute_types.each_pair do |attr_name, type|
          value = record.send(attr_name)
          next if value.nil?
          valid_type = Zermelo::ALL_TYPES[type]
          next if valid_type.any? { |t| value.is_a?(t) }

          record.errors.add(attr_name, error_msg(valid_type, value))
        end
      end

      private

        def error_msg(valid_type, value)
          "should be #{valid_type.size > 1 ? 'one of ' : ''} " \
          "#{valid_type.collect(&:name).join(', ')} " \
          "but is #{value.class.name}"
        end
    end
  end
end
