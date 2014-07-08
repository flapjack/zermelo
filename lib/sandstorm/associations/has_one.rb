require 'sandstorm'
require 'sandstorm/records/key'

module Sandstorm
  module Associations
    class HasOne

      def initialize(parent, name, options = {})
        @record_id = Sandstorm::Records::Key.new(
          :class  => parent.class.send(:class_key),
          :id     => parent.id,
          :name   => "#{name}_id",
          :type   => :string,
          :object => :association
        )

        @parent = parent

        # TODO trap possible constantize error
        @associated_class = (options[:class_name] || name.classify).constantize

        @inverse = @associated_class.send(:inverse_of, name.to_sym, @parent.class)
      end

      def value
        @parent.class.send(:lock, @parent.class, @associated_class) do
          if id = backend.get(@record_id)
            @associated_class.send(:load, id)
          else
            nil
          end
        end
      end

      def add(record)
        raise 'Invalid record class' unless record.is_a?(@associated_class)
        raise 'Record must have been saved' unless record.persisted?
        @parent.class.send(:lock, @parent.class, @associated_class) do
          unless @inverse.nil?
            @associated_class.send(:load, record.id).send("#{@inverse}=", @parent)
          end
          backend.set(@record_id, record.id)
        end
      end

      def delete(record)
        raise 'Invalid record class' unless record.is_a?(@associated_class)
        raise 'Record must have been saved' unless record.persisted?
        @parent.class.send(:lock, @parent.class, @associated_class) do
          delete_without_lock(record)
        end
      end

      private

      def delete_without_lock(record)
        unless @inverse.nil?
          @associated_class.send(:load, backend.get(@record_id)).send("#{@inverse}=", nil)
        end
        backend.clear(@record_id)
      end

      # associated will be a belongs_to; on_remove already runs inside lock
      def on_remove
        unless @inverse.nil?
          if record_id = backend.get(@record_id)
            @associated_class.send(:load, record_id).send("#{@inverse}=", nil)
          end
        end
        backend.clear(@record_id)
      end

      def backend
        @backend ||= @parent.class.send(:backend)
      end

    end
  end
end