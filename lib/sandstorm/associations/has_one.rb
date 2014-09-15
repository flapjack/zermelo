module Sandstorm
  module Associations
    class HasOne

      def initialize(parent, name, record_id_key, backend, options = {})
        @parent = parent
        @name = name

        @record_id_key = record_id_key
        @backend = backend

        # TODO trap possible constantize error
        @associated_class = (options[:class_name] || name.classify).constantize

        @inverse = @associated_class.send(:inverse_of, name.to_sym, @parent.class)
      end

      def value
        @backend.lock(@parent.class, @associated_class) do
          if id = @backend.get(@record_id_key)
            @associated_class.send(:load, id)
          else
            nil
          end
        end
      end

      def add(record)
        raise 'Invalid record class' unless record.is_a?(@associated_class)
        raise 'Record must have been saved' unless record.persisted?
        @parent.class.lock(@associated_class) do
          unless @inverse.nil?
            @associated_class.send(:load, record.id).send("#{@inverse}=", @parent)
          end

          new_txn = @backend.begin_transaction
          @backend.set(@record_id_key, record.id)
          @backend.commit_transaction if new_txn
        end
      end

      def delete(record)
        raise 'Invalid record class' unless record.is_a?(@associated_class)
        raise 'Record must have been saved' unless record.persisted?
        @parent.class.lock(@associated_class) do
          delete_without_lock(record)
        end
      end

      private

      def delete_without_lock(record)
        unless @inverse.nil?
          @associated_class.send(:load, @backend.get(@record_id_key)).send("#{@inverse}=", nil)
        end
        new_txn = @backend.begin_transaction
        @backend.clear(@record_id_key)
        @backend.commit_transaction if new_txn
      end

      # associated will be a belongs_to; on_remove already runs inside lock and transaction
      def on_remove
        unless @inverse.nil?
          if record_id = @backend.get(@record_id_key)
            @associated_class.send(:load, record_id).send("#{@inverse}=", nil)
          end
        end
        @backend.clear(@record_id_key)
      end

    end
  end
end