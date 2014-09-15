# The other side of a has_one, has_many, or has_sorted_set association

module Sandstorm
  module Associations
    class BelongsTo

      # NB a single instance of this class doesn't need to care about the hash
      # used for storage, that should be done in the save method of the parent

      def initialize(parent, name, record_ids_key, backend, options = {})
        @parent = parent
        @name = name

        @record_ids_key = record_ids_key
        @backend = backend

        # TODO trap possible constantize error
        @associated_class = (options[:class_name] || name.classify).constantize
        @class_name = options[:class_name].demodulize.underscore

        raise ':inverse_of must be set' if options[:inverse_of].nil?
        @inverse = options[:inverse_of].to_s
        @inverse_key = "#{@name}_id"
      end

      def inverse_of?(source)
        !source.nil? && (@inverse == source.to_s)
      end

      # intrinsically atomic, so no locking needed
      def value=(record)
        new_txn = @backend.begin_transaction
        if record.nil?
          @backend.delete(@record_ids_key, @inverse_key)
        else
          raise 'Invalid record class' unless record.is_a?(@associated_class)
          raise 'Record must have been saved' unless record.persisted?
          @backend.add(@record_ids_key, @inverse_key => record.id)
        end
        @backend.commit_transaction if new_txn
      end

      def value
        @parent.class.lock(@associated_class) do
          # FIXME uses hgetall, need separate getter for hash/list/set
          if id = @backend.get(@record_ids_key)[@inverse_key.to_s]
            @associated_class.send(:load, id)
          else
            nil
          end
        end
      end

      private

      # on_remove already runs inside a lock & transaction
      def on_remove
        value.send("#{@inverse}_proxy".to_sym).send(:delete, @parent) unless value.nil?
        @backend.clear(@record_ids_key)
      end

    end
  end
end