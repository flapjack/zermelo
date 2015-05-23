# The other side of a has_one, has_many, or has_sorted_set association

module Zermelo
  module Associations
    class BelongsTo

      # NB a single instance of this class doesn't need to care about the hash
      # used for storage, that should be done in the save method of the parent

      def initialize(parent, name)
        @parent = parent
        @name   = name

        @backend = parent.send(:backend)

        @record_ids_key = Zermelo::Records::Key.new(
          :klass  => parent.class,
          :id     => parent.id,
          :name   => 'belongs_to',
          :type   => :hash,
          :object => :association
        )

        parent.class.send(:with_association_data, name.to_sym) do |data|
          @associated_class = data.data_klass
          @lock_klasses     = [data.data_klass] + data.related_klasses
          @inverse          = data.inverse
          @callbacks        = data.callbacks
        end

        raise ':inverse_of must be set' if @inverse.nil?
        @inverse_key = "#{name}_id"
      end

      def value=(record)
        if record.nil?
          @parent.class.lock(*@lock_klasses) do
            r = @associated_class.send(:load, @backend.get(@record_ids_key)[@inverse_key.to_s])
            bc = @callbacks[:before_clear]
            if bc.nil? || !@parent.respond_to?(bc) || !@parent.send(bc, r).is_a?(FalseClass)
              new_txn = @backend.begin_transaction
              @backend.delete(@record_ids_key, @inverse_key)
              @backend.commit_transaction if new_txn
              ac = @callbacks[:after_clear]
              @parent.send(ac, r) if !ac.nil? && @parent.respond_to?(ac)
            end
          end
        else
          raise 'Invalid record class' unless record.is_a?(@associated_class)
          raise 'Record must have been saved' unless record.persisted?
          @parent.class.lock(*@lock_klasses) do
            bs = @callbacks[:before_set]
            if bs.nil? || !@parent.respond_to?(bs) || !@parent.send(bs, r).is_a?(FalseClass)
              new_txn = @backend.begin_transaction
              @backend.add(@record_ids_key, @inverse_key => record.id)
              @backend.commit_transaction if new_txn
              as = @callbacks[:after_set]
              @parent.send(as, record) if !as.nil? && @parent.respond_to?(as)
            end
          end
        end
      end

      def value
        v = nil
        @parent.class.lock(*@lock_klasses) do
          # FIXME uses hgetall, need separate getter for hash/list/set
          br = @callbacks[:before_read]
          @parent.send(br) if !br.nil? && @parent.respond_to?(br)
          id = @backend.get(@record_ids_key)[@inverse_key.to_s]
          # id = @backend.get_hash_value(@record_ids_key, @inverse_key.to_s)
          v = @associated_class.send(:load, id) unless id.nil?
          ar = @callbacks[:after_read]
          @parent.send(ar, v) if !ar.nil? && @parent.respond_to?(ar)
        end
        v
      end

      private

      # on_remove already runs inside a lock & transaction
      def on_remove
        unless value.nil?
          assoc = value.send("#{@inverse}_proxy".to_sym)
          if assoc.respond_to?(:remove)
            assoc.send(:remove, @parent)
          elsif assoc.respond_to?(:value=)
            assoc.send(:value=, nil)
          end
        end
        @backend.clear(@record_ids_key)
      end

      def self.associated_ids_for(backend, klass, name, inversed, *these_ids)
        these_ids.each_with_object({}) do |this_id, memo|
          key = Zermelo::Records::Key.new(
            :klass  => klass,
            :id     => this_id,
            :name   => 'belongs_to',
            :type   => :hash,
            :object => :association
          )

          assoc_id = backend.get(key)["#{name}_id"]
          # assoc_id = backend.get_hash_value(key, "#{name}_id")

          if inversed
            memo[assoc_id] ||= []
            memo[assoc_id] << this_id
          else
            memo[this_id] = assoc_id
          end
        end
      end

    end
  end
end