module Sandstorm
  module Associations
    class HasOne

      def initialize(parent, name)
        @parent = parent

        @backend = parent.send(:backend)

        # TODO would be better as a 'has_one' hash, a bit like belongs_to
        @record_id_key = Sandstorm::Records::Key.new(
          :klass  => parent.class.send(:class_key),
          :id     => parent.id,
          :name   => "#{name}_id",
          :type   => :string,
          :object => :association
        )

        parent.class.send(:with_association_data, name.to_sym) do |data|
          @associated_class = data.data_klass
          @inverse          = data.inverse
          @callbacks        = data.callbacks
        end
      end

      def value
        @parent.class.lock(@associated_class) do
          if id = @backend.get(@record_id_key)
            @associated_class.send(:load, id)
          else
            nil
          end
        end
      end

      def value=(record)
        if record.nil?
          @parent.class.lock(@associated_class) do
           id = @backend.get(@record_id_key)
           unless id.nil?
             r = @associated_class.send(:load, id)
             unless r.nil?
               bc = @callbacks[:before_clear]
               if bc.nil? || !@parent.respond_to?(bc) || !@parent.send(bc, r).is_a?(FalseClass)
                  delete_without_lock(r)
                  ac = @callbacks[:after_clear]
                  @parent.send(ac, r) if !ac.nil? && @parent.respond_to?(ac)
                end
              end
            end
          end
        else
          raise 'Invalid record class' unless record.is_a?(@associated_class)
          raise 'Record must have been saved' unless record.persisted?
          @parent.class.lock(@associated_class) do
            bs = @callbacks[:before_set]
            if bs.nil? || !@parent.respond_to?(bs) || !@parent.send(bs, r).is_a?(FalseClass)
              unless @inverse.nil?
                @associated_class.send(:load, record.id).send("#{@inverse}=", @parent)
              end

              new_txn = @backend.begin_transaction
              @backend.set(@record_id_key, record.id)
              @backend.commit_transaction if new_txn
              as = @callbacks[:after_set]
              @parent.send(as, record) if !as.nil? && @parent.respond_to?(as)
            end
          end
        end
      end

      private

      def delete_without_lock(record)
        unless @inverse.nil?
          record.send("#{@inverse}=", nil)
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

      def self.associated_ids_for(backend, class_key, name, *these_ids)
        these_ids.each_with_object({}) do |this_id, memo|
          key = Sandstorm::Records::Key.new(
            :klass  => class_key,
            :id     => this_id,
            :name   => "#{name}_id",
            :type   => :string,
            :object => :association
          )
          memo[this_id] = backend.get(key)
        end
      end

    end
  end
end