module Zermelo
  module Associations
    class Singular

      def initialize(type, parent_klass, parent_id, name)
        @type = type
        @parent_klass = parent_klass
        @parent_id = parent_id
        @name   = name

        @backend = parent_klass.send(:backend)

        key_name, key_type = case type
        when :belongs_to
          ['belongs_to', :hash]
        when :has_one
          # TODO can this be represented via a 'has_one' hash?
          ["#{name}_id", :string]
        end

        @record_ids_key = Zermelo::Records::Key.new(
          :klass  => parent_klass,
          :id     => parent_id,
          :name   => key_name,
          :type   => key_type,
          :object => :association
        )

        parent_klass.send(:with_association_data, name.to_sym) do |data|
          @associated_class = data.data_klass
          @lock_klasses     = [data.data_klass] + data.related_klasses
          @inverse          = data.inverse
          @callbacks        = data.callbacks
        end

        # this bit 'belongs_to' only
        raise ':inverse_of must be set' if @inverse.nil?
        @inverse_key = "#{name}_id"
      end

      def value=(record)
        if record.nil?
          @parent.class.lock(*@lock_klasses) do
            _clear(true)
          end
        else
          raise 'Invalid record class' unless record.is_a?(@associated_class)
          raise 'Record must have been saved' unless record.persisted?
          @parent_klass.lock(*@lock_klasses) do
            _set(true, record.id)
          end
        end
      end

      def value
        v = nil
        @parent.class.lock(*@lock_klasses) do
          br = @callbacks[:before_read]
          @parent_klass.send(br, @parent_id) if !br.nil? && @parent_klass.respond_to?(br)
          id = case @type
          when :belongs_to
            @backend.get(@record_ids_key)[@inverse_key.to_s]
            # # TODO maybe: uses hgetall, need separate getter for hash/list/set
            # @backend.get_hash_value(@record_ids_key, @inverse_key.to_s)
          when :has_one
            @backend.get(@record_id_key)
          end
          v = @associated_class.send(:load, id) unless id.nil?
          ar = @callbacks[:after_read]
          @parent_klass.send(ar, @parent_id, v) if !ar.nil? && @parent_klass.respond_to?(ar)
        end
        v
      end

      private

      def _clear(update_inverse)
        bc = @callbacks[:before_clear]
        if bc.nil? || !@parent_klass.respond_to?(bc) ||
          !@parent_klass.send(bc, @parent_id).is_a?(FalseClass)

          new_txn = @backend.begin_transaction

          if update_inverse
            # FIXME
          end

          case @type
          when :belongs_to
            @backend.delete(@record_ids_key, @inverse_key)
          when :has_one
            # FIXME
            # delete_without_lock(r)
          end

          @backend.commit_transaction if new_txn

          ac = @callbacks[:after_clear]
          @parent_klass.send(ac, @parent_id) if !ac.nil? && @parent_klass.respond_to?(ac)
        end
      end

      def _set(update_inverse, record_id)
        bs = @callbacks[:before_set]
        if bs.nil? || !@parent_klass.respond_to?(bs) ||
          !@parent_klass.send(bs, @parent_id, record_id).is_a?(FalseClass)

          new_txn = @backend.begin_transaction

          if update_inverse
            # FIXME

            # has_one
            # unless @inverse.nil?
            #   @associated_class.send(:load, record.id).send("#{@inverse}=", @parent)
            # end
          end

          case @type
          when :belongs_to
            @backend.add(@record_ids_key, @inverse_key => record_id)
          when :has_one
            @backend.set(@record_ids_key, record_id)
          end

          @backend.commit_transaction if new_txn

          as = @callbacks[:after_set]
          @parent_klass.send(as, @parent_id, record_id) if !as.nil? && @parent_klass.respond_to?(as)
        end
      end

      # on_remove already runs inside a lock & transaction
      def on_remove

        # # belongs_to
        # unless value.nil?
        #   assoc = value.send("#{@inverse}_proxy".to_sym)
        #   if assoc.respond_to?(:remove)
        #     assoc.send(:remove, @parent)
        #   elsif assoc.respond_to?(:value=)
        #     assoc.send(:value=, nil)
        #   end
        # end
        # @backend.clear(@record_ids_key)


        # has_one

        # unless @inverse.nil?
        #   if record_id = @backend.get(@record_id_key)
        #     @associated_class.send(:load, record_id).send("#{@inverse}=", nil)
        #   end
        # end
        # @backend.clear(@record_id_key)

      end

      def self.associated_ids_for(backend, type, klass, name, inversed, *these_ids)
        key_name, key_type = case type
        when :belongs_to
          ['belongs_to', :hash]
        when :has_one
          # TODO can this be represented via a 'has_one' hash?
          ["#{name}_id", :string]
        end

        these_ids.each_with_object({}) do |this_id, memo|
          key = Zermelo::Records::Key.new(
            :klass  => klass,
            :id     => this_id,
            :name   => key_name,
            :type   => key_type,
            :object => :association
          )

          case type
          when :belongs_to
            assoc_id = backend.get(key)["#{name}_id"]
            # assoc_id = backend.get_hash_value(key, "#{name}_id")

            if inversed
              memo[assoc_id] ||= []
              memo[assoc_id] << this_id
            else
              memo[this_id] = assoc_id
            end
          when :has_one
            memo[this_id] = backend.get(key)
          end
        end
      end

    end
  end
end
