module Zermelo
  module Associations
    class Singular

      def initialize(type, parent_klass, parent_id, name)
        @type = type
        @parent_klass = parent_klass
        @parent_id = parent_id
        @name   = name

        @backend = parent_klass.send(:backend)

        @record_id_key = Zermelo::Records::Key.new(
          :klass  => parent_klass,
          :id     => parent_id,
          :name   => type.to_s,
          :type   => :hash,
          :object => :association
        )

        parent_klass.send(:with_association_data, name.to_sym) do |data|
          @associated_class = data.data_klass
          @lock_klasses     = [data.data_klass] + data.related_klasses
          @inverse          = data.inverse
          @callbacks        = data.callbacks
        end

        raise ':inverse_of must be set' if @inverse.nil?
        @inverse_key = "#{@inverse}_id"
      end

      def value=(record)
        if record.nil?
          @parent_klass.lock(*@lock_klasses) do
            _clear(:callbacks => true)
          end
        else
          raise 'Invalid record class' unless record.is_a?(@associated_class)
          raise 'Record must have been saved' unless record.persisted?
          @parent_klass.lock(*@lock_klasses) do
            opts = {:callbacks => true}
            if :sorted_set.eql?(_inverse.type)
              opts[:score] = @parent_klass.find_by_id!(@parent_id).send(@inverse_sort_key.to_sym).to_f
            end
            _set(opts, record.id)
          end
        end
      end

      def value
        v = nil
        @parent_klass.lock(*@lock_klasses) do
          br = @callbacks[:before_read]
          @parent_klass.send(br, @parent_id) if !br.nil? && @parent_klass.respond_to?(br)
          id = @backend.get(@record_id_key)["#{@name}_id"]
          # # TODO maybe: uses hgetall, need separate getter for hash/list/set
          # @backend.get_hash_value(@record_id_key, "#{@name}_id")
          v = @associated_class.send(:load, id) unless id.nil?
          ar = @callbacks[:after_read]
          @parent_klass.send(ar, @parent_id, v) if !ar.nil? && @parent_klass.respond_to?(ar)
        end
        v
      end

      def key_dump
        [@backend.key_to_backend_key(@record_id_key), @record_id_key]
      end

      private

      # on_remove already runs inside a lock & transaction
      def on_remove
        _clear(:callbacks => false)
      end

      def _inverse
        return @inverse_obj unless @inverse_obj.nil?
        @associated_class.send(:with_association_data, @inverse.to_sym) do |data|
          @inverse_obj = case @type
          when :belongs_to
            key_name, key_type = case data.data_type
            when :has_many
              ["#{@inverse}_ids", :set]
            when :has_sorted_set
              ["#{@inverse}_ids", :sorted_set]
            when :has_one
              ["has_one", :hash]
            end

            @inverse_sort_key = data.sort_key

            Zermelo::Records::Key.new(
              :klass  => @associated_class,
              :name   => key_name,
              :type   => key_type,
              :object => :association
            )
          when :has_one
            # inverse is belongs_to
            Zermelo::Records::Key.new(
              :klass  => @associated_class,
              :name   => 'belongs_to',
              :type   => :hash,
              :object => :association
            )
          end
        end
        @inverse_obj
      end

      def _clear(opts = {})
        bc = @callbacks[:before_clear]
        if bc.nil? || !opts[:callbacks] || !@parent_klass.respond_to?(bc) ||
          !@parent_klass.send(bc, @parent_id).is_a?(FalseClass)

          record_id = @backend.get(@record_id_key)["#{@name}_id"]
          _inverse.id = record_id

          new_txn = @backend.begin_transaction

          case @type
          when :belongs_to, :has_one
            # FIXME can we access the assoc type instead?
            case _inverse.type
            when :set, :sorted_set
              @backend.delete(_inverse, @parent_id)
            when :hash
              @backend.delete(_inverse, "#{@inverse}_id")
            end

            @backend.delete(@record_id_key, "#{@name}_id")
          end

          @backend.commit_transaction if new_txn

          ac = @callbacks[:after_clear]
          if !ac.nil? && opts[:callbacks] && @parent_klass.respond_to?(ac)
            @parent_klass.send(ac, @parent_id, record_id)
          end
        end
      end

      def _set(opts = {}, record_id)
        bs = @callbacks[:before_set]
        if bs.nil? || !opts[:callbacks] || !@parent_klass.respond_to?(bs) ||
          !@parent_klass.send(bs, @parent_id, record_id).is_a?(FalseClass)

          _inverse.id = record_id

          new_txn = @backend.begin_transaction

          # FIXME can we access the assoc type instead?
          case _inverse.type
          when :set
            @backend.add(_inverse, @parent_id)
          when :sorted_set
            @backend.add(_inverse, [opts[:score], @parent_id])
          when :hash
            @backend.add(_inverse, @inverse_key => @parent_id)
          end

          @backend.add(@record_id_key, "#{@name}_id" => record_id)

          @backend.commit_transaction if new_txn

          as = @callbacks[:after_set]
          if !as.nil? && opts[:callbacks] && @parent_klass.respond_to?(as)
            @parent_klass.send(as, @parent_id, record_id)
          end
        end
      end

      def self.associated_ids_for(backend, type, klass, name, inversed, *these_ids)
        these_ids.each_with_object({}) do |this_id, memo|
          key = Zermelo::Records::Key.new(
            :klass  => klass,
            :id     => this_id,
            :name   => type.to_s,
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
