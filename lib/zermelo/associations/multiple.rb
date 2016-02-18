require 'forwardable'

module Zermelo
  module Associations
    class Multiple

      extend Forwardable

      def_delegators :filter,
        :intersect, :union, :diff, :sort, :offset, :page, :empty,
        :find_by_id, :find_by_ids, :find_by_id!, :find_by_ids!,
        :all, :each, :collect, :map,
        :select, :find_all, :reject, :destroy_all,
        :ids, :count, :empty?, :exists?,
        :associated_ids_for, :associations_for

      def initialize(type, parent_klass, parent_id, name)
        @type         = type
        @parent_klass = parent_klass
        @parent_id    = parent_id
        @name         = name

        @backend      = parent_klass.send(:backend)

        @key_type = case @type
        when :has_many, :has_and_belongs_to_many
          :set
        when :has_sorted_set
          :sorted_set
        end

        @record_ids_key = Zermelo::Records::Key.new(
          :klass  => parent_klass,
          :id     => parent_id,
          :name   => "#{name}_ids",
          :type   => @key_type,
          :object => :association
        )

        parent_klass.send(:with_association_data, name.to_sym) do |data|
          @associated_class = data.data_klass
          @lock_klasses     = [data.data_klass] + data.related_klasses
          @inverse          = data.inverse
          @sort_key         = data.sort_key
          @sort_order       = data.sort_order
          @callbacks        = data.callbacks
        end

        raise ':inverse_of must be set' if @inverse.nil?
      end

      def first
        # FIXME raise error unless :has_sorted_set.eql?(@type)
        filter.first
      end

      def last
        # FIXME raise error unless :has_sorted_set.eql?(@type)
        filter.last
      end

      def <<(record)
        add(record)
        self  # for << 'a' << 'b'
      end

      def add(*records)
        raise 'No records to add' if records.empty?
        raise 'Invalid record class' if records.any? {|r| !r.is_a?(@associated_class)}
        raise 'Record(s) must have been saved' unless records.all? {|r| r.persisted?} # may need to be moved
        @parent_klass.lock(*@lock_klasses) do
          record_ids = case @type
          when :has_many, :has_and_belongs_to_many
            records.is_a?(Zermelo::Filter) ? records.ids : records.map(&:id)
          when :has_sorted_set
            records.map {|r| [r.send(@sort_key.to_sym).to_f, r.id]}
          end
          _add_ids({:callbacks => true}, *record_ids)
        end
      end

      def add_ids(*record_ids)
        raise 'No record ids to add' if record_ids.empty?
        @parent_klass.lock(*@lock_klasses) do
          @associated_class.find_by_ids!(*record_ids) # ensure they exist
          _add_ids({:callbacks => true}, *record_ids)
        end
      end

      # TODO support dependent delete, for now just removes the association
      def remove(*records)
        raise 'No records to remove' if records.empty?
        raise 'Invalid record class' if records.any? {|r| !r.is_a?(@associated_class)}
        raise 'Record(s) must have been saved' unless records.all? {|r| r.persisted?} # may need to be moved
        @parent_klass.lock(*@lock_klasses) do
          _remove_ids({:callbacks => true},
            *(records.is_a?(Zermelo::Filter) ? records.ids : records.map(&:id)))
        end
      end

      def remove_ids(*record_ids)
        raise 'No record ids to remove' if record_ids.empty?
        @parent_klass.lock(*@lock_klasses) do
          _remove_ids({:callbacks => true}, *record_ids)
        end
      end

      def clear
        @parent_klass.lock(*@lock_klasses) do
          _remove_ids({:callbacks => true}, *filter.ids) unless filter.empty?
        end
      end

      def key_dump
        [@backend.key_to_backend_key(@record_ids_key), @record_ids_key]
      end

      private

      def _inverse
        return @inverse_obj unless @inverse_obj.nil?
        @associated_class.send(:with_association_data, @inverse.to_sym) do |data|
          @inverse_obj = case @type
          when :has_many, :has_sorted_set
            # inverse is belongs_to
            # FIXME neater to do multiple hash keys at once, if backends support it
            Zermelo::Records::Key.new(
              :klass  => @associated_class,
              :name   => 'belongs_to',
              :type   => :hash,
              :object => :association
            )
          when :has_and_belongs_to_many
            Zermelo::Records::Key.new(
              :klass  => @associated_class,
              :name   => "#{@inverse}_ids",
              :type   => :set,
              :object => :association
            )
          end
        end
        @inverse_obj
      end

      # associated will be a belongs_to; on remove already runs inside a lock and transaction
      def on_remove
        _remove_ids({:callbacks => false}, *filter.ids) unless filter.empty?
      end

      def _add_ids(opts = {}, *record_ids)
        ba = @callbacks[:before_add]
        if ba.nil? || !opts[:callbacks] || !@parent_klass.respond_to?(ba) ||
          !@parent_klass.send(ba, @parent_id, *record_ids).is_a?(FalseClass)

          new_txn = @backend.begin_transaction

          # FIXME neater to do multiple hash keys at once for inverse, if backends support it
          case @type
          when :has_many
            # inverse is belongs_to
            record_ids.each do |record_id|
              _inverse_copy = _inverse.clone
              _inverse_copy.id = record_id
              @backend.add(_inverse_copy, "#{@inverse}_id" => @parent_id)
            end
          when :has_sorted_set
            # inverse is belongs_to
            record_ids.each do |score_and_record_id|
              _inverse_copy = _inverse.clone
              _inverse_copy.id = score_and_record_id.last
              @backend.add(_inverse_copy, "#{@inverse}_id" => @parent_id)
            end
          when :has_and_belongs_to_many
            # inverse is has_and_belongs_to_many
            record_ids.each do |record_id|
              _inverse_copy = _inverse.clone
              _inverse_copy.id = record_id
              @backend.add(_inverse_copy, @parent_id)
            end
          end

          @backend.add(@record_ids_key, record_ids)

          @backend.commit_transaction if new_txn
          aa = @callbacks[:after_add]
          if !aa.nil? && opts[:callbacks] && @parent_klass.respond_to?(aa)
            @parent_klass.send(aa, @parent_id, *record_ids)
          end
        end
      end

      def _remove_ids(opts = {}, *record_ids)
        br = @callbacks[:before_remove]
        if br.nil? || !opts[:callbacks] || !@parent_klass.respond_to?(br) ||
          !@parent_klass.send(br, @parent_id, *record_ids).is_a?(FalseClass)

          new_txn = @backend.begin_transaction

          # FIXME neater to do multiple hash keys at once for inverse, if backends support it
          case @type
          when :has_many, :has_sorted_set
            # inverse is belongs_to
            record_ids.each do |record_id|
              _inverse_copy = _inverse.clone
              _inverse_copy.id = record_id
              @backend.delete(_inverse_copy, "#{@inverse}_id")
            end
          when :has_and_belongs_to_many
            # inverse is has_and_belongs_to_many
            record_ids.each do |record_id|
              _inverse_copy = _inverse.clone
              _inverse_copy.id = record_id
              @backend.delete(_inverse_copy, @parent_id)
            end
          end

          @backend.delete(@record_ids_key, record_ids)

          @backend.commit_transaction if new_txn

          ar = @callbacks[:after_remove]
          if !ar.nil? && opts[:callbacks] && @parent_klass.respond_to?(ar)
            @parent_klass.send(ar, @parent_id, *record_ids)
          end
        end
      end

      # creates a new filter class each time it's called, to store the
      # state for this particular filter chain
      def filter
        @backend.filter(@record_ids_key, @associated_class, @parent_klass,
                        @parent_id, @callbacks, @sort_order)
      end

      def self.associated_ids_for(backend, type, klass, name, *these_ids)
        key_type = case type
        when :has_many, :has_and_belongs_to_many
          :set
        when :has_sorted_set
          :sorted_set
        end

        these_ids.each_with_object({}) do |this_id, memo|
          key = Zermelo::Records::Key.new(
            :klass  => klass,
            :id     => this_id,
            :name   => "#{name}_ids",
            :type   => key_type,
            :object => :association
          )
          memo[this_id] = backend.get(key)
        end
      end

    end
  end
end
