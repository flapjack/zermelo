require 'forwardable'

# much like a has_many, but with different add/remove behaviour, as it's paired
# with another has_and_belongs_to_many association. both sides must set the
# inverse association name.

module Zermelo
  module Associations
    class HasAndBelongsToMany

      extend Forwardable

      def_delegators :filter, :intersect, :union, :diff, :sort,
                       :find_by_id, :find_by_ids, :find_by_id!, :find_by_ids!,
                       :page, :all, :each, :collect, :map,
                       :select, :find_all, :reject, :destroy_all,
                       :ids, :count, :empty?, :exists?,
                       :associated_ids_for

      def initialize(parent, name)
        @parent = parent

        @backend = parent.send(:backend)

        @record_ids_key = Zermelo::Records::Key.new(
          :klass  => parent.class,
          :id     => parent.id,
          :name   => "#{name}_ids",
          :type   => :set,
          :object => :association
        )

        parent.class.send(:with_association_data, name.to_sym) do |data|
          @associated_class = data.data_klass
          @lock_klasses     = [data.data_klass] + data.related_klasses
          @inverse          = data.inverse
          @callbacks        = data.callbacks
        end
      end

      def <<(record)
        add(record)
        self  # for << 'a' << 'b'
      end

      def add(*records)
        raise 'No records to add' if records.empty?
        raise 'Invalid record class' unless records.all? {|r| r.is_a?(@associated_class)}
        raise "Record(s) must have been saved" unless records.all? {|r| r.persisted?}
        @parent.class.lock(*@lock_klasses) do
          ba = @callbacks[:before_add]
          if ba.nil? || !@parent.respond_to?(ba) || !@parent.send(ba, *records).is_a?(FalseClass)
            records.each do |record|
              @associated_class.send(:load, record.id).send(@inverse.to_sym).
                send(:add_without_inverse, @parent)
            end
            add_without_inverse(*records)
            aa = @callbacks[:after_add]
            @parent.send(aa, *records) if !aa.nil? && @parent.respond_to?(aa)
          end
        end
      end

      # TODO support dependent delete, for now just deletes the association
      def delete(*records)
        raise 'No records to delete' if records.empty?
        raise 'Invalid record class' unless records.all? {|r| r.is_a?(@associated_class)}
        raise "Record(s) must have been saved" unless records.all? {|r| r.persisted?}
        @parent.class.lock(*@lock_klasses) do
          _delete(*records)
        end
      end

      def clear
        @parent.class.lock(*@lock_klasses) do
          records = filter.all
          _delete(*records) unless records.empty?
        end
      end

      private

      def add_without_inverse(*records)
        new_txn = @backend.begin_transaction
        @backend.add(@record_ids_key, records.map(&:id))
        @backend.commit_transaction if new_txn
      end

      def delete_without_inverse(*records)
        new_txn = @backend.begin_transaction
        @backend.delete(@record_ids_key, records.map(&:id))
        @backend.commit_transaction if new_txn
      end

      # associated will be the other side of the HaBTM; on_remove is always
      # called inside a lock
      def on_remove
        ids.each do |record_id|
          @associated_class.send(:load, record_id).send(@inverse.to_sym).
            send(:delete_without_inverse, @parent)
        end
        @backend.purge(@record_ids_key)
      end

      def _delete(*records)
        br = @callbacks[:before_remove]
        if br.nil? || !@parent.respond_to?(br) || !@parent.send(br, *records).is_a?(FalseClass)
          records.each do |record|
            @associated_class.send(:load, record.id).send(@inverse.to_sym).
              send(:delete_without_inverse, @parent)
          end
          delete_without_inverse(*records)
          ar = @callbacks[:after_remove]
          @parent.send(ar, *records) if !ar.nil? && @parent.respond_to?(ar)
        end
      end

      # creates a new filter class each time it's called, to store the
      # state for this particular filter chain
      def filter
        @backend.filter(@record_ids_key, @associated_class, @parent, @callbacks)
      end

      def self.associated_ids_for(backend, klass, name, *these_ids)
        these_ids.each_with_object({}) do |this_id, memo|
          key = Zermelo::Records::Key.new(
            :klass  => klass,
            :id     => this_id,
            :name   => "#{name}_ids",
            :type   => :set,
            :object => :association
          )
          memo[this_id] = backend.get(key)
        end
      end

    end
  end
end