require 'forwardable'

# much like a has_many, but with different add/remove behaviour, as it's paired
# with another has_and_belongs_to_many association. both sides must set the
# inverse association name.

module Sandstorm
  module Associations
    class HasAndBelongsToMany

      extend Forwardable

      def_delegators :filter, :intersect, :union, :diff, :sort,
                       :find_by_id, :find_by_ids, :find_by_id!, :find_by_ids!,
                       :page, :all, :each, :collect, :map,
                       :select, :find_all, :reject, :destroy_all,
                       :ids, :count, :empty?, :exists?

      def initialize(parent, name)
        @parent = parent

        @backend = parent.send(:backend)

        @record_ids_key = Sandstorm::Records::Key.new(
          :klass  => parent.class.send(:class_key),
          :id     => parent.id,
          :name   => "#{name}_ids",
          :type   => :set,
          :object => :association
        )

        parent.class.send(:with_association_data, name.to_sym) do |data|
          @associated_class = data.data_klass
          @inverse          = data.inverse
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
        @parent.class.lock(@associated_class) do
          records.each do |record|
            @associated_class.send(:load, record.id).send(@inverse.to_sym).
              send(:add_without_inverse, @parent)
          end
          add_without_inverse(*records)
        end
      end

      # TODO support dependent delete, for now just deletes the association
      def delete(*records)
        raise 'No records to delete' if records.empty?
        raise 'Invalid record class' unless records.all? {|r| r.is_a?(@associated_class)}
        raise "Record(s) must have been saved" unless records.all? {|r| r.persisted?}
        @parent.class.lock(@associated_class) do
          records.each do |record|
            @associated_class.send(:load, record.id).send(@inverse.to_sym).
              send(:delete_without_inverse, @parent)
          end
          delete_without_inverse(*records)
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

      # creates a new filter class each time it's called, to store the
      # state for this particular filter chain
      def filter
        @backend.filter(@record_ids_key, @associated_class)
      end

      def self.associated_ids_for(backend, class_key, name, *these_ids)
        these_ids.each_with_object({}) do |this_id, memo|
          key = Sandstorm::Records::Key.new(
            :klass  => class_key,
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