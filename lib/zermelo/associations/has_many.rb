require 'forwardable'

module Zermelo
  module Associations
    class HasMany

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
        raise 'Invalid record class' if records.any? {|r| !r.is_a?(@associated_class)}
        raise 'Record(s) must have been saved' unless records.all? {|r| r.persisted?} # may need to be moved
        @parent.class.lock(*@lock_klasses) do
          ba = @callbacks[:before_add]
          if ba.nil? || !@parent.respond_to?(ba) || !@parent.send(ba, *records).is_a?(FalseClass)
            unless @inverse.nil?
              records.each do |record|
                @associated_class.send(:load, record.id).send("#{@inverse}=", @parent)
              end
            end

            new_txn = @backend.begin_transaction
            @backend.add(@record_ids_key, records.map(&:id))
            @backend.commit_transaction if new_txn
            aa = @callbacks[:after_add]
            @parent.send(aa, *records) if !aa.nil? && @parent.respond_to?(aa)
          end
        end
      end

      # TODO support dependent delete, for now just deletes the association
      def delete(*records)
        raise 'No records to delete' if records.empty?
        raise 'Invalid record class' if records.any? {|r| !r.is_a?(@associated_class)}
        raise 'Record(s) must have been saved' unless records.all? {|r| r.persisted?} # may need to be moved
        @parent.class.lock(*@lock_klasses) do
          br = @callbacks[:before_remove]
          if br.nil? || !@parent.respond_to?(br) || !@parent.send(br, *records).is_a?(FalseClass)
            unless @inverse.nil?
              records.each do |record|
                @associated_class.send(:load, record.id).send("#{@inverse}=", nil)
              end
            end

            new_txn = @backend.begin_transaction
            @backend.delete(@record_ids_key, records.map(&:id))
            @backend.commit_transaction if new_txn
            ar = @callbacks[:after_remove]
            @parent.send(ar, *records) if !ar.nil? && @parent.respond_to?(ar)
          end
        end
      end

      private

      # associated will be a belongs_to; on remove already runs inside a lock and transaction
      def on_remove
        unless @inverse.nil?
          self.ids.each do |record_id|
            @associated_class.send(:load, record_id).send("#{@inverse}=", nil)
          end
        end
        @backend.clear(@record_ids_key)
      end

      # creates a new filter class each time it's called, to store the
      # state for this particular filter chain
      def filter
        @backend.filter(@record_ids_key, @associated_class)
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