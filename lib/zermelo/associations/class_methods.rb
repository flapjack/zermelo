require 'zermelo/associations/association_data'
require 'zermelo/associations/index_data'

require 'zermelo/associations/singular'
require 'zermelo/associations/multiple'
require 'zermelo/associations/index'
require 'zermelo/associations/range_index'
require 'zermelo/associations/unique_index'

# NB: this module gets mixed in to Zermelo::Record as class methods

# TODO update other side of associations without having to load the record (?)
# TODO callbacks on before/after add/delete on association?

module Zermelo
  module Associations
    module ClassMethods

      protected

      # used by classes including a Zermelo Record to set up
      # indices and associations
      def index_by(*args)
        att_types = attribute_types
        args.each do |arg|
          index(::Zermelo::Associations::Index, arg, :type => att_types[arg])
        end
        nil
      end

      def range_index_by(*args)
        att_types = attribute_types
        args.each do |arg|
          index(::Zermelo::Associations::RangeIndex, arg, :type => att_types[arg])
        end
        nil
      end

      def unique_index_by(*args)
        att_types = attribute_types
        args.each do |arg|
          index(::Zermelo::Associations::UniqueIndex, arg, :type => att_types[arg])
        end
        nil
      end

      def has_many(name, args = {})
        associate(::Zermelo::Associations::Multiple, :has_many, name, args)
        nil
      end

      def has_sorted_set(name, args = {})
        associate(::Zermelo::Associations::Multiple, :has_sorted_set, name, args)
        nil
      end

      def has_and_belongs_to_many(name, args = {})
        associate(::Zermelo::Associations::Multiple, :has_and_belongs_to_many, name, args)
        nil
      end

      def has_one(name, args = {})
        associate(::Zermelo::Associations::Singular, :has_one, name, args)
        nil
      end

      def belongs_to(name, args = {})
        associate(::Zermelo::Associations::Singular, :belongs_to, name, args)
        nil
      end
      # end used by client classes

      private

      # used internally by other parts of Zermelo to implement the above
      # configuration

      # Works out which classes should be locked when updating associations
      # TODO work out if this can be replaced by 'related_klasses' assoc data
      def associated_classes(visited = [], cascade = true)
        visited |= [self]
        return visited unless cascade
        @lock.synchronize do
          @association_data ||= {}
          @association_data.values.each do |data|
            klass = data.data_klass
            next if visited.include?(klass)
            visited |= klass.send(:associated_classes, visited, false)
          end
        end
        visited
      end

      def with_associations(record)
        @lock.synchronize do
          @association_data ||= {}
          @association_data.keys.each do |name|
            yield record.send("#{name}_proxy".to_sym)
          end
        end
      end

      def with_association_data(name = nil)
        @lock.synchronize do
          @association_data ||= {}
          assoc_data = name.nil? ? @association_data : @association_data[name]
          yield assoc_data unless assoc_data.nil?
        end
      end

      def with_index_data(name = nil)
        @lock.synchronize do
          @index_data ||= {}
          idx_data = name.nil? ? @index_data : @index_data[name]
          yield idx_data unless idx_data.nil?
        end
      end
      # end used internally within Zermelo

      def add_index_data(klass, name, args = {})
        return if name.nil?

        data = Zermelo::Associations::IndexData.new(
          :name            => name,
          :type            => args[:type],
          :index_klass     => klass
        )

        @lock.synchronize do
          @index_data ||= {}
          @index_data[name] = data
        end
      end

      def index(klass, name, args = {})
        return if name.nil?

        add_index_data(klass, name, args)

        idx = %Q{
          private

          def #{name}_index
            @#{name}_index ||= #{klass.name}.new(self, '#{name}')
            @#{name}_index
          end
        }
        instance_eval idx, __FILE__, __LINE__
      end

      def add_association_data(klass, type, name, args = {})
        inverse = if args[:inverse_of].nil? || args[:inverse_of].to_s.empty?
          nil
        else
          args[:inverse_of].to_s
        end

        callbacks = case klass.name
        when ::Zermelo::Associations::Multiple.name
          [:before_add, :after_add, :before_remove, :after_remove, :before_read, :after_read]
        when ::Zermelo::Associations::Singular.name
          [:before_set, :after_set, :before_clear, :after_clear, :before_read, :after_read]
        else
          []
        end

        data = Zermelo::Associations::AssociationData.new(
          :name                => name,
          :data_klass_name     => args[:class_name],
          :data_type           => type,
          :type_klass          => klass,
          :inverse             => inverse,
          :related_klass_names => args[:related_class_names],
          :callbacks           => callbacks.each_with_object({}) {|c, memo|
                                    memo[c] = args[c]
                                  }
        )

        if :has_sorted_set.eql?(type)
          data.sort_key   = args[:key]
          data.sort_order =
            !args[:order].nil? && :desc.eql?(args[:order].to_sym) ? :desc : :asc
        end

        @lock.synchronize do
          @association_data ||= {}
          @association_data[name] = data
        end
      end

      def associate(klass, type, name, args = {})
        return if name.nil?

        add_association_data(klass, type, name, args)

        assoc = case klass.name
        when ::Zermelo::Associations::Multiple.name
          %Q{
            def #{name}
              #{name}_proxy
            end
          }

        when ::Zermelo::Associations::Singular.name
          %Q{
            def #{name}
              #{name}_proxy.value
            end

            def #{name}=(obj)
              #{name}_proxy.value = obj
            end
          }
        end

        return if assoc.nil?

        proxy = %Q{
          def #{name}_proxy
            raise "Associations cannot be invoked for records without an id" if self.id.nil?

            @#{name}_proxy ||= #{klass.name}.new(:#{type}, self.class, self.id, '#{name}')
          end
          private :#{name}_proxy
        }
        class_eval proxy, __FILE__, __LINE__
        class_eval assoc, __FILE__, __LINE__
      end
    end
  end
end