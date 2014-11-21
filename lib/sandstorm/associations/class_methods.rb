require 'sandstorm/associations/association_data'
require 'sandstorm/associations/index_data'

require 'sandstorm/associations/belongs_to'
require 'sandstorm/associations/has_and_belongs_to_many'
require 'sandstorm/associations/has_many'
require 'sandstorm/associations/has_one'
require 'sandstorm/associations/has_sorted_set'
require 'sandstorm/associations/index'
require 'sandstorm/associations/unique_index'

# NB: this module gets mixed in to Sandstorm::Record as class methods

# TODO update other side of associations without having to load the record (?)
# TODO callbacks on before/after add/delete on association?

module Sandstorm
  module Associations
    module ClassMethods

      protected

      # used by classes including a Sandstorm Record to set up
      # indices and associations
      def index_by(*args)
        att_types = attribute_types
        args.each do |arg|
          index(::Sandstorm::Associations::Index, arg, :type => att_types[arg])
        end
        nil
      end

      def unique_index_by(*args)
        att_types = attribute_types
        args.each do |arg|
          index(::Sandstorm::Associations::UniqueIndex, arg, :type => att_types[arg])
        end
        nil
      end

      def has_many(name, args = {})
        associate(::Sandstorm::Associations::HasMany, name, args)
        nil
      end

      def has_one(name, args = {})
        associate(::Sandstorm::Associations::HasOne, name, args)
        nil
      end

      def has_sorted_set(name, args = {})
        associate(::Sandstorm::Associations::HasSortedSet, name, args)
        nil
      end

      def has_and_belongs_to_many(name, args = {})
        associate(::Sandstorm::Associations::HasAndBelongsToMany, name, args)
        nil
      end

      def belongs_to(name, args = {})
        associate(::Sandstorm::Associations::BelongsTo, name, args)
        nil
      end
      # end used by client classes

      # used internally by other parts of Sandstorm to implement the above
      # configuration

      # Works out which classes should be locked when updating associations
      def associated_classes(visited = [], cascade = true)
        visited |= [self]
        return visited unless cascade
        @lock.synchronize do
          @association_data ||= {}
          @association_data.values.each do |data|
            klass = data.data_klass
            next if visited.include?(klass)
            visited |= klass.associated_classes(visited, false)
          end
        end
        visited
      end

      # TODO for each association: check whether it has changed
      # would need an instance-level hash with association name as key,
      #   boolean 'changed' value
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
      # end used internally within Sandstorm

      # # TODO  can remove need for some of the inverse mapping
      # # was inverse_of(source, klass)
      # with_association_data do |d|
      #   d.detect {|name, data| data.klass == klass && data.inverse == source}
      # end

      private

      def add_index_data(klass, name, args = {})
        return if name.nil?

        data = Sandstorm::Associations::IndexData.new(
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

      def add_association_data(klass, name, args = {})

        # TODO have inverse be a reference (or copy) of the association data
        # record for that inverse association
        inverse = if args[:inverse_of].nil? || args[:inverse_of].to_s.empty?
          nil
        else
          args[:inverse_of].to_s
        end

        data = Sandstorm::Associations::AssociationData.new(
          :name            => name,
          :data_klass_name => args[:class_name],
          :type_klass      => klass,
          :inverse         => inverse,
          :callbacks       => [:before_add, :after_add,
                               :before_remove, :after_remove].each_with_object({}) {|c, memo|
                                memo[c] = args[c]
                              }
        )

        if klass.name == 'Sandstorm::Associations::HasSortedSet'
          data.sort_key = (args[:key] || :id)
        end

        @lock.synchronize do
          @association_data ||= {}
          @association_data[name] = data
        end
      end

      def associate(klass, name, args = {})
        return if name.nil?

        add_association_data(klass, name, args)

        assoc = case klass.name
        when ::Sandstorm::Associations::HasMany.name,
             ::Sandstorm::Associations::HasSortedSet.name,
             ::Sandstorm::Associations::HasAndBelongsToMany.name

          %Q{
            def #{name}
              #{name}_proxy
            end
          }

        when ::Sandstorm::Associations::HasOne.name,
             ::Sandstorm::Associations::BelongsTo.name

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

            @#{name}_proxy ||= #{klass.name}.new(self, '#{name}')
          end
          private :#{name}_proxy
        }
        class_eval proxy, __FILE__, __LINE__
        class_eval assoc, __FILE__, __LINE__
      end
    end
  end
end