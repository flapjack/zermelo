require 'sandstorm/associations/belongs_to'
require 'sandstorm/associations/has_and_belongs_to_many'
require 'sandstorm/associations/has_many'
require 'sandstorm/associations/has_one'
require 'sandstorm/associations/has_sorted_set'
require 'sandstorm/associations/index'
require 'sandstorm/associations/unique_index'

# TODO update other side of associations without having to load the record, so
# that it can happen inside the multi/exec block

# TODO redis-level locking around deleting keys if, e.g. value or set empty

module Sandstorm

  module Associations

    protected

    def indexed_attributes
      ret = nil
      @lock.synchronize do
        @indexes ||= []
        ret = @indexes.dup
      end
      ret
    end

    def inverse_of(source)
      ret = nil
      @lock.synchronize do
        @inverses ||= {}
        ret = @inverses[source.to_sym]
      end
      ret
    end

    def remove_from_associated(record)
      @lock.synchronize do
        @associations.each do |name|
          record.send("#{name}_proxy".to_sym).send(:on_remove)
        end
      end
    end

    # NB: key must be a string or boolean type, TODO validate this
    def index_by(*args)
      args.each do |arg|
        index = associate(::Sandstorm::Associations::Index, self, arg)
        @lock.synchronize do
          @indexes ||= []
          @indexes << arg.to_s
        end
      end
      nil
    end

    def unique_index_by(*args)
      args.each do |arg|
        index = associate(::Sandstorm::Associations::UniqueIndex, self, arg)
        @lock.synchronize do
          @indexes ||= []
          @indexes << arg.to_s
        end
      end
      nil
    end

    def has_many(name, args = {})
      associate(::Sandstorm::Associations::HasMany, self, name, args)
      @lock.synchronize do
        @associations ||= []
        @associations << name
      end
      nil
    end

    def has_one(name, args = {})
      associate(::Sandstorm::Associations::HasOne, self, name, args)
      @lock.synchronize do
        @associations ||= []
        @associations << name
      end
      nil
    end

    def has_sorted_set(name, args = {})
      associate(::Sandstorm::Associations::HasSortedSet, self, name, args)
      @lock.synchronize do
        @associations ||= []
        @associations << name
      end
      nil
    end

    def has_and_belongs_to_many(name, args = {})
      associate(::Sandstorm::Associations::HasAndBelongsToMany, self, name, args)
      @lock.synchronize do
        @associations ||= []
        @associations << name
      end
      nil
    end

    def belongs_to(name, args = {})
      associate(::Sandstorm::Associations::BelongsTo, self, name, args)
      @lock.synchronize do
        @associations ||= []
        @associations << name
        @inverses ||= {}
        @inverses[args[:inverse_of]] = name
      end
      nil
    end

    private

    def associate(klass, parent, name, args = {})
      assoc = nil
      case klass.name
      when ::Sandstorm::Associations::Index.name, ::Sandstorm::Associations::UniqueIndex.name

        # TODO check method_defined? ( which relative to instance_eval ?)

        unless name.nil?
          assoc = %Q{
            private

            def #{name}_index(value)
              @#{name}_index ||=
                #{klass.name}.new(self, "#{class_key}", "#{name}")
              @#{name}_index.value = value
              @#{name}_index
            end
          }
          instance_eval assoc, __FILE__, __LINE__
        end

      when ::Sandstorm::Associations::HasMany.name, ::Sandstorm::Associations::HasSortedSet.name,
        ::Sandstorm::Associations::HasAndBelongsToMany.name

        assoc_args = []

        if args[:class_name]
          assoc_args << %Q{:class_name => "#{args[:class_name]}"}
        end

        if (klass == ::Sandstorm::Associations::HasSortedSet) && args[:key]
          assoc_args << %Q{:key => "#{(args[:key] || :id).to_s}"}
        end

        if (klass == ::Sandstorm::Associations::HasAndBelongsToMany) && args[:inverse_of]
          assoc_args << %Q{:inverse_of => :#{args[:inverse_of].to_s}}
        end

        # TODO check method_defined? ( which relative to class_eval ? )

        unless name.nil?
          assoc = %Q{
            def #{name}
              #{name}_proxy
            end

            def #{name}_ids
              #{name}_proxy.ids
            end

            private

            def #{name}_proxy
              @#{name}_proxy ||=
                #{klass.name}.new(self, "#{name}", #{assoc_args.join(', ')})
            end
          }
          class_eval assoc, __FILE__, __LINE__
        end

      when ::Sandstorm::Associations::HasOne.name
        assoc_args = []

        if args[:class_name]
          assoc_args << %Q{:class_name => "#{args[:class_name]}"}
        end

        # TODO check method_defined? ( which relative to class_eval ? )

        unless name.nil?
          assoc = %Q{
            def #{name}
              #{name}_proxy.value
            end

            def #{name}=(obj)
              if obj.nil?
                #{name}_proxy.delete(obj)
              else
                #{name}_proxy.add(obj)
              end
            end

            private

            def #{name}_proxy
              @#{name}_proxy ||= #{klass.name}.new(self, "#{name}", #{assoc_args.join(', ')})
            end
          }
          class_eval assoc, __FILE__, __LINE__
        end

      when ::Sandstorm::Associations::BelongsTo.name
        assoc_args = []

        if args[:class_name]
          assoc_args << %Q{:class_name => "#{args[:class_name]}"}
        end

        if args[:inverse_of]
          assoc_args << %Q{:inverse_of => :#{args[:inverse_of].to_s}}
        end

        # TODO check method_defined? ( which relative to class_eval ? )

        unless name.nil?
          assoc = %Q{
            def #{name}
              #{name}_proxy.value
            end

            def #{name}=(obj)
              #{name}_proxy.value = obj
            end

            private

            def #{name}_proxy
              @#{name}_proxy ||= #{klass.name}.new(self, "#{name}", #{assoc_args.join(', ')})
            end
          }
          class_eval assoc, __FILE__, __LINE__
        end

      end

    end

  end

end