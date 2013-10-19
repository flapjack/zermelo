require 'sandstorm/associations/belongs_to'
require 'sandstorm/associations/has_many'
require 'sandstorm/associations/has_one'
require 'sandstorm/associations/has_sorted_set'
require 'sandstorm/associations/index'

module Sandstorm

  module Associations

    protected

    def indexed_attributes
      (Thread.current[self.object_id.to_s.to_sym] ||= {})[:indexed_attributes] ||= []
    end

    # NB: key must be a string or boolean type, TODO validate this
    def index_by(*args)
      args.each do |arg|
        indexed_attributes << arg.to_s
        associate(::Sandstorm::Associations::Index, self, [arg])
      end
      nil
    end

    def has_many(*args)
      associate(::Sandstorm::Associations::HasMany, self, args)
      nil
    end

    def has_one(*args)
      associate(::Sandstorm::Associations::HasOne, self, args)
      nil
    end

    def has_sorted_set(*args)
      associate(::Sandstorm::Associations::HasSortedSet, self, args)
      nil
    end

    def belongs_to(*args)
      associate(::Sandstorm::Associations::BelongsTo, self, args)
      nil
    end

    private

    # TODO clean up method params, it's a mish-mash
    def associate(klass, parent, args)
      assoc = nil
      case klass.name
      when ::Sandstorm::Associations::Index.name
        name = args.first

        # TODO check method_defined? ( which relative to instance_eval ?)

        unless name.nil?
          assoc = %Q{
            def #{name}_index(value)
              ret = #{name}_proxy_index
              ret.value = value
              ret
            end

            private

            def #{name}_proxy_index
              @#{name}_proxy_index ||=
                #{klass.name}.new(self, "#{class_key}", "#{name}")
            end
          }
          instance_eval assoc, __FILE__, __LINE__
        end

      when ::Sandstorm::Associations::HasMany.name, ::Sandstorm::Associations::HasSortedSet.name
        options = args.extract_options!
        name = args.first.to_s

        p name
        p options
        p klass.name

        assoc_args = []

        if options[:class_name]
          assoc_args << %Q{:class_name => "#{options[:class_name]}"}
        end

        if options[:inverse_of]
          assoc_args << %Q{:inverse_of => :#{options[:inverse_of].to_s}}
        end

        if (klass == ::Sandstorm::Associations::HasSortedSet) && options[:key]
          assoc_args << %Q{:key => "#{(options[:key] || :id).to_s}"}
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

      when ::Sandstorm::Associations::HasOne.name, ::Sandstorm::Associations::BelongsTo.name
        options = args.extract_options!
        name = args.first.to_s

        assoc_args = []

        if options[:class_name]
          assoc_args << %Q{:class_name => "#{options[:class_name]}"}
        end

        if options[:inverse_of]
          assoc_args << %Q{:inverse_of => :#{options[:inverse_of].to_s}}
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