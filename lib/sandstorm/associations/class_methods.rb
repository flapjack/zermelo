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

      # Returns a hash of indexed attribute names (as symbols) and their
      # index class types. Used in queries, saving and deletion. Duplicated
      # while synchronized as it's class-level data and direct access to it
      # wouldn't be thread-safe otherwise.
      def indexed_attributes
        ret = nil
        @lock.synchronize do
          @indices ||= {}
          ret = @indices.dup
        end
        ret
      end

      # Find the inverse mapping for a belongs_to association within the class
      # this module is mixed into.
      def inverse_of(source, klass)
        ret = nil
        @lock.synchronize do
          @inverses ||= {}
          ret = @inverses["#{klass.name.demodulize.underscore}_#{source.to_s}"]
        end
        ret
      end

      # Works out which classes should be locked when updating associations
      def associated_classes(visited = [], cascade = true)
        visited |= [self]
        return visited unless cascade
        @association_klasses.each_pair do |assoc, klass_name|
          klass = klass_name.constantize # not optimal
          next if visited.include?(klass)
          visited |= klass.associated_classes(visited, false)
        end
        visited
      end

      # TODO for each association: check whether it has changed
      # would need an instance-level hash with association name as key,
      #   boolean 'changed' value
      def with_associations(record)
        @lock.synchronize do
           @association_klasses.keys.collect do |name|
            yield record.send("#{name}_proxy".to_sym)
          end
        end
      end

      def index_by(*args)
        att_types = attribute_types
        args.each do |arg|
          index = associate(::Sandstorm::Associations::Index, self, arg, :type => att_types[arg])
          @lock.synchronize do
            @indices ||= {}
            @indices[arg.to_s] = ::Sandstorm::Associations::Index
          end
        end
        nil
      end

      def unique_index_by(*args)
        att_types = attribute_types
        args.each do |arg|
          index = associate(::Sandstorm::Associations::UniqueIndex, self, arg, :type => att_types[arg])
          @lock.synchronize do
            @indices ||= {}
            @indices[arg.to_s] = ::Sandstorm::Associations::UniqueIndex
          end
        end
        nil
      end

      def has_many(name, args = {})
        associate(::Sandstorm::Associations::HasMany, self, name, args)
        @lock.synchronize do
          @association_klasses ||= {}
          @association_klasses[name] = args[:class_name]
        end
        nil
      end

      def has_one(name, args = {})
        associate(::Sandstorm::Associations::HasOne, self, name, args)
        @lock.synchronize do
          @association_klasses ||= {}
          @association_klasses[name] = args[:class_name]
        end
        nil
      end

      def has_sorted_set(name, args = {})
        associate(::Sandstorm::Associations::HasSortedSet, self, name, args)
        @lock.synchronize do
          @association_klasses ||= {}
          @association_klasses[name] = args[:class_name]
        end
        nil
      end

      def has_and_belongs_to_many(name, args = {})
        associate(::Sandstorm::Associations::HasAndBelongsToMany, self, name, args)
        @lock.synchronize do
          @association_klasses ||= {}
          @association_klasses[name] = args[:class_name]
        end
        nil
      end

      def belongs_to(name, args = {})
        associate(::Sandstorm::Associations::BelongsTo, self, name, args)
        @lock.synchronize do
          @association_klasses ||= {}
          @association_klasses[name] = args[:class_name]
          @inverses ||= {}
          @inverses["#{args[:class_name].demodulize.underscore}_#{args[:inverse_of]}"] = name
        end
        nil
      end

      private

      def associate(klass, parent, name, args = {})
        return if name.nil?
        case klass.name
        when ::Sandstorm::Associations::Index.name, ::Sandstorm::Associations::UniqueIndex.name
          index = %Q{
            private

            def #{name}_index
              @#{name}_index ||=
                #{klass.name}.new(self, "#{class_key}", "#{name}", :#{args[:type]})
              @#{name}_index
            end
          }
          instance_eval index, __FILE__, __LINE__

        when ::Sandstorm::Associations::HasMany.name,
          ::Sandstorm::Associations::HasSortedSet.name,
          ::Sandstorm::Associations::HasAndBelongsToMany.name

          assoc_args = []

          if args[:class_name]
            assoc_args << %Q{:class_name => "#{args[:class_name]}"}
          end

          if klass == ::Sandstorm::Associations::HasSortedSet
            key_type = ':sorted_set'
            if args[:key]
              assoc_args << %Q{:key => "#{(args[:key] || :id).to_s}"}
            end
          else
            key_type = ':set'
          end

          if (klass == ::Sandstorm::Associations::HasAndBelongsToMany) && args[:inverse_of]
            assoc_args << %Q{:inverse_of => :#{args[:inverse_of].to_s}}
          end

          assoc = %Q{
            def #{name}
              #{name}_proxy
            end

            def #{name}_ids
              #{name}_proxy.ids
            end

            def self.associated_ids_for_#{name}(*this_ids)
              this_ids.inject({}) do |memo, this_id|
                key = Sandstorm::Records::Key.new(
                  :class  => class_key,
                  :id     => this_id,
                  :name   => '#{name}_ids',
                  :type   => #{key_type},
                  :object => :association
                )
                memo[this_id] = backend.get(key)
                memo
              end
            end

            private

            def #{name}_proxy
              raise "Associations cannot be invoked for records without an id" if self.id.nil?

              @association_keys['#{name}'] ||= Sandstorm::Records::Key.new(
                :class  => "#{class_key}",
                :id     => self.id,
                :name   => '#{name}_ids',
                :type   => #{key_type},
                :object => :association
              )

              @#{name}_proxy ||=
                #{klass.name}.new(self, '#{name}', @association_keys['#{name}'], backend,
                  #{assoc_args.join(', ')})
            end
          }
          class_eval assoc, __FILE__, __LINE__

        when ::Sandstorm::Associations::HasOne.name
          assoc_args = []

          if args[:class_name]
            assoc_args << %Q{:class_name => "#{args[:class_name]}"}
          end

          # TODO would be better as a 'has_one' hash

          assoc = %Q{
            def #{name}
              #{name}_proxy.value
            end

            def #{name}=(obj)
              #{name}_proxy.send(obj.nil? ? :delete : :add, obj)
            end

            def self.associated_ids_for_#{name}(*this_ids)
              this_ids.inject({}) do |memo, this_id|
                key = Sandstorm::Records::Key.new(
                  :class  => class_key,
                  :id     => this_id,
                  :name   => '#{name}_id',
                  :type   => :string,
                  :object => :association
                )
                memo[this_id] = backend.get(key)
                memo
              end
            end

            private

            def #{name}_proxy
              raise "Associations cannot be invoked for records without an id" if self.id.nil?

              @association_keys['#{name}'] ||= Sandstorm::Records::Key.new(
                :class  => "#{class_key}",
                :id     => self.id,
                :name   => '#{name}_id',
                :type   => :string,
                :object => :association
              )

              @#{name}_proxy ||= #{klass.name}.new(self, '#{name}',
                @association_keys['#{name}'], backend, #{assoc_args.join(', ')})
            end
          }
          class_eval assoc, __FILE__, __LINE__

        when ::Sandstorm::Associations::BelongsTo.name
          assoc_args = []

          if args[:class_name]
            assoc_args << %Q{:class_name => "#{args[:class_name]}"}
          end

          if args[:inverse_of]
            assoc_args << %Q{:inverse_of => :#{args[:inverse_of].to_s}}
          end

          assoc = %Q{
            def #{name}
              #{name}_proxy.value
            end

            def #{name}=(obj)
              #{name}_proxy.value = obj
            end

            def self.associated_ids_for_#{name}(*this_ids)
              this_ids.inject({}) do |memo, this_id|
                key = Sandstorm::Records::Key.new(
                  :class  => class_key,
                  :id     => this_id,
                  :name   => 'belongs_to',
                  :type   => :hash,
                  :object => :association
                )
                memo[this_id] = backend.get(key)['#{name}_id']
                memo
              end
            end

            private

            def #{name}_proxy
              raise "Associations cannot be invoked for records without an id" if self.id.nil?

              @association_keys['belongs_to'] ||= Sandstorm::Records::Key.new(
                :class  => "#{class_key}",
                :id     => self.id,
                :name   => 'belongs_to',
                :type   => :hash,
                :object => :association
              )

              @#{name}_proxy ||= #{klass.name}.new(self, '#{name}',
                @association_keys['belongs_to'], backend, #{assoc_args.join(', ')})
            end
          }

          class_eval assoc, __FILE__, __LINE__

        end

      end

    end

  end

end