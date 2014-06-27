require 'securerandom'

require 'sandstorm/records/key'

module Sandstorm

  module Records

    # module renamed to avoid ActiveSupport::Concern deprecation warning
    module InstMethods

      def initialize(attributes = {})
        @is_new = true
        @attributes = {}
        attributes.each_pair do |k, v|
          self.send("#{k}=".to_sym, v)
        end
      end

      def persisted?
        !@is_new
      end

      def load(id)
        self.id = id
        refresh
      end

      def refresh
        # resets AM::Dirty changed state
        @previously_changed.clear unless @previously_changed.nil?
        @changed_attributes.clear unless @changed_attributes.nil?

        attr_types = self.class.attribute_types

        @attributes = {'id' => self.id}

        # TODO start lock (this class only)

        # TODO unlock if returning
        return false unless backend.exists?(simple_attributes)

        @is_new = false

        # TODO this is clunky
        simple_attrs = backend.get_all( simple_attributes ).inject({}) do |memo, (name, value)|
          if type = attr_types[name.to_sym]
            memo[name] = case type
            when :string
              value.to_s
            when :integer
              value.to_i
            when :float
              value.to_f
            when :timestamp
              Time.at(value.to_f)
            when :boolean
              value.downcase == 'true'
            end
          end
          memo
        end

        complex_attrs = complex_attributes.inject({}) do |memo, (name, item_key)|
          if type = attr_types[name.to_sym]
          memo[name] = case type
            when :list
              backend.get_all(item_key)
            when :set
              Set.new( backend.get_all(item_key) )
            when :hash
              backend.get_all(item_key)
            end
          end
          memo
        end

        # TODO end lock

        @attributes.update(simple_attrs)
        @attributes.update(complex_attrs)
      end

      # TODO limit to only those attribute names defined in define_attributes
      def update_attributes(attributes = {})
        attributes.each_pair do |att, v|
          unless value == @attributes[att.to_s]
            @attributes[att.to_s] = v
            send("#{att}_will_change!")
          end
        end
        save
      end

      def save
        return unless self.changed?
        return false unless valid?

        run_callbacks( (self.persisted? ? :update : :create) ) do

          self.id ||= SecureRandom.hex(16)

          idx_attrs = self.class.send(:indexed_attributes)

          self.class.transaction do

            simple_attrs = {}
            remove_attrs = []

            attr_types = self.class.attribute_types.reject {|k, v| k == :id}

            attr_types.each_pair do |name, type|
              value = @attributes[name.to_s]
              if value.nil?
                remove_attrs << name.to_s
                next
              end
              case type
              when :string, :integer
                simple_attrs[name.to_s] = value.blank? ? nil : value.to_s
              when :timestamp
                simple_attrs[name.to_s] = value.blank? ? nil : value.to_f
              when :boolean
                simple_attrs[name.to_s] = (!!value).to_s
              when :list, :set, :hash
                item_key = complex_attributes[name.to_s]
                unless value.blank?
                  case attr_types[name.to_sym]
                  when :list
                    backend.clear(item_key)
                    backend.add(item_key, value)
                  when :set
                    backend.clear(item_key)
                    backend.add(item_key, value.to_a)
                  when :hash
                    backend.clear(item_key)
                    values = value.inject([]) do |memo, (k, v)|
                      memo += [k, v]
                    end
                    backend.add(item_key, Hash[*values])
                  end
                end
              end
            end

            # uses hmset
            # TODO check that nil value deletes relevant hash key
            unless remove_attrs.empty?
              backend.delete(simple_attributes, remove_attrs)
            end
            values = simple_attrs.inject([]) do |memo, (k, v)|
              memo += [k, v]
            end
            unless values.empty?
              backend.add(simple_attributes, Hash[*values])
            end

            # update indices
            self.changes.each_pair do |att, old_new|
              next unless idx_attrs.include?(att)

              if old_new.first.nil?
                # sadd
                self.class.send("#{att}_index", old_new.last).add_id( @attributes['id'] )
              elsif old_new.last.nil?
                # srem
                self.class.send("#{att}_index", old_new.first).delete_id( @attributes['id'] )
              else
                # smove
                self.class.send("#{att}_index", old_new.first).move_id( @attributes['id'],
                                self.class.send("#{att}_index", old_new.last))
              end
            end

            # ids is a set, so update won't create duplicates
            # NB influxdb backend doesn't need this
            self.class.add_id(@attributes['id'])
          end
        end

        # AM::Dirty
        @previously_changed = self.changes
        @changed_attributes.clear

        @is_new = false

        true
      end

      def destroy
        run_callbacks :destroy do

          assoc_classes = self.class.send(:associated_classes)

          self.class.send(:lock, *assoc_classes) do
            self.class.send(:remove_from_associated, self)
            index_attrs = (self.attributes.keys & self.class.send(:indexed_attributes))

            self.class.transaction do
              self.class.delete_id(@attributes['id'])
              index_attrs.each {|att|
                self.class.send("#{att}_index", @attributes[att]).delete_id( @attributes['id'])
              }

              backend.clear(simple_attributes)

              complex_attr_types = self.class.attribute_types.select {|k, v|
                [:list, :set, :hash].include?(v)
              }

              complex_attr_types.each_pair do |name, type|
                item_key = complex_attributes[name.to_s]
                case attr_types[name.to_sym]
                when :list
                  backend.list_clear(item_key)
                when :set
                  backend.set_clear(item_key)
                when :hash
                  backend.hash_clear(item_key)
                end
              end

            end

          end

        end
      end

      private

      def backend
        self.class.send(:backend)
      end

      def simple_attributes
        @simple_attributes ||= Sandstorm::Records::Key.new(:class => self.class.send(:class_key),
          :id => self.id, :name => 'attrs', :type => :hash)
      end

      def complex_attributes
        @complex_attributes ||= self.class.attribute_types.inject({}) do |memo, (name, type)|
          if Sandstorm::COLLECTION_TYPES.has_key?(type)
            memo[name.to_s] = Sandstorm::Records::Key.new(:class => self.class.send(:class_key),
              :id => self.id, :name => "attrs:#{name}", :type => type)
          end
          memo
        end
      end

      # http://stackoverflow.com/questions/7613574/activemodel-fields-not-mapped-to-accessors
      #
      # Simulate attribute writers from method_missing
      def attribute=(att, value)
        return if value == @attributes[att.to_s]
        if att.to_s == 'id'
          raise "Cannot reassign id" unless @attributes['id'].nil?
          send("id_will_change!")
          @attributes['id'] = value.to_s
        else
          send("#{att}_will_change!")
          if (self.class.attribute_types[att.to_sym] == :set) && !value.is_a?(Set)
            @attributes[att.to_s] = Set.new(value)
          else
            @attributes[att.to_s] = value
          end
        end
      end

      # Simulate attribute readers from method_missing
      def attribute(att)
        value = @attributes[att.to_s]
        return value unless (self.class.attribute_types[att.to_sym] == :timestamp)
        value.is_a?(Integer) ? Time.at(value) : value
      end

      # Used by ActiveModel to lookup attributes during validations.
      def read_attribute_for_validation(att)
        @attributes[att.to_s]
      end

    end

  end

end