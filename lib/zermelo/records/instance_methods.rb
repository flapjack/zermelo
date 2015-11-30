require 'zermelo/records/key'

module Zermelo

  module Records

    # module renamed to avoid ActiveSupport::Concern deprecation warning
    module InstMethods

      def initialize(attrs = {})
        @is_new = true
        @attributes = self.class.attribute_types.keys.inject({}) do |memo, ak|
          memo[ak.to_s] = attrs[ak]
          memo
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
        # AM::Dirty -- private method 'clear_changes_information' in 4.2.0+,
        # private method 'reset_changes' in 4.1.0+, internal state before that
        if self.respond_to?(:clear_changes_information, true)
          clear_changes_information
        elsif self.respond_to?(:reset_changes, true)
          reset_changes
        else
          @previously_changed.clear unless @previously_changed.nil?
          @changed_attributes.clear
        end

        attr_types = self.class.attribute_types

        @attributes = {'id' => self.id}

        attrs = nil

        self.class.lock do
          class_key = self.class.send(:class_key)

          # TODO: check for record existence in backend-agnostic fashion
          # TODO fail if id not found
          @is_new = false

          attr_types = self.class.attribute_types.reject {|k, v| k == :id}

          attrs_to_load = attr_types.collect do |name, type|
            Zermelo::Records::Key.new(:klass => self.class,
              :id => self.id, :name => name, :type => type, :object => :attribute)
          end

          result = backend.get_multiple(*attrs_to_load)
          attrs = result[class_key][self.id] unless result.empty?
        end

        @attributes.update(attrs) unless attrs.nil? || attrs.empty?
        true
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

      def save!
        return unless @is_new || self.changed?
        self.id ||= self.class.generate_id
        raise Zermelo::Records::Errors::RecordInvalid.new(self) unless valid?

        creating = !self.persisted?
        saved = false

        sort_val = nil
        case self
        when Zermelo::Records::Ordered
          sort_attr = self.class.instance_variable_get('@sort_attribute')
          raise 'Ordered record types must define_sort_attribute' if sort_attr.nil?
          sort_val = @attributes[sort_attr.to_s]
          raise "Value required for sort_attribute #{sort_attr}" if sort_val.nil?
        end

        run_callbacks( (creating ? :create : :update) ) do

          idx_attrs = self.class.send(:with_index_data) do |d|
            d.each_with_object({}) do |(name, data), memo|
              memo[name.to_s] = data.index_klass
            end
          end

          self.class.transaction do

            apply_attribute = proc {|att, attr_key, old_new|
              backend.set(attr_key, old_new.last) unless att.eql?('id')

              if idx_attrs.has_key?(att)
                # update indices
                if creating
                  self.class.send("#{att}_index").add_id( @attributes['id'], old_new.last)
                else
                  self.class.send("#{att}_index").move_id( @attributes['id'], old_new.first,
                                  self.class.send("#{att}_index"), old_new.last)
                end
              end
            }

            attr_keys = attribute_keys

            if creating
              attr_keys.each_pair do |att, attr_key|
                apply_attribute.call(att, attr_key, [nil, @attributes[att]])
              end
            else
              self.changes.each_pair do |att, old_new|
                apply_attribute.call(att, attr_keys[att], old_new)
              end
            end

            # ids is a set/sorted set, so update won't create duplicates
            # NB influxdb backend doesn't need this

            # FIXME distinguish between this in the class methods?
            case self
            when Zermelo::Records::Ordered
              self.class.add_id(@attributes['id'], sort_val)
            when Zermelo::Records::Unordered
              self.class.add_id(@attributes['id'])
            end
          end

          @is_new = false
          saved = true
        end

        raise Zermelo::Records::Errors::RecordNotSaved.new(self) unless saved

        # AM::Dirty -- private method in 4.1.0+, internal state before that
        if self.respond_to?(:changes_applied, true)
          changes_applied
        else
          @previously_changed = changes
          @changed_attributes.clear
        end

        true
      end

      def save
        save!
      rescue Zermelo::Records::Errors::RecordInvalid, Zermelo::Records::Errors::RecordNotSaved
        false
      end

      def destroy
        raise "Record was not persisted" unless persisted?

        run_callbacks :destroy do

          assoc_classes = self.class.send(:associated_classes)
          index_attrs   = self.class.send(:with_index_data) {|d| d.keys }

          self.class.lock(*assoc_classes) do
            self.class.send(:with_associations, self) do |assoc|
              assoc.send(:on_remove)
            end

            self.class.transaction do
              self.class.delete_id(@attributes['id'])
              index_attrs.each do |att|
                idx = self.class.send("#{att}_index")
                idx.delete_id( @attributes['id'], @attributes[att.to_s])
              end

              self.class.attribute_types.each_pair {|name, type|
                key = Zermelo::Records::Key.new(:klass => self.class,
                  :id => self.id, :name => name.to_s, :type => type, :object => :attribute)
                backend.clear(key)
              }

              record_key = Zermelo::Records::Key.new(:klass => self.class,
                  :id => self.id)
              backend.purge(record_key)
            end
          end
        end
      end

      def key_dump
        inst_keys = []

        attr_key = attribute_keys.values.first
        unless attr_key.nil?
          inst_keys += [backend.key_to_backend_key(attr_key), attr_key]
        end

        self.class.send(:with_associations, self) do |assoc|
          inst_keys += assoc.key_dump
        end

        Hash[*inst_keys]
      end

      private

      def backend
        self.class.send(:backend)
      end

      def attribute_keys
        @attribute_keys ||= self.class.attribute_types.reject {|k, v|
          k == :id
        }.inject({}) {|memo, (name, type)|
          memo[name.to_s] = Zermelo::Records::Key.new(:klass => self.class,
            :id => self.id, :name => name.to_s, :type => type, :object => :attribute)
          memo
        }
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