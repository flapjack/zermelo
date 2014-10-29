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
        # AM::Dirty -- private method in 4.1.0+, internal state before that
        if self.respond_to?(:reset_changes, true)
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
          @is_new = false

          attr_types = self.class.attribute_types.reject {|k, v| k == :id}

          attrs_to_load = attr_types.collect do |name, type|
            Sandstorm::Records::Key.new(:klass => class_key,
              :id => self.id, :name => name, :type => type, :object => :attribute)
          end

          attrs = backend.get_multiple(*attrs_to_load)[class_key][self.id]
        end

        # TODO:? return false unless record_exists

        @attributes.update(attrs) if attrs.present?

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

      def save
        return unless @is_new || self.changed?
        return false unless valid?

        creating = !self.persisted?

        run_callbacks( (creating ? :create : :update) ) do

          self.id ||= self.class.generate_id

          idx_attrs = self.class.send(:with_index_data) do |d|
            idx_attrs = d.each_with_object({}) do |(name, data), memo|
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
              attribute_keys.each_pair do |att, attr_key|
                apply_attribute.call(att, attr_key, [nil, @attributes[att]])
              end
            else
              self.changes.each_pair do |att, old_new|
                apply_attribute.call(att, attr_keys[att], old_new)
              end
            end

            # ids is a set, so update won't create duplicates
            # NB influxdb backend doesn't need this
            self.class.add_id(@attributes['id'])
          end

          @is_new = false
        end

        # AM::Dirty -- private method in 4.1.0+, internal state before that
        if self.respond_to?(:changes_applied, true)
          changes_applied
        else
          @previously_changed = changes
          @changed_attributes.clear
        end

        true
      end

      def destroy
        raise "Record was not persisted" if !persisted?

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
                key = Sandstorm::Records::Key.new(:klass => self.class.send(:class_key),
                  :id => self.id, :name => name.to_s, :type => type, :object => :attribute)
                backend.clear(key)
              }

              record_key = Sandstorm::Records::Key.new(:klass => self.class.send(:class_key),
                  :id => self.id)
              backend.purge(record_key)
            end

          end

        end
      end

      private

      def backend
        self.class.send(:backend)
      end

      def attribute_keys
        @attribute_keys ||= self.class.attribute_types.reject {|k, v|
          k == :id
        }.inject({}) {|memo, (name, type)|
          memo[name.to_s] = Sandstorm::Records::Key.new(:klass => self.class.send(:class_key),
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