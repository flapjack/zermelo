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

        @association_keys = {}
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

        attrs = nil

        backend.lock(self.class) do
          class_key = self.class.send(:class_key)

          # TODO: check for record existence in backend-agnostic fashion
          @is_new = false

          attr_types = self.class.attribute_types.reject {|k, v| k == :id}

          attrs_to_load = attr_types.collect do |name, type|
            Sandstorm::Records::Key.new(:class => class_key,
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
        return unless self.changed?
        return false unless valid?

        run_callbacks( (self.persisted? ? :update : :create) ) do

          self.id ||= self.class.generate_id

          idx_attrs = self.class.send(:indexed_attributes)

          self.class.transaction do

            self.changes.each_pair do |att, old_new|
              backend.set(attribute_keys[att], old_new.last) unless att.eql?('id')

              next unless idx_attrs.has_key?(att)

              # update indices
              if old_new.first.nil?
                self.class.send("#{att}_index", old_new.last).add_id( @attributes['id'] )
              elsif old_new.last.nil?
                self.class.send("#{att}_index", old_new.first).delete_id( @attributes['id'] )
              else
                self.class.send("#{att}_index", old_new.first).move_id( @attributes['id'],
                                self.class.send("#{att}_index", old_new.last))
              end
            end

            # ids is a set, so update won't create duplicates
            # NB influxdb backend doesn't need this
            self.class.add_id(@attributes['id'])
          end

          @is_new = false
        end

        # AM::Dirty
        @previously_changed = self.changes
        @changed_attributes.clear

        true
      end

      def destroy
        raise "Record was not persisted" if !persisted?

        run_callbacks :destroy do

          assoc_classes = self.class.send(:associated_classes)

          backend.lock(*assoc_classes) do
            self.class.send(:with_associations, self) {|assoc| assoc.send(:on_remove) }
            index_attrs = (self.attributes.keys & self.class.send(:indexed_attributes).keys)

            self.class.transaction do
              self.class.delete_id(@attributes['id'])
              index_attrs.each do |att|
                self.class.send("#{att}_index", @attributes[att]).delete_id( @attributes['id'])
              end

              self.class.attribute_types.each_pair {|name, type|
                key = Sandstorm::Records::Key.new(:class => self.class.send(:class_key),
                  :id => self.id, :name => name.to_s, :type => type, :object => :attribute)
                backend.clear(key)
              }

              record_key = Sandstorm::Records::Key.new(:class => self.class.send(:class_key),
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
          memo[name.to_s] = Sandstorm::Records::Key.new(:class => self.class.send(:class_key),
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