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

        attrs = nil

        self.class.send(:lock) do
          class_key = self.class.send(:class_key)

          # TODO check for record existence in backend-agnostic fashion
          @is_new = false

          attr_types = self.class.attribute_types.reject {|k, v| k == :id}

          attrs_to_load = attr_types.collect do |name, type|
            Sandstorm::Records::Key.new(:class => class_key,
              :id => self.id, :name => name, :type => type)
          end

          attrs = backend.get(*attrs_to_load)[class_key][self.id]
        end

        # return false unless record_exists

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

          self.id ||= SecureRandom.hex(16)

          idx_attrs = self.class.send(:indexed_attributes)

          self.class.transaction do
            self.class.attribute_types.reject {|k, v|
              k == :id
            }.each_pair do |name, type|
              attr_key = Sandstorm::Records::Key.new(:class => self.class.send(:class_key),
                :id => self.id, :name => name, :type => type)

              value = @attributes[name.to_s]
              value.nil? ? backend.clear(attr_key) : backend.set(attr_key, value)
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
        raise "Record was not persisted" if !persisted?

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

              self.class.attribute_types.each_pair {|name, type|
                key = Sandstorm::Records::Key.new(:class => self.class.send(:class_key),
                  :id => self.id, :name => name.to_s, :type => type)
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