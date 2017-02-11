require 'zermelo/records/key'

module Zermelo
  module Records
    # module renamed to avoid ActiveSupport::Concern deprecation warning
    module InstMethods # rubocop:disable Metrics/ModuleLength
      def initialize(attrs = {})
        @is_new = true
        @attributes = self.class.attribute_types.keys.each_with_object({}) do |ak, memo|
          memo[ak.to_s] = attrs[ak]
        end
      end

      def persisted?
        !@is_new
      end

      def load(id)
        self.id = id
        refresh
      end

      def refresh # rubocop:disable Metrics/AbcSize,Metrics/CyclomaticComplexity,Metrics/MethodLength,Metrics/PerceivedComplexity
        # AM::Dirty -- private method 'clear_changes_information' in 4.2.0+,
        # private method 'reset_changes' in 4.1.0+, internal state before that
        if respond_to?(:clear_changes_information, true)
          clear_changes_information
        elsif respond_to?(:reset_changes, true)
          reset_changes
        else
          @previously_changed.clear unless @previously_changed.nil?
          @changed_attributes.clear
        end

        attr_types = self.class.attribute_types

        @attributes = { 'id' => id }

        attrs = nil

        self.class.lock do
          class_key = self.class.send(:class_key)

          # TODO: check for record existence in backend-agnostic fashion
          # TODO fail if id not found
          @is_new = false

          attr_types = self.class.attribute_types.reject { |k, _v| k.eql?(:id) }

          attrs_to_load = attr_types.collect do |attr_name, type|
            Zermelo::Records::Key.new(
              klass: self.class,
              id: id,
              name: attr_name,
              type: type,
              object: :attribute
            )
          end

          result = backend.get_multiple(*attrs_to_load)
          attrs = result[class_key][id] unless result.empty?
        end

        @attributes.update(attrs) unless attrs.nil? || attrs.empty?
        true
      end

      # TODO: limit to only those attribute names defined in define_attributes
      def update_attributes(attributes = {})
        attributes.each_pair do |att, v|
          unless value == @attributes[att.to_s]
            @attributes[att.to_s] = v
            send("#{att}_will_change!")
          end
        end
        save
      end

      def save! # rubocop:disable Metrics/AbcSize,Metrics/CyclomaticComplexity,Metrics/MethodLength,Metrics/PerceivedComplexity
        return unless @is_new || changed?
        self.id ||= self.class.generate_id
        raise(Zermelo::Records::Errors::RecordInvalid.new(self), 'Invalid record') unless valid?

        creating = !persisted?
        saved = false

        sort_val = nil
        case self
        when Zermelo::Records::Ordered
          sort_attr = self.class.instance_variable_get('@sort_attribute')
          raise 'Ordered record types must define_sort_attribute' if sort_attr.nil?
          sort_val = @attributes[sort_attr.to_s]
          raise "Value required for sort_attribute #{sort_attr}" if sort_val.nil?
        end

        run_callbacks(creating ? :create : :update) do # rubocop:disable Metrics/BlockLength
          idx_attrs = self.class.send(:with_index_data) do |d|
            d.each_with_object({}) do |(name, data), memo|
              memo[name.to_s] = data.index_klass
            end
          end

          self.class.transaction do # rubocop:disable Metrics/BlockLength
            apply_attribute = proc do |att, attr_key, old_new|
              backend.set(attr_key, old_new.last) unless att.eql?('id')

              if idx_attrs.key?(att)
                # update indices
                if creating
                  self.class.send("#{att}_index").
                    add_id(@attributes['id'], old_new.last)
                else
                  self.class.send("#{att}_index").
                    move_id(@attributes['id'], old_new.first, self.class.send("#{att}_index"), old_new.last)
                end
              end
            end

            attr_keys = attribute_keys

            if creating
              attr_keys.each_pair do |att, attr_key|
                apply_attribute.call(att, attr_key, [nil, @attributes[att]])
              end
            else
              changes.each_pair do |att, old_new|
                apply_attribute.call(att, attr_keys[att], old_new)
              end
            end

            # ids is a set/sorted set, so update won't create duplicates
            # NB influxdb backend doesn't need this

            # FIXME: distinguish between this in the class methods?
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

        unless saved
          raise(Zermelo::Records::Errors::RecordNotSaved.new(self), 'Record not saved')
        end

        # AM::Dirty -- private method in 4.1.0+, internal state before that
        if respond_to?(:changes_applied, true)
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

      def destroy # rubocop:disable Metrics/AbcSize,Metrics/MethodLength
        raise 'Record was not persisted' unless persisted?

        run_callbacks :destroy do # rubocop:disable Metrics/BlockLength
          assoc_classes = self.class.send(:associated_classes)
          index_attrs   = self.class.send(:with_index_data, &:keys)

          self.class.lock(*assoc_classes) do
            self.class.send(:with_associations, self) do |assoc|
              assoc.send(:on_remove)
            end

            self.class.transaction do
              self.class.delete_id(@attributes['id'])
              index_attrs.each do |att|
                idx = self.class.send("#{att}_index")
                idx.delete_id(@attributes['id'], @attributes[att.to_s])
              end

              self.class.attribute_types.each_pair do |attr_name, type|
                key = Zermelo::Records::Key.new(
                  klass: self.class,
                  id: self.id,
                  name: attr_name.to_s,
                  type: type,
                  object: :attribute
                )
                backend.clear(key)
              end

              record_key = Zermelo::Records::Key.new(
                klass: self.class,
                id: self.id
              )
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

        def attribute_keys # rubocop:disable Metrics/MethodLength
          @attribute_keys ||= self.class.attribute_types.each_with_object({}) do |(attr_name, type), memo|
            next if attr_name.eql?(:id)
            attr_name_str = attr_name.to_s
            memo[attr_name_str] =
              Zermelo::Records::Key.new(
                klass: self.class,
                id: self.id,
                name: attr_name_str,
                type: type,
                object: :attribute
              )
          end
        end

        # http://stackoverflow.com/questions/7613574/activemodel-fields-not-mapped-to-accessors
        #
        # Simulate attribute writers from method_missing
        # FIXME: replace with methods from 'activemodel_experiments' branch
        def attribute=(att, value) # rubocop:disable Metrics/AbcSize,Metrics/MethodLength
          return if value == @attributes[att.to_s]
          if att.to_s == 'id'
            raise 'Cannot reassign id' unless @attributes['id'].nil?
            send('id_will_change!')
            @attributes['id'] = value.to_s
            return
          end

          send("#{att}_will_change!")
          if self.class.attribute_types[att.to_sym].eql?(:set) && !value.is_a?(Set)
            @attributes[att.to_s] = Set.new(value)
            return
          end

          @attributes[att.to_s] = value
        end

        # Simulate attribute readers from method_missing
        def attribute(att)
          value = @attributes[att.to_s]
          return value unless self.class.attribute_types[att.to_sym].eql?(:timestamp)
          value.is_a?(Integer) ? Time.at(value) : value
        end

        # Used by ActiveModel to lookup attributes during validations.
        def read_attribute_for_validation(att)
          @attributes[att.to_s]
        end
    end
  end
end
