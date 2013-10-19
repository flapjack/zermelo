require 'active_support/concern'
require 'active_support/core_ext/object/blank'
require 'active_support/inflector'
require 'active_model'

require 'sandstorm/associations'
require 'sandstorm/filter'
require 'sandstorm/redis_key'
require 'sandstorm/type_validator'

# TODO: cleanup redis-objects usage from associations, main class and folder
# TODO: start porting tests over
# TODO: class base name only

# TODO escape ids and index_keys -- shouldn't allow bare : or space

# TODO callbacks on before/after add/delete on association?

# TODO optional sort via Redis SORT, first/last for has_many via those

# TODO clean up the multiple condition case for filters, current union(A | B) case
# should work as intersect(A | B) with the union applied first

# TODO get DIFF working for exclusion case against ZSETs

module Sandstorm

  module Record

    extend ActiveSupport::Concern

    included do
      include ActiveModel::AttributeMethods
      extend ActiveModel::Callbacks
      include ActiveModel::Dirty
      include ActiveModel::Validations

      # # including classes can do this
      # include ActiveModel::Serializers::JSON
      # self.include_root_in_json = false

      extend Sandstorm::Associations

      attr_accessor :attributes

      define_model_callbacks :create, :update, :destroy

      attribute_method_suffix  "="  # attr_writers
      # attribute_method_suffix  ""   # attr_readers # DEPRECATED

      validates_with Sandstorm::TypeValidator

      define_attributes :id => :string
    end

    module ClassMethods

      def count
        Sandstorm.redis.card(ids.key)
      end

      def ids
        Sandstorm.redis.smembers(ids.key)
      end

      def add_id(id)
        Sandstorm.redis.sadd(ids.key, id.to_s)
      end

      def delete_id(id)
        Sandstorm.redis.srem(ids.key, id.to_s)
      end

      def exists?(id)
        Sandstorm.redis.sismember(ids.key, id.to_s)
      end

      def all
        ids.collect {|id| load(id) }
      end

      def delete_all
        ids.each {|id|
          next unless record = load(id)
          record.destroy
        }
      end

      def intersect(opts = {})
        Sandstorm::Filter.new(ids, self).intersect(opts)
      end

      def union(opts = {})
        Sandstorm::Filter.new(ids, self).union(opts)
      end

      def diff(opts = {})
        Sandstorm::Filter.new(ids, self).diff(opts)
      end

      def find_by_id(id)
        return unless id && exists?(id.to_s)
        load(id.to_s)
      end

      def attribute_types
        (Thread.current[self.object_id.to_s.to_sym] ||= {})[:attribute_types] ||= {}
      end

      protected

      def define_attributes(options = {})
        options.each_pair do |key, value|
          raise "Unknown attribute type ':#{value}' for ':#{key}'" unless
            Sandstorm::ALL_TYPES.include?(value)
          self.define_attribute_methods([key])
        end
        attribute_types.update(options)
      end

      private

      def ids
        (Thread.current[self.object_id.to_s.to_sym] ||= {})[:ids] ||=
          Sandstorm::RedisKey.new("#{class_key}::ids", :set)
      end

      def class_key
        self.name.demodulize.underscore
      end

      def load(id)
        object = self.new
        object.load(id)
        object
      end

    end

    def initialize(attributes = {})
      @attributes = {}
      attributes.each_pair do |k, v|
        self.send("#{k}=".to_sym, v)
      end
    end

    def persisted?
      !@attributes['id'].nil? && self.class.exists?(@attributes['id'])
    end

    def load(id)
      self.id = id
      refresh
    end

    def record_key
      "#{self.class.send(:class_key)}:#{self.id}"
    end

    def refresh
      # resets AM::Dirty changed state
      @previously_changed.clear unless @previously_changed.nil?
      @changed_attributes.clear unless @changed_attributes.nil?

      attr_types = self.class.attribute_types

      @attributes = {'id' => self.id}

      # TODO this is clunky
      simple_attrs = Sandstorm.redis.hgetall( @simple_attributes.key ).inject({}) do |memo, (name, value)|
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

      complex_attrs = @complex_attributes.inject({}) do |memo, (name, redis_key)|
        if type = attr_types[name.to_sym]
        memo[name] = case type
          when :list
            Sandstorm.redis.lrange(redis_key.key, 0, -1)
          when :set
            Set.new(Sandstorm.redis.smembers(redis_key.key))
          when :hash
            Sandstorm.redis.hgetall(redis_key.key)
          end
        end
        memo
      end

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

        Sandstorm.redis.multi

        simple_attrs  = {}
        complex_attrs = {}
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
            redis_key = @complex_attributes[name.to_s]
            Sandstorm.redis.del(redis_key.key)
            unless value.blank?
              case attr_types[name.to_sym]
              when :list
                Sandstorm.redis.rpush(redis_key.key, *value)
              when :set
                Sandstorm.redis.sadd(redis_key.key, *value.to_a)
              when :hash
                values = value.inject([]) do |memo, (k, v)|
                  memo += [k, v]
                end
                Sandstorm.redis.hmset(redis_key.key, *values )
              end
            end
          end
        end

        # uses hmset
        # TODO check that nil value deletes relevant hash key
        unless remove_attrs.empty?
          Sandstorm.redis.hdel(@simple_attributes.key, remove_attrs)
        end
        values = simple_attrs.inject([]) do |memo, (k, v)|
          memo += [k, v]
        end
        unless values.empty?
          Sandstorm.redis.hmset(@simple_attributes.key, *values)
        end

        # update indices
        self.changes.select {|k| idx_attrs.include?(k)}.each_pair do |att, old_new|
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
        self.class.add_id(@attributes['id'])

        Sandstorm.redis.exec
      end

      # AM::Dirty
      @previously_changed = self.changes
      @changed_attributes.clear
      true
    end

    def key(att)
      # TODO raise error if not a 'complex' attribute
      @complex_attributes[att.to_s].key
    end

    def destroy
      run_callbacks :destroy do
        Sandstorm.redis.multi
        self.class.delete_id(@attributes['id'])
        (self.attributes.keys & self.class.send(:indexed_attributes)).each {|att|
          self.class.send("#{att}_index", @attributes[att]).delete_id( @attributes['id'])
        }
        Sandstorm.redis.del(@simple_attributes.key)
        @complex_attributes.values do |redis_key|
          Sandstorm.redis.del(redis_key.key)
        end
        Sandstorm.redis.exec
      end
    end

    private

    # http://stackoverflow.com/questions/7613574/activemodel-fields-not-mapped-to-accessors
    #
    # Simulate attribute writers from method_missing
    def attribute=(att, value)
      return if value == @attributes[att.to_s]
      if att.to_s == 'id'
        raise "Cannot reassign id" unless @attributes['id'].nil?
        send("id_will_change!")
        @attributes['id'] = value.to_s
        @simple_attributes = Sandstorm::RedisKey.new("#{record_key}:attrs", :hash)
        @complex_attributes = self.class.attribute_types.inject({}) do |memo, (name, type)|
          if Sandstorm::COLLECTION_TYPES.has_key?(type)
            memo[name.to_s] = Sandstorm::RedisKey.new("#{record_key}:attrs:#{name}", type)
          end
          memo
        end
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
      @attributes[att.to_s]
    end

    # Used by ActiveModel to lookup attributes during validations.
    def read_attribute_for_validation(att)
      @attributes[att.to_s]
    end

  end

end