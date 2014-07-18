require 'sandstorm/backends/base'

require 'sandstorm/filters/redis_filter'

require 'sandstorm/locks/redis_lock'

module Sandstorm

  module Backends

    class RedisBackend

      include Sandstorm::Backends::Base

      def filter(ids_key, record)
        Sandstorm::Filters::RedisFilter.new(self, ids_key, record)
      end

      def get_multiple(*attr_keys)
        attr_keys.inject({}) do |memo, attr_key|

          redis_attr_key = key_to_redis_key(attr_key)

          memo[attr_key.klass] ||= {}
          memo[attr_key.klass][attr_key.id] ||= {}
          memo[attr_key.klass][attr_key.id][attr_key.name.to_s] = if Sandstorm::COLLECTION_TYPES.has_key?(attr_key.type)

            case attr_key.type
            when :list
              Sandstorm.redis.lrange(redis_attr_key, 0, -1)
            when :set
              Set.new( Sandstorm.redis.smembers(redis_attr_key) )
            when :hash
              Sandstorm.redis.hgetall(redis_attr_key)
            when :sorted_set
              Sandstorm.redis.zrange(redis_attr_key, 0, -1)
            end

          else
            value = Sandstorm.redis.hget(redis_attr_key, attr_key.name.to_s)

            if value.nil?
              nil
            else
              case attr_key.type
              when :string
                value.to_s
              when :integer
                value.to_i
              when :float
                value.to_f
              when :timestamp
                Time.at(value.to_f)
              when :boolean
                case value
                when TrueClass
                  true
                when FalseClass
                  false
                when String
                  'true'.eql?(value.downcase)
                else
                  nil
                end
              end
            end
          end
          memo
        end
      end

      def exists?(key)
        Sandstorm.redis.exists(key_to_redis_key(key))
      end

      def include?(key, id)
        case key.type
        when :set
          Sandstorm.redis.sismember(key_to_redis_key(key), id)
        else
          raise "Not implemented"
        end
      end

      def lock(*klasses)
        Sandstorm::Locks::RedisLock.new(*klasses)
      end

      def begin_transaction
        return false if @in_transaction
        Sandstorm.redis.multi
        @in_transaction = true
        @changes = []
        true
      end

      def commit_transaction
        return false unless @in_transaction
        apply_changes(@changes)
        Sandstorm.redis.exec
        @in_transaction = false
        @changes = []
        true
      end

      def abort_transaction
        return false unless @in_transaction
        Sandstorm.redis.discard
        @in_transaction = false
        @changes = []
        true
      end

      # used by redis_filter
      def key_to_redis_key(key)
        obj = case key.object
        when :attribute
          'attrs'
        when :association
          'assocs'
        when :index
          'indices'
        end

        name = Sandstorm::COLLECTION_TYPES.has_key?(key.type) ? ":#{key.name}" : ''

        "#{key.klass}:#{key.id.nil? ? '' : key.id}:#{obj}#{name}"
      end

      private

      def change(op, key, value = nil, key_to = nil)
        ch = [op, key, value, key_to]
        if @in_transaction
          @changes << ch
        else
          apply_changes([ch])
        end
      end

      def apply_changes(changes)
        simple_attrs  = {}

        purges = []

        changes.each do |ch|
          op     = ch[0]
          key    = ch[1]
          value  = ch[2]
          key_to = ch[3]

          # TODO check that collection types handle nil value for whole thing
          if Sandstorm::COLLECTION_TYPES.has_key?(key.type)

            complex_attr_key = key_to_redis_key(key)

            case op
            when :add, :set
              case key.type
              when :list
                Sandstorm.redis.del(complex_attr_key) if :set.eql?(op)
                Sandstorm.redis.rpush(complex_attr_key, value)
              when :set
                Sandstorm.redis.del(complex_attr_key) if :set.eql?(op)
                Sandstorm.redis.sadd(complex_attr_key, value)
              when :hash
                Sandstorm.redis.del(complex_attr_key) if :set.eql?(op)
                kv = value.inject([]) do |memo, (k, v)|
                  memo += [k, v]
                  memo
                end
                Sandstorm.redis.hmset(complex_attr_key, *kv)
              when :sorted_set
                Sandstorm.redis.zadd(complex_attr_key, *value)
              end
            when :move
              case key.type
              when :set
                Sandstorm.redis.smove(complex_attr_key, key_to_redis_key(key_to), value)
              when :list
                raise "Not yet implemented"
              when :hash
                values = value.to_a.flatten
                Sandstorm.redis.hdel(complex_attr_key, values)
                Sandstorm.redis.hset(key_to_redis_key(key_to), *values)
              when :sorted_set
                raise "Not yet implemented"
              end
            when :delete
              case key.type
              when :list
                Sandstorm.redis.lrem(complex_attr_key, value, 0)
              when :set
                Sandstorm.redis.srem(complex_attr_key, value)
              when :hash
                Sandstorm.redis.hdel(complex_attr_key, value)
              when :sorted_set
                Sandstorm.redis.zrem(complex_attr_key, value)
              end
            when :clear
              Sandstorm.redis.del(complex_attr_key)
            end

          elsif :purge.eql?(op)
            # TODO get keys for all assocs & indices, purge them too
            purges << ["#{key.klass}:#{key.id}:attrs"]
          else
            simple_attr_key = key_to_redis_key(key)
            simple_attrs[simple_attr_key] ||= {}

            case op
            when :set
              simple_attrs[simple_attr_key][key.name] = if value.blank?
                nil
              else
                case key.type
                when :string, :integer
                  value.to_s
                when :timestamp
                  value.to_f
                when :boolean
                  (!!value).to_s
                end
              end
            when :clear
              simple_attrs[simple_attr_key][key.name] = nil
            end
          end
        end

        unless simple_attrs.empty?
          simple_attrs.each_pair do |simple_attr_key, values|
            hset = []
            hdel = []
            values.each_pair do |k, v|
              if v.nil?
                hdel << k
              else
                hset += [k, v]
              end
            end
            Sandstorm.redis.hmset(simple_attr_key, *hset) if hset.present?
            Sandstorm.redis.hdel(simple_attr_key, hdel) if hdel.present?
          end
        end

        purges.each {|purge_key | Sandstorm.redis.del(purge_key) }
      end

    end

  end

end