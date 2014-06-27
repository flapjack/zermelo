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

      def redis_key(key)
        "#{key.klass}:#{key.id.nil? ? '' : key.id}:#{key.name}"
      end

      # only relevant for :sorted_set
      def get_at(key, index)
        raise "Invalid data type for #get_at ('#{key.type})'" unless :sorted_set.eql?(key.type)

        # TODO
        nil
      end

      # only relevant for :sorted_set (e.g. redis by_score) & :hash
      def get(key, value)
        raise "Invalid data type for #get ('#{key.type})'" unless [:sorted_set, :hash].include?(key.type)

        # TODO
        nil
      end

      def get_all(key)
        case key.type
        when :list
          Sandstorm.redis.lrange(redis_key(key), 0, -1)
        when :set
          Sandstorm.redis.smembers(redis_key(key))
        # when :sorted_set
        #   Sandstorm.redis.zrange(redis_key(key), 0, -1)
        when :hash
          Sandstorm.redis.hgetall(redis_key(key))
        end
      end

      def exists?(key)
        Sandstorm.redis.exists(redis_key(key))
      end

      def include?(key, id)
        case key.type
        when :set
          Sandstorm.redis.sismember(redis_key(key), id)
        when :sorted_set
          !Sandstorm.redis.zscore(redis_key(key), id).nil?
        else
          raise "Not implemented"
        end
      end

      def lock(*klasses)
        Sandstorm::Locks::RedisLock.new(*klasses)
      end

      def begin_transaction
        Sandstorm.redis.multi
        super
      end

      def commit_transaction
        @steps.each do |step|
          op     = step[0]
          key    = step[1]
          values = step[2..-1]

          case op
          when :add
            case key.type
            when :list
              Sandstorm.redis.rpush(redis_key(key), 0, values)
            when :set
              Sandstorm.redis.sadd(redis_key(key), values)
            # when :sorted_set
              # Sandstorm.redis.zadd(redis_key(key), values)
            when :hash
              kv = values.inject([]) do |memo, hash|
                hash.each_pair do |k, v|
                  memo += [k, v]
                end
                memo
              end
              Sandstorm.redis.hmset(redis_key(key), *kv)
            end
          when :delete
            case key.type
            when :list
              Sandstorm.redis.lrem(redis_key(key), 0, values)
            when :set
              Sandstorm.redis.srem(redis_key(key), values)
            # when :sorted_set
            #   Sandstorm.redis.zrem(redis_key(key), values)
            when :hash
              Sandstorm.redis.hdel(redis_key(key), values)
            end
          when :clear
            Sandstorm.redis.del(redis_key(key))
          end
        end

        super

        Sandstorm.redis.exec
      end

      def abort_transaction
        super
        Sandstorm.redis.discard
      end

    end

  end

end