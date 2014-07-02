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

      def add(key, value)
        change(:add, key, value)
      end

      def delete(key, value)
        change(:delete, key, value)
      end

      def clear(key)
        change(:clear, key)
      end

      def set(key, value)
        change(:set, key, value)
      end

      def purge(key)
        change(:purge, key)
      end

      def get(*attr_keys)
        attr_keys.inject({}) do |memo, attr_key|

          memo[attr_key.klass] ||= {}
          memo[attr_key.klass][attr_key.id] ||= {}
          memo[attr_key.klass][attr_key.id][attr_key.name.to_s] = if Sandstorm::COLLECTION_TYPES.has_key?(attr_key.type)

            complex_attr_key = "#{key.klass}:#{key.id}:attrs:#{attr_key.name}"

            case attr_key.type
            when :list
              # if sub_key.nil?
                # get all
                Sandstorm.redis.lrange(complex_attr_key, 0, -1)
              # else
              #   # sub_key is an index
              #   Sandstorm.redis.lrange(complex_attr_key, sub_key, sub_key)
              # end
            when :set
              # if sub_key.nil?
                # get all
                Set.new( Sandstorm.redis.smembers(complex_attr_key) )
              # end
            when :sorted_set
              # if sub_key.nil?
              #   # get all
              # else
              #   # get sub
              # end
            when :hash
              # if sub_key.nil?
                Sandstorm.redis.hgetall(complex_attr_key)
              # else
              #   Sandstorm.redis.hget(complex_attr_key, sub_key)
              # end
            end

          else
            value = Sandstorm.redis.hget("#{attr_key.klass}:#{attr_key.id}:attrs",
                                         attr_key.name.to_s)

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
        @in_transaction = true
        @changes = []
      end

      def commit_transaction
        apply_changes(@changes)
        Sandstorm.redis.exec
        @in_transaction = false
        @changes = []
      end

      def abort_transaction
        Sandstorm.redis.discard
        @in_transaction = false
        @changes = []
      end

      def redis_key(key)
        "#{key.klass}:#{key.id.nil? ? '' : key.id}:#{key.name}"
      end

      private

      def change(op, key, value = nil)
        ch = [op, key, value]
        if @in_transaction
          @changes << ch
        else
          apply_changes([ch])
        end
      end

      def apply_changes(changes)
        simple_attrs  = {}

        purges = []

        # p changes

        changes.each do |ch|
          op    = ch[0]
          key   = ch[1]
          value = ch[2]

          # TODO check that collection types handle nil value for whole thing
          if Sandstorm::COLLECTION_TYPES.has_key?(key.type)

            complex_attr_key = key.id.nil? ?
              "#{key.klass}::#{key.name}" :
              "#{key.klass}:#{key.id}:attrs:#{key.name}"

            case op
            when :add, :set
              case key.type
              when :list
                Sandstorm.redis.del(complex_attr_key) if :set.eql?(op)
                Sandstorm.redis.rpush(complex_attr_key, value)
              when :set
                Sandstorm.redis.del(complex_attr_key) if :set.eql?(op)
                Sandstorm.redis.sadd(complex_attr_key, value)
              # when :sorted_set
              when :hash
                Sandstorm.redis.del(complex_attr_key) if :set.eql?(op)
                kv = value.inject([]) do |memo, (k, v)|
                  memo += [k, v]
                  memo
                end
                Sandstorm.redis.hmset(complex_attr_key, *kv)
              end
            when :delete
              case key.type
              when :list
                Sandstorm.redis.lrem(complex_attr_key, value, 0)
              when :set
                Sandstorm.redis.srem(complex_attr_key, value)
              # when :sorted_set
              when :hash
                Sandstorm.redis.hdel(complex_attr_key, value)
              end
            when :clear
              Sandstorm.redis.del(complex_attr_key)
            end

          elsif :purge.eql?(op)
            purges << "#{key.klass}:#{key.id}:attrs"
          else
            simple_attr_key = "#{key.klass}:#{key.id}:attrs"
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