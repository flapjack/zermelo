require 'zermelo/filters/base'

# TODO check escaping of ids and index_keys -- shouldn't allow bare :

module Zermelo

  module Filters

    class RedisFilter

      include Zermelo::Filters::Base

      # more step users
      def first
        unless [:list, :sorted_set].include?(@initial_key.type) ||
          @steps.any? {|s| s.is_a?(Zermelo::Filters::Steps::SortStep) }

          raise "Can't get first member of a non-sorted set"
        end

        lock {
          first_id = resolve_steps do |collection|
            op = {:list => :lrange, :sorted_set => :zrange}[collection.type]
            Zermelo.redis.send(op, backend.key_to_redis_key(collection), 0, 0).first
          end
          first_id.nil? ? nil : _load(first_id)
        }
      end

      def last
        unless [:list, :sorted_set].include?(@initial_key.type) ||
          @steps.any? {|s| s.is_a?(Zermelo::Filters::Steps::SortStep) }

          raise "Can't get last member of a non-sorted set"
        end

        lock {
          last_id = resolve_steps do |collection|
            op = {:list => :lrevrange, :sorted_set => :zrevrange}[collection.type]
            Zermelo.redis.send(op, backend.key_to_redis_key(collection), 0, 0).first
          end
          last_id.nil? ? nil : _load(last_id)
        }
      end
      # end step users

      private

      def _count
        resolve_steps(:list       => :llen,
                      :set        => :scard,
                      :sorted_set => :zcard)
      end

      def _ids
        resolve_steps(:list       => :lrange,
                      :set        => :smembers,
                      :sorted_set => :zrange)
      end

      def _exists?(id)
        return if id.nil?
        resolve_steps do |collection|
          case collection.type
          when :list
            Zermelo.redis.lrange(backend.key_to_redis_key(collection), 0, -1).include?(id)
          when :set
            Zermelo.redis.sismember(backend.key_to_redis_key(collection), id)
          when :sorted_set
            !Zermelo.redis.zscore(backend.key_to_redis_key(collection), id).nil?
          end
        end
      end

      def solve(key, opts = {}, &block)
        delete    = opts[:delete].is_a?(TrueClass)
        shortcuts = opts[:shortcuts]
        if shortcuts.empty?
          if block_given?
            yield key
          else
            [key, delete]
          end
        elsif :sorted_set.eql?(key.type) && :zrange.eql?(shortcuts[:sorted_set])
          Zermelo.redis.zrange(backend.key_to_redis_key(key), 0, -1)
        elsif :list.eql?(key.type) && :lrange.eql?(shortcuts[:list])
          Zermelo.redis.lrange(backend.key_to_redis_key(key), 0, -1)
        else
          Zermelo.redis.send(shortcuts[key.type], backend.key_to_redis_key(key))
        end
      end

      # TODO could parts of this move to a stored Lua script in the redis server?

      # If called with a block --  takes a block and passes the name of a set to
      # it; deletes all temporary sets once done

      # If called with any arguments -- treats them as a hash of shortcuts

      # If not called with any arguments -- returns two values, the first is
      # the name of a set containing the filtered ids, the second is a boolean
      # for whether or not to clear up that set once it's been used

      def resolve_steps(shortcuts = {}, &block)
        return solve(@initial_key, :shortcuts => shortcuts, &block) if @steps.empty?

        idx_attrs = @associated_class.send(:with_index_data) do |d|
          d.each_with_object({}) do |(name, data), memo|
            memo[name.to_s] = data.index_klass
          end
        end

        attr_types = @associated_class.send(:attribute_types)

        backend.temp_key_wrap do |temp_keys|

          collection = @initial_key
          last_step  = @steps.last

          @steps.each do |step|
            unless step.class.accepted_types.include?(collection.type)
              raise "'#{step.class.name}' does not accept input type #{collection.type}"
            end

            collection = step.resolve(backend, @associated_class, collection,
              idx_attrs, attr_types, temp_keys,
              :shortcuts => ((step == last_step) ? shortcuts : nil))
          end

          case collection
          when Zermelo::Records::Key
            solve(collection, :delete => !temp_keys.delete(collection).nil?,
              :shortcuts => shortcuts, &block)
          else
            collection
          end
        end
      end
    end

  end

end