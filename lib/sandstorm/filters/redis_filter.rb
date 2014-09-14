require 'sandstorm/filters/base'

# TODO check escaping of ids and index_keys -- shouldn't allow bare :

module Sandstorm

  module Filters

    class RedisFilter

      include Sandstorm::Filters::Base

      # more step users
      def first
        error = nil
        first_obj = lock {
          first_id = resolve_steps {|redis_obj, redis_obj_type, order_desc|
            case redis_obj_type
            when :list
              Sandstorm.redis.lrange(redis_obj, 0, 0).first
            when :set
              error = "Can't get first member of a non-sorted set"
              nil
            when :sorted_set
              Sandstorm.redis.zrange(redis_obj, 0, 0).first
            end
          }
          first_id.nil? ? nil : _load(first_id)
        }
        raise error unless error.nil?
        first_obj
      end

      def last
        error = nil
        last_obj = lock {
          last_id = resolve_steps {|redis_obj, redis_obj_type, order_desc|
            case redis_obj_type
            when :list
              Sandstorm.redis.lrevrange(redis_obj, 0, 0).first
            when :set
              error = "Can't get first member of a non-sorted set"
              nil
            when :sorted_set
              Sandstorm.redis.zrevrange(redis_obj, 0, 0).first
            end
          }
          last_id.nil? ? nil : _load(last_id)
        }
        raise error unless error.nil?
        last_obj
      end
      # end step users

      private

      def _count
        resolve_steps(:list       => :llen,
                      :set        => :scard,
                      :sorted_set => :zcard)
      end

      def _exists?(id)
        return if id.nil?
        resolve_steps {|redis_obj, redis_obj_type, order_desc|
          case redis_obj_type
          when :list
            !Sandstorm.redis.lindex(redis_obj, id).nil?
          when :set
            Sandstorm.redis.sismember(redis_obj, id)
          when :sorted_set
            !Sandstorm.redis.zscore(redis_obj, id).nil?
          end
        }
      end

      def _ids
        if [:set].include?(@initial_set.type) &&
        # if [:set, :sorted_set].include?(@initial_set.type) &&
          @steps.none? {|s| :sort.eql?(s.action) }

          # TODO ensure zrange is done as zrevrange if order == desc
          # and, indeed, apply shortcut in this case in resolve_steps
          resolve_steps(:set        => :smembers,
                        :sorted_set => :zrange)

        else
          resolve_steps {|redis_obj, redis_obj_type, order_desc|
            case redis_obj_type
            when :list
              Sandstorm.redis.lrange(redis_obj, 0, -1)
            when :set
              Sandstorm.redis.smembers(redis_obj)
            when :sorted_set
              Sandstorm.redis.send((order_desc ? :zrevrange : :zrange), redis_obj, 0, -1)
            end
          }
        end
      end

      def temp_set_name
        "#{class_key}::tmp:#{SecureRandom.hex(16)}"
      end

      def class_key
        @class_key ||= @associated_class.send(:class_key)
      end

      def indexed_step_to_set(att, idx_class, value)

        case value
        when Regexp
          idx_result = temp_set_name
          case idx_class.name
          when 'Sandstorm::Associations::UniqueIndex'
            index_key = backend.key_to_redis_key(Sandstorm::Records::Key.new(
              :class  => class_key,
              :name   => "by_#{att}",
              :type   => :hash,
              :object => :index
            ))

            candidates = Sandstorm.redis.hgetall(index_key)
            matching_ids = candidates.values_at(*candidates.keys.select {|k| value === k })
            Sandstorm.redis.sadd(idx_result, *matching_ids) unless matching_ids.empty?

          when 'Sandstorm::Associations::Index'
            key_root = backend.key_to_redis_key(Sandstorm::Records::Key.new(
              :class  => class_key,
              :name   => nil,
              :type   => :set,
              :object => :index
            )) + "by_#{att}:"

            matching_sets = Sandstorm.redis.keys(key_root + "*").inject([]) do |memo, k|
              k =~ /^#{key_root}(.+)$/
              memo << k if value === $1
              memo
            end

            Sandstorm.redis.sinterstore(idx_result, *matching_sets) unless matching_sets.empty?
          end
          [idx_result, true]
        else
          index = @associated_class.send("#{att}_index", value)

          case index
          when Sandstorm::Associations::UniqueIndex
            idx_result = temp_set_name
            Sandstorm.redis.sadd(idx_result,
              Sandstorm.redis.hget(backend.key_to_redis_key(index.key), value))
            [idx_result, true]
          when Sandstorm::Associations::Index
            [backend.key_to_redis_key(index.key), false]
          end
        end
      end

      def resolve_step(step, source, idx_attrs, &block)
        temp_sets   = []
        source_keys = [source]

        if [:intersect, :union, :diff].include?(step.action)

          source_keys += step.attributes.inject([]) do |memo, (att, value)|
            idx_class = idx_attrs[att.to_s]
            raise "'#{att}' property is not indexed" if idx_class.nil?

            if value.is_a?(Enumerable)
              conditions_set = temp_set_name
              temp_idx_sets = []
              Sandstorm.redis.sunionstore(conditions_set, *value.collect {|val|
                idx_set, clear = indexed_step_to_set(att, idx_class, val)
                temp_idx_sets << idx_set if clear
                idx_set
              })
              Sandstorm.redis.del(temp_idx_sets) unless temp_idx_sets.empty?
              temp_sets << conditions_set
              memo << conditions_set
            else
              idx_set, clear = indexed_step_to_set(att, idx_class, value)
              temp_sets << idx_set if clear
              memo << idx_set
            end

            memo
          end

        elsif [:intersect_range, :union_range, :diff_range].include?(step.action)
          range_ids_set = temp_set_name

          options = step.options || {}

          start = options[:start]
          finish = options[:finish]

          order_desc = options[:order] && 'desc'.eql?(options[:order].downcase)

          if options[:by_score]
            start = '-inf' if start.nil? || (start <= 0)
            finish = '+inf' if finish.nil? || (finish <= 0)
          else
            start = 0 if start.nil?
            finish = -1 if finish.nil?
          end

          args = [start, finish]

          if order_desc
            if options[:by_score]
              query = :zrevrangebyscore
              args = args.map(&:to_s).reverse
            else
              query = :zrevrange
            end
          elsif options[:by_score]
            query = :zrangebyscore
            args = args.map(&:to_s)
          else
            query = :zrange
          end

          args << {:with_scores => :true}

          if options[:limit]
            args.last.update(:limit => [0, options[:limit].to_i])
          end

          args.unshift(source)

          range_ids_scores = Sandstorm.redis.send(query, *args)

          unless range_ids_scores.empty?
            Sandstorm.redis.zadd(range_ids_set, range_ids_scores.map(&:reverse))
          end
          source_keys << range_ids_set
          temp_sets << range_ids_set
        end

        yield(source_keys)

        unless temp_sets.empty?
          Sandstorm.redis.del(*temp_sets)
          temp_sets.clear
        end
      end

      # TODO possible candidate for moving to a stored Lua script in the redis server?

      # takes a block and passes the name of the temporary set to it; deletes
      # the temporary set once done
      def resolve_steps(shortcuts = {}, &block)
        source      = backend.key_to_redis_key(@initial_set)
        source_type = @initial_set.type

        if @steps.empty?
          ret = if shortcuts.empty?
            block ? block.call(source, source_type, false) : nil
          else
            Sandstorm.redis.send(shortcuts[source_type], source)
          end
          return ret
        end

        dest_set = nil
        temp_sets = []

        if shortcuts.empty?
          dest_set = temp_set_name
          temp_sets << dest_set
        end

        idx_attrs = @associated_class.send(:indexed_attributes)

        order_desc = nil

        members = nil

        @steps.each_with_index do |step, idx|

          resolve_step(step, source, idx_attrs) do |source_keys|
            options = step.options || {}
            order_opts = options[:order] ? options[:order].downcase.split : []
            order_desc = order_desc ^ order_opts.include?('desc')

            case source_type
            when :set
              case step.action
              when :union
                if (idx == (@steps.size - 1)) && :smembers.eql?(shortcuts[:set])
                  members = Sandstorm.redis.sunion(*source_keys)
                else
                  Sandstorm.redis.sunionstore(dest_set, *source_keys)
                end
              when :intersect
                if (idx == (@steps.size - 1)) && :smembers.eql?(shortcuts[:set])
                  members = Sandstorm.redis.sinter(*source_keys)
                else
                  Sandstorm.redis.sinterstore(dest_set, *source_keys)
                end
              when :diff
                if (idx == (@steps.size - 1)) && :smembers.eql?(shortcuts[:set])
                  members = Sandstorm.redis.sdiff(*source_keys)
                else
                  Sandstorm.redis.sdiffstore(dest_set, *source_keys)
                end
              when :sort
                # TODO 'sort' takes limit option, check if included in main
                # once that's implemented

                sort_attr = options[:key].to_s

                # sort by simple attribute -- hash member
                # TODO check if complex attribute types or associations
                # can be used for sorting
                opts = {:by => "#{class_key}:*:attrs->#{sort_attr}", :store => dest_set}

                order_parts = ['alpha', 'desc'].inject([]) do |memo, ord|
                  memo << ord if order_opts.include?(ord)
                  memo
                end

                unless order_parts.empty?
                  opts.update(:order => order_parts.join(' '))
                end

                if source_keys.length > 1
                  Sandstorm.redis.sunionstore(dest_set, *source_keys)
                  Sandstorm.redis.sort(dest_set, opts)
                else
                  Sandstorm.redis.sort(source_keys.first, opts)
                end

                source_type = :list
              end

            when :list

              # TODO handle any operations chained after source_type
              # becomes :list via :sort

            when :sorted_set
              weights = case step.action
              when :union, :union_range
                [1.0] + ([0.0] * (source_keys.length - 1))
              when :diff, :diff_range
                [1.0] + ([-1.0] * (source_keys.length - 1))
              end

              case step.action
              when :union, :union_range
                Sandstorm.redis.zunionstore(dest_set, source_keys, :weights => weights, :aggregate => 'max')
              when :intersect, :intersect_range
                Sandstorm.redis.zinterstore(dest_set, source_keys, :weights => weights, :aggregate => 'max')
              when :diff, :diff_range
                # 'zdiffstore' via weights, relies on non-zero scores being used
                # see https://code.google.com/p/redis/issues/detail?id=579
                Sandstorm.redis.zunionstore(dest_set, source_keys, :weights => weights, :aggregate => 'sum')
                Sandstorm.redis.zremrangebyscore(dest_set, "0", "0")
              end

            end
          end

          source = dest_set
        end

        ret = if shortcuts.empty?
          block ? block.call(dest_set, source_type, order_desc) : nil
        else
          members || Sandstorm.redis.send(shortcuts[source_type], dest_set)
        end

        unless temp_sets.empty?
          Sandstorm.redis.del(*temp_sets)
          temp_sets.clear
        end
        ret
      end
    end

  end

end