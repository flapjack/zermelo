require 'sandstorm/filters/base'

# TODO check escaping of ids and index_keys -- shouldn't allow bare :

module Sandstorm

  module Filters

    class RedisFilter

     # abstraction for a set or list of record ids
      class Collection
        attr_reader :name, :type
        def initialize(opts = {})
          @name  = opts[:name]
          @type  = opts[:type]
        end
      end

      include Sandstorm::Filters::Base

      # more step users
      def first
        unless [:list, :sorted_set].include?(@initial_set.type) ||
          @steps.any? {|s| s.is_a?(Sandstorm::Filters::Steps::SortStep) }

          raise "Can't get first member of a non-sorted set"
        end

        lock {
          first_id = resolve_steps do |collection|
            op = {:list => :lrange, :sorted_set => :zrange}[collection.type]
            Sandstorm.redis.send(op, collection.name, 0, 0).first
          end
          first_id.nil? ? nil : _load(first_id)
        }
      end

      def last
        unless [:list, :sorted_set].include?(@initial_set.type) ||
          @steps.any? {|s| s.is_a?(Sandstorm::Filters::Steps::SortStep) }

          raise "Can't get last member of a non-sorted set"
        end

        lock {
          last_id = resolve_steps do |collection|
            op = {:list => :lrevrange, :sorted_set => :zrevrange}[collection.type]
            Sandstorm.redis.send(op, collection.name, 0, 0).first
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
            Sandstorm.redis.lrange(collection.name, 0, -1).include?(id)
          when :set
            Sandstorm.redis.sismember(collection.name, id)
          when :sorted_set
            !Sandstorm.redis.zscore(collection.name, id).nil?
          end
        end
      end

      def temp_set_name
        "#{class_key}::tmp:#{SecureRandom.hex(16)}"
      end

      def class_key
        @class_key ||= @associated_class.send(:class_key)
      end

      def indexed_step_to_set(att, idx_class, value, attr_type)
        # TODO (maybe) if a filter from different backend, resolve to ids and
        # put that in a Redis temp set

        case value
        when Sandstorm::Filters::RedisFilter

          collection, should_be_deleted = value.resolve_steps

          if should_be_deleted
            temp_sets << collection.name
          end

          unless :set.eql?(collection.type)
            raise "Unsure as yet if non-sets are safe as Filter step values"
          end

        when Regexp
          raise "Can't query non-string values via regexp" unless :string.eql?(attr_type)
          idx_result = temp_set_name
          starts_with_string_re = /^string:/
          case idx_class.name
          when 'Sandstorm::Associations::UniqueIndex'
            index_key = backend.key_to_redis_key(Sandstorm::Records::Key.new(
              :klass  => class_key,
              :name   => "by_#{att}",
              :type   => :hash,
              :object => :index
            ))
            candidates = Sandstorm.redis.hgetall(index_key)
            matching_ids = candidates.values_at(*candidates.keys.select {|k|
              (starts_with_string_re === k) &&
                (value === backend.unescape_key_name(k.sub(starts_with_string_re, '')))
            })
            Sandstorm.redis.sadd(idx_result, *matching_ids) unless matching_ids.empty?
          when 'Sandstorm::Associations::Index'
            key_root = backend.key_to_redis_key(Sandstorm::Records::Key.new(
              :klass  => class_key,
              :name   => "by_#{att}:string",
              :type   => :set,
              :object => :index
            ))

            matching_sets = Sandstorm.redis.keys(key_root + ":*").inject([]) do |memo, k|
              k =~ /^#{key_root}:(.+)$/
              memo << k if value === $1
              memo
            end

            Sandstorm.redis.sunionstore(idx_result, matching_sets) unless matching_sets.empty?
          end
          [idx_result, true]
        else
          index = @associated_class.send("#{att}_index")

          case index
          when Sandstorm::Associations::UniqueIndex
            idx_result = temp_set_name
            Sandstorm.redis.sadd(idx_result,
              Sandstorm.redis.hget(backend.key_to_redis_key(index.key),
                                   backend.index_keys(attr_type, value).join(':')))
            [idx_result, true]
          when Sandstorm::Associations::Index
            [backend.key_to_redis_key(index.key(value)), false]
          end
        end
      end

      def resolve_step(step, source, idx_attrs, attr_types, &block)
        temp_sets   = []
        source_keys = []

        case step
        when Sandstorm::Filters::Steps::IntersectStep,
             Sandstorm::Filters::Steps::UnionStep,
             Sandstorm::Filters::Steps::DiffStep

          source_keys += step.attributes.inject([]) do |memo, (att, value)|

            val = value.is_a?(Set) ? value.to_a : value

            if :id.eql?(att)
              ts = temp_set_name
              temp_sets << ts
              Sandstorm.redis.sadd(ts, val)
              memo << ts
            else
              idx_class = idx_attrs[att.to_s]
              raise "'#{att}' property is not indexed" if idx_class.nil?

              if val.is_a?(Enumerable)
                conditions_set = temp_set_name
                temp_idx_sets = []
                Sandstorm.redis.sunionstore(conditions_set, *val.collect {|v|
                  idx_set, clear = indexed_step_to_set(att, idx_class, v, attr_types[att])
                  temp_idx_sets << idx_set if clear
                  idx_set
                })
                Sandstorm.redis.del(temp_idx_sets) unless temp_idx_sets.empty?
                temp_sets << conditions_set
                memo << conditions_set
              else
                idx_set, clear = indexed_step_to_set(att, idx_class, val, attr_types[att])
                temp_sets << idx_set if clear
                memo << idx_set
              end
            end

            memo
          end

        when Sandstorm::Filters::Steps::IntersectRangeStep,
             Sandstorm::Filters::Steps::UnionRangeStep,
             Sandstorm::Filters::Steps::DiffRangeStep

          range_ids_set = temp_set_name

          options = step.options || {}

          start = options[:start]
          finish = options[:finish]

          order_desc = options[:desc].is_a?(TrueClass)

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

      # TODO could parts of this move to a stored Lua script in the redis server?

      # If called with a block --  takes a block and passes the name of a set to
      # it; deletes all temporary sets once done

      # If called with any arguments -- treats them as a hash of shortcuts

      # If not called with any arguments -- returns two values, the first is
      # the name of a set containing the filtered ids, the second is a boolean
      # for whether or not to clear up that set once it's been used

      def resolve_steps(shortcuts = {}, &block)
        source      = backend.key_to_redis_key(@initial_set)
        source_type = @initial_set.type

        if @steps.empty?
          ret = if shortcuts.empty?
            data = Sandstorm::Filters::RedisFilter::Collection.new(
              :name => source, :type => source_type)
            if block.nil?
              [data, false]
            else
              block.call(data)
            end
          else
            if :sorted_set.eql?(source_type) && :zrange.eql?(shortcuts[source_type])
              Sandstorm.redis.zrange(source, 0, -1)
            elsif :list.eql?(source_type) && :lrange.eql?(shortcuts[source_type])
              Sandstorm.redis.lrange(source, 0, -1)
            else
              Sandstorm.redis.send(shortcuts[source_type], source)
            end
          end
          return ret
        end

        temp_sets = []
        dest_set = nil
        ret = nil

        idx_attrs = @associated_class.send(:with_index_data) do |d|
          d.each_with_object({}) do |(name, data), memo|
            memo[name.to_s] = data.index_klass
          end
        end

        attr_types = @associated_class.send(:attribute_types)

        offset = nil
        limit  = nil
        order_desc = nil

        members = nil

        begin

          @steps.each_with_index do |step, idx|

            resolve_step(step, source, idx_attrs, attr_types) do |source_keys|
              options = step.options || {}

              order_desc = order_desc ^ options[:desc].is_a?(TrueClass)

              unless step.class.accepted_types.include?(source_type)
                raise "'#{step.class.name}' does not accept input type #{source_type}"
              end

              case source_type
              when :set
                sort_set = if step.is_a?(Sandstorm::Filters::Steps::SortStep)

                  proc {

                    # TODO raise error in step construction if keys not
                    # passed as expected below
                    sort_attrs_and_orders = case options[:keys]
                    when String, Symbol
                      {options[:keys].to_s => options[:desc].is_a?(TrueClass) ? :desc : :asc}
                    when Array
                      options[:keys].each_with_object({}) do |k, memo|
                        memo[k.to_sym] = (options[:desc].is_a?(TrueClass) ? :desc : :asc)
                      end
                    when Hash
                      options[:keys]
                    end

                    # TODO check if complex attribute types or associations
                    # can be used for sorting

                    Sandstorm.redis.sunionstore(dest_set, source, *source_keys)

                    sort_attrs_and_orders.keys.reverse.each_with_index do |sort_attr, idx|

                      order = sort_attrs_and_orders[sort_attr]

                      opts = {}

                      unless 'id'.eql?(sort_attr.to_s)
                        opts.update(:by => "#{class_key}:*:attrs->#{sort_attr}")
                      end

                      if (idx + 1) == sort_attrs_and_orders.size
                        # only apply offset & limit on the last sort
                        o = options[:offset]
                        l = options[:limit]

                        if !(l.nil? && o.nil?)
                          o = o.nil? ? 0 : o.to_i
                          l = (l.nil? || (l.to_i < 1)) ? (Sandstorm.redis.llen(dest_set) - o) : l
                          opts.update(:limit => [o, l])
                        end
                      end

                      order_parts = []
                      sort_attr_type = attr_types[sort_attr.to_sym]
                      unless [:integer, :float, :timestamp].include?(sort_attr_type)
                        order_parts << 'alpha'
                      end
                      order_parts << 'desc' if 'desc'.eql?(order.to_s)

                      unless order_parts.empty?
                        opts.update(:order => order_parts.join(' '))
                      end

                      opts.update(:store => dest_set)
                      Sandstorm.redis.sort(dest_set, opts)
                    end

                    source_type = :list
                  }
                else
                  nil
                end

                dest_set = temp_set_name
                temp_sets << dest_set

                if (idx == (@steps.size - 1)) && :smembers.eql?(shortcuts[:set])
                  members = case step
                  when Sandstorm::Filters::Steps::UnionStep
                    Sandstorm.redis.sinterstore(dest_set, *source_keys)
                    Sandstorm.redis.sunion(dest_set, source)
                  when Sandstorm::Filters::Steps::IntersectStep
                    Sandstorm.redis.sinter(source, *source_keys)
                  when Sandstorm::Filters::Steps::DiffStep
                    Sandstorm.redis.sinterstore(dest_set, *source_keys)
                    Sandstorm.redis.sdiff(source, dest_set)
                  when Sandstorm::Filters::Steps::SortStep
                    sort_set.call
                    Sandstorm.redis.send((order_desc ? :lrevrange : :lrange),
                                         dest_set, 0, -1)
                  end
                else

                  case step
                  when Sandstorm::Filters::Steps::UnionStep
                    Sandstorm.redis.sinterstore(dest_set, *source_keys)
                    Sandstorm.redis.sunionstore(dest_set, source, dest_set)
                  when Sandstorm::Filters::Steps::IntersectStep
                    Sandstorm.redis.sinterstore(dest_set, *source_keys)
                  when Sandstorm::Filters::Steps::DiffStep
                    Sandstorm.redis.sinterstore(dest_set, *source_keys)
                    Sandstorm.redis.sdiffstore(dest_set, source, dest_set)
                  when Sandstorm::Filters::Steps::SortStep
                    sort_set.call
                  end

                  source = dest_set
                end

              when :list
                # TODO could allow reversion into set by :union, :intersect, :diff,
                # or application of :sort again to re-order. For now, YAGNI, and
                # document the limitations.

                case step
                when Sandstorm::Filters::Steps::OffsetStep
                  offset = options[:amount]
                when Sandstorm::Filters::Steps::LimitStep
                  limit = options[:amount]
                end

              when :sorted_set
                weights = case step
                when Sandstorm::Filters::Steps::UnionStep, Sandstorm::Filters::Steps::UnionRangeStep
                  [0.0] * source_keys.length
                when Sandstorm::Filters::Steps::DiffStep, Sandstorm::Filters::Steps::DiffRangeStep
                  [-1.0] * source_keys.length
                end

                dest_set = temp_set_name
                temp_sets << dest_set

                case step
                when Sandstorm::Filters::Steps::UnionStep, Sandstorm::Filters::Steps::UnionRangeStep
                  Sandstorm.redis.zinterstore(dest_set, source_keys, :weights => weights, :aggregate => 'max')
                  Sandstorm.redis.zunionstore(dest_set, [source, dest_set])
                when Sandstorm::Filters::Steps::IntersectStep, Sandstorm::Filters::Steps::IntersectRangeStep
                  Sandstorm.redis.zinterstore(dest_set, [source] + source_keys, :weights => weights, :aggregate => 'max')
                when Sandstorm::Filters::Steps::DiffStep, Sandstorm::Filters::Steps::DiffRangeStep
                  # 'zdiffstore' via weights, relies on non-zero scores being used
                  # see https://code.google.com/p/redis/issues/detail?id=579
                  Sandstorm.redis.zinterstore(dest_set, source_keys, :weights => weights, :aggregate => 'max')
                  Sandstorm.redis.zunionstore(dest_set, [source, dest_set])
                  Sandstorm.redis.zremrangebyscore(dest_set, "0", "0")
                end

                if (idx == (@steps.size - 1)) && :zrange.eql?(shortcuts[:sorted_set])
                  # supporting shortcut structures here as it helps preserve the
                  # win gained by the shortcut for empty steps, but this is
                  # no better than passing it through to a block would be; if
                  # Redis still supported ZINTER and ZUNION it would work better
                  members = Sandstorm.redis.send((order_desc ? :zrevrange : :zrange),
                                                 dest_set, 0, -1)
                else
                  source = dest_set
                end

              end
            end

          end

          ret = if members.nil?
            if :list.eql?(source_type) && !(offset.nil? && limit.nil?)

              # TODO need a guaranteed non-existing key for non-sorting 'sort'
              o = offset.to_i
              l = limit.to_i
              l = (Sandstorm.redis.llen(dest_set) - o) if (limit < 1)

              opts = {:by => 'no_sort', :limit => [o, l]}

              # https://github.com/antirez/redis/issues/2079, fixed in redis 2.8.19
              if (Sandstorm.redis_version <=> ['2', '8', '18']) == 1
                opts.update(:store => dest_set)
                Sandstorm.redis.sort(dest_set, opts)
              else
                data = Sandstorm.redis.sort(dest_set, opts)

                if data.empty?
                  Sandstorm.redis.del(dest_set)
                else
                  limited = temp_set_name
                  temp_sets << limited

                  Sandstorm.redis.rpush(limited, data)

                  dest_set = limited
                end
              end
            end

            if shortcuts.empty?
              data = Sandstorm::Filters::RedisFilter::Collection.new(
                :name => dest_set, :type => source_type)
              if block.nil?
                should_be_deleted = !temp_sets.delete(dest_set).nil?
                [data, should_be_deleted]
              else
                block.call(data)
              end
            elsif :sorted_set.eql?(source_type) && :zrange.eql?(shortcuts[source_type])
              Sandstorm.redis.zrange(dest_set, 0, -1)
            elsif :list.eql?(source_type) && :lrange.eql?(shortcuts[source_type])
              Sandstorm.redis.lrange(dest_set, 0, -1)
            else
              Sandstorm.redis.send(shortcuts[source_type], dest_set)
            end

          else
            members
          end

        rescue
          raise
        ensure
          unless temp_sets.empty?
            Sandstorm.redis.del(*temp_sets)
            temp_sets.clear
          end
        end

        ret
      end
    end

  end

end