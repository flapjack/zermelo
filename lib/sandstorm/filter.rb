
module Sandstorm

  class Filter

    # initial set         a Sandstorm::RedisKey object
    # associated_class    the class of the result record
    def initialize(initial_set, associated_class)
      @initial_set = initial_set
      @associated_class = associated_class
      @steps = []
    end

    def intersect(opts = {})
      @steps += [:intersect, opts]
      self
    end

    def union(opts = {})
      @steps += [:union, opts]
      self
    end

    def diff(opts = {})
      @steps += [:diff, opts]
      self
    end

    def intersect_range(opts = {})
      @steps += [:intersect_range, opts]
      self
    end

    def union_range(opts = {})
      @steps += [:union_range, opts]
      self
    end

    def count
      resolve_steps {|set, desc|
        case @initial_set.type
        when :sorted_set
          Sandstorm.redis.zcard(set)
        when :set, nil
          Sandstorm.redis.scard(set)
        end
      }
    end

    def empty?
      count == 0
    end

    def all
      ids.map {|id| @associated_class.send(:load, id) }
    end

    def first
      raise "Can't get first member of a non-sorted set" unless @initial_set.type == :sorted_set
      first_id = resolve_steps{|set, order_desc|
        Sandstorm.redis.send(:zrange, set, 0, 0).first
      }
      return if first_id.nil?
      @associated_class.send(:load, first_id)
    end

    def last
      raise "Can't get last member of a non-sorted set" unless @initial_set.type == :sorted_set
      last_id = resolve_steps{|set, order_desc|
        Sandstorm.redis.send(:zrevrange, set, 0, 0).first
      }
      return if last_id.nil?
      @associated_class.send(:load, last_id)
    end

    def collect(&block)
      ids.collect {|id| block.call(@associated_class.send(:load, id))}
    end

    def each(&block)
      ids.each {|id| block.call(@associated_class.send(:load, id))}
    end

    def ids
      resolve_steps {|set, order_desc|
        case @initial_set.type
        when :sorted_set
          Sandstorm.redis.send((order_desc ? :zrevrange : :zrange), set, 0, -1)
        when :set, nil
          Sandstorm.redis.smembers(set).sort
        end
      }
    end

    private

    # TODO possible candidate for moving to a stored Lua script in the redis server?

    def temp_set_name
      "#{@associated_class.send(:class_key)}::tmp:#{SecureRandom.hex(16)}"
    end

    # return
    def indexed_step_to_set(att, value)
      index = @associated_class.send("#{att}_index", value)

      case index
      when Sandstorm::Associations::UniqueIndex
        idx_result = temp_set_name
        Sandstorm.redis.sadd(idx_result,
          Sandstorm.redis.hget(@associated_class.send("#{att}_index", value).key, value))
        [idx_result, true]
      when Sandstorm::Associations::Index
        [index.key, false]
      end
    end

    # takes a block and passes the name of the temporary set to it; deletes
    # the temporary set once done
    # NB set case could use sunion/sinter for the last step, sorted set case can't
    # TODO break this up, it's too large
    def resolve_steps(&block)
      return block.call(@initial_set.key, false) if @steps.empty?

      source_set = @initial_set.key
      dest_set   = temp_set_name

      idx_attrs = @associated_class.send(:indexed_attributes)

      order_desc = nil

      @steps.each_slice(2) do |step|

        order_desc = false

        temp_sets  = []
        source_keys = [source_set]

        if [:intersect, :union, :diff].include?(step.first)

          source_keys += step.last.inject([]) do |memo, (att, value)|
            next memo unless idx_attrs.include?(att.to_s)

            if value.is_a?(Enumerable)
              conditions_set = temp_set_name
              temp_idx_sets = []
              Sandstorm.redis.sunionstore(conditions_set, *value.collect {|val|
                idx_set, clear = indexed_step_to_set(att, val)
                temp_idx_sets << idx_set if clear
                idx_set
              })
              Sandstorm.redis.del(temp_idx_sets) unless temp_idx_sets.empty?
              temp_sets << conditions_set
              memo << conditions_set
            else
              idx_set, clear = indexed_step_to_set(att, value)
              temp_sets << idx_set if clear
              memo << idx_set
            end

            memo
          end

        elsif [:intersect_range, :union_range].include?(step.first)
          range_ids_set = temp_set_name

          options = step.last

          start = options[:start]
          finish = options[:end]

          if options[:by_score]
            start = '-inf' if start.nil? || (start <= 0)
            finish = '+inf' if finish.nil? || (finish <= 0)
          else
            start = 0 if start.nil?
            finish = -1 if finish.nil?
          end

          args = [start, finish]

          order_desc = options[:order] && 'desc'.eql?(options[:order].downcase)
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

          args.unshift(source_set)

          range_ids_scores = Sandstorm.redis.send(query, *args)

          unless range_ids_scores.empty?
            Sandstorm.redis.zadd(range_ids_set, range_ids_scores.map(&:reverse))
          end
          source_keys << range_ids_set
          temp_sets << range_ids_set
        end

        case @initial_set.type
        when :sorted_set
          weights = [1.0] + ([0.0] * (source_keys.length - 1))
          case step.first
          when :union, :union_range
            Sandstorm.redis.zunionstore(dest_set, source_keys, :weights => weights, :aggregate => 'max')
          when :intersect, :intersect_range
            Sandstorm.redis.zinterstore(dest_set, source_keys, :weights => weights, :aggregate => 'max')
          end
          Sandstorm.redis.del(range_ids_set) unless range_ids_set.nil?
        when :set, nil
          case step.first
          when :union
            Sandstorm.redis.sunionstore(dest_set, *source_keys)
          when :intersect
            Sandstorm.redis.sinterstore(dest_set, *source_keys)
          when :diff
            Sandstorm.redis.sdiffstore(dest_set, *source_keys)
          end
        end
        unless temp_sets.empty?
          Sandstorm.redis.del(temp_sets)
          temp_sets.clear
        end
        source_set = dest_set
      end
      ret = block.call(dest_set, order_desc)
      Sandstorm.redis.del(dest_set)

      ret
    end
  end

end
