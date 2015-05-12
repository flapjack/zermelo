require 'zermelo/filter'

require 'zermelo/filters/index_range'

# TODO check escaping of ids and index_keys -- shouldn't allow bare :

module Zermelo

  module Filters

    class Redis

      include Zermelo::Filter

      SHORTCUTS = {
        :list => {
          :ids     => proc {|key|     Zermelo.redis.lrange(key, 0, -1) },
          :count   => proc {|key|     Zermelo.redis.llen(key) },
          :exists? => proc {|key, id| Zermelo.redis.lrange(key, 0, -1).include?(id) },
          :first   => proc {|key|     Zermelo.redis.lrange(key, 0, 0).first },
          :last    => proc {|key|     Zermelo.redis.lrevrange(key, 0, 0).first }
        },
        :set => {
          :ids     => proc {|key|     Zermelo.redis.smembers(key) },
          :count   => proc {|key|     Zermelo.redis.scard(key) },
          :exists? => proc {|key, id| Zermelo.redis.sismember(key, id) }
        },
        :sorted_set => {
          :ids     => proc {|key|     Zermelo.redis.zrange(key, 0, -1) },
          :count   => proc {|key|     Zermelo.redis.zcard(key) },
          :exists? => proc {|key, id| !Zermelo.redis.zscore(key, id).nil? },
          :first   => proc {|key|     Zermelo.redis.zrange(key, 0, 0).first },
          :last    => proc {|key|     Zermelo.redis.zrevrange(key, 0, 0).first }
        }
      }

      # TODO polite error when first/last applied to set

      # more step users
      def first
        lock {
          first_id = resolve_steps(:first)
          first_id.nil? ? nil : _load(first_id)
        }
      end

      def last
        lock {
          last_id = resolve_steps(:last)
          last_id.nil? ? nil : _load(last_id)
        }
      end
      # end step users

      private

      def _count
        resolve_steps(:count)
      end

      def _ids
        resolve_steps(:ids)
      end

      def _exists?(id)
        return if id.nil?
        resolve_steps(:exists?, id)
      end

      # If called with a block --  takes a block and passes the name of a set to
      # it; deletes all temporary sets once done

      # If called with any arguments -- treats them as a hash of shortcuts

      # If not called with any arguments -- returns two values, the first is
      # the name of a set containing the filtered ids, the second is a boolean
      # for whether or not to clear up that set once it's been used

      def resolve_steps(shortcut, *args)
        if @steps.empty?

          unless @callback_target.nil? || @callbacks.nil?
            br = @callbacks[:before_read]
            @callback_target.send(br) if !br.nil? && @callback_target.respond_to?(br)
          end

          sc = Zermelo::Filters::Redis::SHORTCUTS[@initial_key.type][shortcut]
          ret = if sc.nil?
            yield(@initial_key)
          else
            sc.call(*([backend.key_to_redis_key(@initial_key)] + args))
          end

          unless @callback_target.nil? || @callbacks.nil?
            ar = @callbacks[:after_read]
            @callback_target.send(ar) if !ar.nil? && @callback_target.respond_to?(ar)
          end

          return(ret)
        end

        idx_attrs = @associated_class.send(:with_index_data) do |d|
          d.each_with_object({}) do |(name, data), memo|
            memo[name.to_s] = data.index_klass
          end
        end

        attr_types = @associated_class.send(:attribute_types)

        backend.temp_key_wrap do |temp_keys|
          result     = nil
          last_step  = @steps.last

          step_opts = {
            :index_attrs => idx_attrs,
            :attr_types  => attr_types,
            :temp_keys   => temp_keys,
            :source      => @initial_key,
            :initial_key => @initial_key
          }

          @steps.each do |step|
            unless step.class.accepted_types.include?(step_opts[:source].type)
              raise "'#{step.class.name}' does not accept input type #{step_opts[:source].type}"
            end

            if step == last_step
              step_opts.update(:shortcut => shortcut, :shortcut_args => args)
            end

            unless @callback_target.nil? || @callbacks.nil?
              br = @callbacks[:before_read]
              @callback_target.send(br) if !br.nil? && @callback_target.respond_to?(br)
            end

            result = step.resolve(backend, @associated_class, step_opts)

            unless @callback_target.nil? || @callbacks.nil?
              ar = @callbacks[:after_read]
              @callback_target.send(ar) if !ar.nil? && @callback_target.respond_to?(ar)
            end

            step_opts[:source] = result unless step == last_step
          end

          result
        end
      end
    end

  end

end