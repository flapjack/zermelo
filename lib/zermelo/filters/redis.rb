require 'zermelo/filter'

require 'zermelo/filters/index_range'

require 'zermelo/ordered_set'

# TODO check escaping of ids and index_keys -- shouldn't allow bare :

module Zermelo

  module Filters

    class Redis

      include Zermelo::Filter

      SHORTCUTS = {
        :list => {
          :ids     => proc {|key|     Zermelo::OrderedSet.new(Zermelo.redis.lrange(key, 0, -1)) },
          :count   => proc {|key|     Zermelo.redis.llen(key) },
          :exists? => proc {|key, id| Zermelo.redis.lrange(key, 0, -1).include?(id) },
          :first   => proc {|key|     Zermelo.redis.lrange(key, 0, 0).first },
          :last    => proc {|key|     Zermelo.redis.lrevrange(key, 0, 0).first }
        },
        :set => {
          :ids     => proc {|key|     Set.new(Zermelo.redis.smembers(key)) },
          :count   => proc {|key|     Zermelo.redis.scard(key) },
          :exists? => proc {|key, id| Zermelo.redis.sismember(key, id) }
        },
        :sorted_set => {
          :ids     => proc {|key, order|
            Zermelo::OrderedSet.new(Zermelo.redis.send((:desc.eql?(order) ? :zrevrange : :zrange), key, 0, -1))
          },
          :count   => proc {|key, order|     Zermelo.redis.zcard(key) },
          :exists? => proc {|key, order, id| !Zermelo.redis.zscore(key, id).nil? },
          :first   => proc {|key, order|
            Zermelo.redis.send((:desc.eql?(order) ? :zrevrange : :zrange), key, 0, 0).first
          },
          :last    => proc {|key, order|
            Zermelo.redis.send((:desc.eql?(order) ? :zrange : :zrevrange), key, 0, 0).first
          }
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

      # If not called with a shortcut, return value is the name of a set
      # containing the filtered ids
      def resolve_steps(shortcut = nil, *args)
        if @steps.empty?
          raise "Shortcut must be provided if no steps" if shortcut.nil?

          unless @callback_target_class.nil? || @callbacks.nil?
            br = @callbacks[:before_read]
            if !br.nil? && @callback_target_class.respond_to?(br)
              @callback_target_class.send(br, @callback_target_id)
            end
          end

          sc = Zermelo::Filters::Redis::SHORTCUTS[@initial_key.type][shortcut]
          r_key = backend.key_to_backend_key(@initial_key)
          shortcut_params = if @initial_key.type == :sorted_set
            [r_key, @sort_order] + args
          else
            [r_key] + args
          end
          ret = sc.call(*shortcut_params)

          unless @callback_target_class.nil? || @callbacks.nil?
            ar = @callbacks[:after_read]
            if !ar.nil? && @callback_target_class.respond_to?(ar)
              @callback_target_class.send(ar, @callback_target_id)
            end
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
            :initial_key => @initial_key,
            :sort_order  => @sort_order
          }

          @steps.each do |step|
            unless step.class.accepted_types.include?(step_opts[:source].type)
              raise "'#{step.class.name}' does not accept input type #{step_opts[:source].type}"
            end

            if step == last_step && !shortcut.nil?
              step_opts.update(:shortcut => shortcut, :shortcut_args => args)
            end

            unless @callback_target_class.nil? || @callbacks.nil?
              br = @callbacks[:before_read]
              if !br.nil? && @callback_target_class.respond_to?(br)
                @callback_target_class.send(br, @callback_target_id)
              end
            end

            result = step.resolve(backend, @associated_class, step_opts)

            unless @callback_target_class.nil? || @callbacks.nil?
              ar = @callbacks[:after_read]
              if !ar.nil? && @callback_target_class.respond_to?(ar)
                @callback_target_class.send(ar, @callback_target_id)
              end
            end

            if step == last_step
              temp_keys.delete(result) if shortcut.nil?
            else
              step_opts[:source] = result
            end
          end

          result
        end
      end
    end

  end

end