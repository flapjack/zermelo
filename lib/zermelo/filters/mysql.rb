require 'zermelo/filter'

require 'zermelo/filters/index_range'

require 'zermelo/ordered_set'

module Zermelo

  module Filters

    class MySQL

      include Zermelo::Filter

      SHORTCUTS = {
        # :list => {
        #   :ids     => proc {|key|     Zermelo::OrderedSet.new(Zermelo.redis.lrange(key, 0, -1)) },
        #   :count   => proc {|key|     Zermelo.redis.llen(key) },
        #   :exists? => proc {|key, id| Zermelo.redis.lrange(key, 0, -1).include?(id) },
        #   :first   => proc {|key|     Zermelo.redis.lrange(key, 0, 0).first },
        #   :last    => proc {|key|     Zermelo.redis.lrevrange(key, 0, 0).first }
        # },
        :set => {
          :ids     => proc {|key|
            ids = Zermelo.mysql.query("SELECT id FROM `#{key}`")
            (ids.count == 0) ? Set.new : Set.new( ids.map {|r| r['id'] } )
          },
          :count   => proc {|key|
            Zermelo.mysql.query("SELECT COUNT(*) FROM `#{key}`").first['COUNT(*)']
          },
          :exists? => proc {|key, id|
            count_sql = %Q[
SELECT COUNT(*)
FROM `#{key}`
WHERE `#{key}`.`id` = ?
]

            count_stmt = Zermelo.mysql.prepare(count_sql)
            count_result = count_stmt.execute(id)
            r = count_result.first['COUNT(*)'] > 0
            count_stmt.close
            r
          }
        },
        # :sorted_set => {
        #   :ids     => proc {|key, order|
        #     Zermelo::OrderedSet.new(Zermelo.redis.send((:desc.eql?(order) ? :zrevrange : :zrange), key, 0, -1))
        #   },
        #   :count   => proc {|key, order|     Zermelo.redis.zcard(key) },
        #   :exists? => proc {|key, order, id| !Zermelo.redis.zscore(key, id).nil? },
        #   :first   => proc {|key, order|
        #     Zermelo.redis.send((:desc.eql?(order) ? :zrevrange : :zrange), key, 0, 0).first
        #   },
        #   :last    => proc {|key, order|
        #     Zermelo.redis.send((:desc.eql?(order) ? :zrange : :zrevrange), key, 0, 0).first
        #   }
        # }
      }

      # # TODO polite error when first/last applied to set

      # more step users
      def first
      #   lock {
      #     first_id = resolve_steps(:first)
      #     first_id.nil? ? nil : _load(first_id)
      #   }
      end

      def last
      #   lock {
      #     last_id = resolve_steps(:last)
      #     last_id.nil? ? nil : _load(last_id)
      #   }
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

      def resolve_steps(shortcut = nil, *args)
        if @steps.empty?
          raise "Shortcut must be provided if no steps" if shortcut.nil?

          unless @callback_target_class.nil? || @callbacks.nil?
            br = @callbacks[:before_read]
            if !br.nil? && @callback_target_class.respond_to?(br)
              @callback_target_class.send(br, @callback_target_id)
            end
          end

          sc = Zermelo::Filters::MySQL::SHORTCUTS[@initial_key.type][shortcut]
          r_key = backend.key_to_backend_key(@initial_key)
      #     shortcut_params = if @initial_key.type == :sorted_set
      #       [r_key, @sort_order] + args
      #     else
      #       [r_key] + args
      #     end
          shortcut_params = [r_key] + args
          ret = sc.call(*shortcut_params)

          unless @callback_target_class.nil? || @callbacks.nil?
            ar = @callbacks[:after_read]
            if !ar.nil? && @callback_target_class.respond_to?(ar)
              @callback_target_class.send(ar, @callback_target_id)
            end
          end

          return(ret)
        end


        class_key = @associated_class.send(:class_key)

        # FIXME escape class key

        query = case shortcut
        when :ids
          "SELECT id FROM `#{class_key}`"
        when :count
          "SELECT COUNT(id) FROM `#{class_key}`"
        end

      #   unless @initial_key.id.nil?
      #     query += ' WHERE '

      #     initial_class_key = @initial_key.klass.send(:class_key)

      #     ii_query = "SELECT #{@initial_key.name} FROM \"#{initial_class_key}/#{@initial_key.id}\" " +
      #       "LIMIT 1"

      #     begin
      #       initial_id_data =
      #         Zermelo.influxdb.query(ii_query)["#{initial_class_key}/#{@initial_key.id}"]
      #     rescue ::InfluxDB::Error => ide
      #       raise unless
      #         /^Field #{@initial_key.name} doesn't exist in series #{initial_class_key}\/#{@initial_key.id}$/ === ide.message

      #       initial_id_data = nil
      #     end

      #     if initial_id_data.nil?
      #       ret = case shortcut
      #       when :ids
      #         Set.new
      #       when :count
      #         0
      #       end
      #       return ret
      #     end

      #     initial_ids = initial_id_data.first[@initial_key.name]

      #     if initial_ids.nil? || initial_ids.empty?
      #       # make it impossible for the query to return anything
      #       query += '(1 = 0)'
      #     else
      #       query += '((' + initial_ids.collect {|id|
      #         "id = #{escaped_id(id)}"
      #       }.join(') OR (') + '))'
      #     end
      #   end

        unless @steps.empty?
          query += (@initial_key.id.nil? ? ' WHERE ' : ' AND ') +
                   ('(' * @steps.size)

          first_step = steps.first

          attr_types = @associated_class.send(:attribute_types)

          query += @steps.collect {|step|
            step.resolve(backend, @associated_class, :first => (step == first_step),
              :attr_types  => attr_types)
          }.join("")
        end

      #   query += " ORDER ASC LIMIT 1"

      # FIXME prepare instead -- pass around string & array for statement and args

      # p query

      result = Zermelo.mysql.query(query)

      case shortcut
      when :count
        result.first["COUNT(id)"]
      when :ids
        if result.count == 0
          Set.new
        else
          Set.new( result.map {|r| r['id']} )
        end
      end

      #   begin
      #     result = Zermelo.influxdb.query(query)
      #   rescue ::InfluxDB::Error => ide
      #     raise unless /^Couldn't look up columns$/ === ide.message
      #     result = {}
      #   end

      #   data_keys = result.keys.select {|k| k =~ /^#{class_key}\// }

      #   case shortcut
      #   when :ids
      #     data_keys.empty? ? Set.new : Set.new(data_keys.collect {|k| k =~ /^#{class_key}\/(.+)$/; $1 })
      #   when :count
      #     data_keys.empty? ? 0 : data_keys.size
      #   end
      # end



      #   idx_attrs = @associated_class.send(:with_index_data) do |d|
      #     d.each_with_object({}) do |(name, data), memo|
      #       memo[name.to_s] = data.index_klass
      #     end
      #   end

      #   attr_types = @associated_class.send(:attribute_types)

      #   backend.temp_key_wrap do |temp_keys|
      #     result     = nil
      #     last_step  = @steps.last

      #     step_opts = {
      #       :index_attrs => idx_attrs,
      #       :attr_types  => attr_types,
      #       :temp_keys   => temp_keys,
      #       :source      => @initial_key,
      #       :initial_key => @initial_key,
      #       :sort_order  => @sort_order
      #     }

      #     @steps.each do |step|
      #       unless step.class.accepted_types.include?(step_opts[:source].type)
      #         raise "'#{step.class.name}' does not accept input type #{step_opts[:source].type}"
      #       end

      #       if step == last_step && !shortcut.nil?
      #         step_opts.update(:shortcut => shortcut, :shortcut_args => args)
      #       end

      #       unless @callback_target_class.nil? || @callbacks.nil?
      #         br = @callbacks[:before_read]
      #         if !br.nil? && @callback_target_class.respond_to?(br)
      #           @callback_target_class.send(br, @callback_target_id)
      #         end
      #       end

      #       result = step.resolve(backend, @associated_class, step_opts)

      #       unless @callback_target_class.nil? || @callbacks.nil?
      #         ar = @callbacks[:after_read]
      #         if !ar.nil? && @callback_target_class.respond_to?(ar)
      #           @callback_target_class.send(ar, @callback_target_id)
      #         end
      #       end

      #       if step == last_step
      #         temp_keys.delete(result) if shortcut.nil?
      #       else
      #         step_opts[:source] = result
      #       end
      #     end

      #     result
      #   end
      end

    end

  end

end