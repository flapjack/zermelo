require 'sandstorm/filters/base'

module Sandstorm
  module Filters
    class InfluxDBFilter

      include Sandstorm::Filters::Base

      private

      def _exists?(id)
        return if id.nil?
        @steps << Sandstorm::Filters::Step.new(:intersect, {}, {:id => id})
        resolve_steps(:count) > 0
      end

      def lock(when_steps_empty = true, *klasses, &block)
        # no-op
        block.call
      end

      def _ids
        resolve_steps(:ids)
      end

      def _count
        resolve_steps(:count)
      end

      def resolve_step(step)
        query = ''

        step_type = step.action
        options   = step.options || {}
        values    = step.attributes

        case step_type
        when :intersect, :union
          query += values.collect {|k, v|
            op, value = case v
            when String
              ["=~", "/^#{Regexp.escape(v).gsub(/\\\\/, "\\")}$/"]
            else
              ["=",  "'#{v}'"]
            end

           "#{k} #{op} #{value}"
          }.join(' AND ')
        when :diff
          query += values.collect {|k, v|
            op, value = case v
            when String
              ["!~", "/^#{Regexp.escape(v).gsub(/\\\\/, "\\")}$/"]
            else
              ["!=",  "'#{v}'"]
            end

            "#{k} #{op} #{value}"
          }.join(' AND ')
        else
          raise "Unhandled filter operation '#{step_type}'"
        end

        query
      end

      def escaped_id(id)
        if id.is_a?(Numeric)
          id
        else
          "'" + id.gsub(/'/, "\\'").gsub(/\\/, "\\\\'") + "'"
        end
      end

      def resolve_steps(result_type)
        query = case result_type
        when :ids
          "SELECT id FROM /#{@associated_class.send(:class_key)}\\/.*/"
        when :count
          "SELECT COUNT(id) FROM /#{@associated_class.send(:class_key)}\\/.*/"
        end

        unless @initial_set.id.nil?
          query += ' WHERE '

          ii_query = "SELECT #{@initial_set.name} FROM \"#{@initial_set.klass}/#{@initial_set.id}\" " +
            "LIMIT 1"

          begin
            initial_id_data =
              Sandstorm.influxdb.query(ii_query)["#{@initial_set.klass}/#{@initial_set.id}"]
          rescue InfluxDB::Error => ide
            raise unless
              /^Field #{@initial_set.name} doesn't exist in series #{@initial_set.klass}\/#{@initial_set.id}$/ === ide.message

            initial_id_data = nil
          end

          return [] if initial_id_data.nil?

          inital_ids = initial_id_data.first[@initial_set.name]

          if inital_ids.nil? || inital_ids.empty?
            # make it impossible for the query to return anything
            query += '(1 = 0)'
          else
            query += '((' + inital_ids.collect {|id|
              "id = #{escaped_id(id)}"
            }.join(') OR (') + '))'
          end
        end

        unless @steps.empty?
          query += (@initial_set.id.nil? ? ' WHERE ' : ' AND ') +
                   ('(' * @steps.size)

          @steps.each_with_index do |step, idx|
            if idx > 0
              case step.action
              when :intersect, :diff
                query += ' AND '
              when :union
                query += ' OR '
              else
                raise "Unhandled filter operation '#{step.action}'"
              end
            end

            query += resolve_step(step)

            query += ")"
          end
        end

        query += " LIMIT 1"

        result = Sandstorm.influxdb.query(query)
        data_keys = result.keys.select {|k| k =~ /^#{@associated_class.send(:class_key)}\// }

        case result_type
        when :ids
          data_keys.empty? ? [] : data_keys.collect {|k| k =~ /^#{@associated_class.send(:class_key)}\/(.+)$/; $1 }
        when :count
          data_keys.empty? ?  0 : data_keys.inject(0) do |memo, k|
            memo += result[k].first['count']
            memo
          end
        end
      end
    end
  end
end
