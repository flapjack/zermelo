require 'sandstorm/filters/base'

module Sandstorm
  module Filters
    class InfluxDBFilter

      include Sandstorm::Filters::Base

      private

      def _exists?(id)
        return if id.nil?
        @steps += [:intersect, {}, {:id => id}]
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

        step_type = step.first
        options   = step[1] || {}
        values    = step.last

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

          ii_query = "SELECT #{@initial_set.name} FROM /#{@initial_set.klass}\\/#{@initial_set.id}/ " +
            "LIMIT 1"

          initial_id_data =
            Sandstorm.influxdb.query(ii_query)["#{@initial_set.klass}/#{@initial_set.id}"]

          inital_ids = initial_id_data.nil? ? nil :
            initial_id_data.first[@initial_set.name]

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
                   ('(' * (@steps.size / 3))

          step_count = 0

          @steps.each_slice(3) do |step|

            step_type = step.first

            if step_count > 0
              case step_type
              when :intersect, :diff
                query += ' AND '
              when :union
                query += ' OR '
              else
                raise "Unhandled filter operation '#{step_type}'"
              end
            end

            query += resolve_step(step)

            query += ")"

            step_count += 3
          end

        end

        query += " LIMIT 1"

        result = Sandstorm.influxdb.query(query)
        data = result.select {|k, v| k =~ /^#{@associated_class.send(:class_key)}\// }

        case result_type
        when :ids
          data.nil? ? [] : data.keys.collect {|k| k =~ /^#{@associated_class.send(:class_key)}\/(.+)$/; $1 }
        when :count
          data.nil? ?  0 : data.values.inject(0) {|memo, d| memo += d.first['count']; memo}
        end
      end
    end
  end
end
