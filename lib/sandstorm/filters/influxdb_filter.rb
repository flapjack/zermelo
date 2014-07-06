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

      def resolve_steps(result_type)
        query = case result_type
        when :ids
          "SELECT id FROM #{@initial_set.klass}"
        when :count
          "SELECT COUNT(id) FROM #{@initial_set.klass}"
        end

        unless @steps.empty?

          query += ' WHERE '
          step_count = 0

          query += ('(' * (@steps.size / 3))

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

        result = Sandstorm.influxdb.query(query)

        data = result[@initial_set.klass]

        case result_type
        when :ids
          data.nil? ? [] : data.collect {|d| d['id']}
        when :count
          data.nil? ? 0 : data.first['count']
        end
      end

    end

  end

end