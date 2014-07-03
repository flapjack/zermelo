require 'sandstorm/filters/base'

module Sandstorm

  module Filters

    class InfluxDBFilter

      include Sandstorm::Filters::Base

      private

      # NB not trying to handle steps yet
      def _exists?(id)
        return if id.nil?
        Sandstorm.influxdb.query("SELECT id from #{@initial_set.klass}")[@initial_set.klass].present?
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
            options   = step[1] || {}
            values    = step.last

            if step_count > 0
              case step_type
              when :intersect
                query += ' AND '
              when :union
                query += ' OR '
              else
                raise "Unhandled filter operation '#{step_type}'"
              end
            end

            query += values.collect {|k, v| "#{k} = '#{v}'" }.join(' AND ') + ")"

            step_count += 3
          end

        end

        result = Sandstorm.influxdb.query(query)

        data = result[@initial_set.klass]

        return [] if data.nil?

        case result_type
        when :ids
          data.collect {|d| d['id']}
        when :count
          data.first['count']
        end
      end

    end

  end

end