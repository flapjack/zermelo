require 'sandstorm/filters/base'

module Sandstorm

  module Filters

    class InfluxDBFilter

      include Sandstorm::Filters::Base

      # NB not trying to handle steps yet
      def _exists?(id)
        return if id.nil?
        Sandstorm.influxdb.query("SELECT id from #{@initial_set.klass}")[@initial_set.klass].size > 0
      end

      private

      def lock(when_steps_empty = true, *klasses, &block)
        # no-op
        block.call
      end

    end

  end

end