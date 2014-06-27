require 'sandstorm/backends/base'

require 'sandstorm/filters/influxdb_filter'

# NB influxdb doesn't support individually addressable deletes, so
# this backend only works to write new records
# (it could just write the new state of the record, and query id by newest limit 1,
#  but for the moment, YAGNI)

module Sandstorm

  module Backends

    class InfluxDBBackend

      include Sandstorm::Backends::Base

      def filter(ids_key, record)
        Sandstorm::Filters::InfluxDBFilter.new(self, ids_key, record)
      end

      # TODO get filter calling this instead of using same logic
      def exists?(key)
        return if key.id.nil?
        Sandstorm.influxdb.query("SELECT id from #{key.klass}")[key.klass].size > 0
      end

      # # only relevant for :sorted_set
      # def get_at(key, index)
      #   raise "Invalid data type for #get_at ('#{key.type})'" unless :sorted_set.eql?(key.type)
      #   # TODO
      # end

      # only relevant for :sorted_set (e.g. redis by_score) & :hash
      def get(key, value)
        return if key.id.nil?

        raise "Invalid data type for #get ('#{key.type})'" unless [:sorted_set, :hash].include?(key.type)
        case key.type
        # when :sorted_set
        when :hash

        end
      end

      def get_all(key)
        return if key.id.nil?

        case key.type
        when :list, :hash
          esc_id = if key.id.is_a?(Numeric)
            key.id
          else
            "'" + key.id.gsub(/'/, "\\'").gsub(/\\/, "\\\\'") + "'"
          end

          records = Sandstorm.influxdb.query("select * from #{key.klass} where id = #{esc_id} limit 1")[key.klass]
          (records && !records.empty?) ? records.first[key.name] : nil
        when :set
          esc_id = if key.id.is_a?(Number)
            key.id
          else
            "'" + key.id.gsub(/'/, "\\'").gsub(/\\/, "\\\\'") + "'"
          end

          records = Sandstorm.influxdb.query("select #{key.name} from #{key.klass} where id = #{esc_id} limit 1")[key.klass]
          (records && !records.empty?) ? Set.new(records.first[key.name]) : nil
        # when :sorted_set
        end
      end

      def add(key, value)
        case key.type
        when :list, :set, :hash #, :sorted_set
          @steps << [:add, key, value]
        end
      end

      def lock(klasses, &block)
        # no-op
      end

      def commit_transaction
        records = {}

        @steps.each do |step|
          op    = step[0]
          key   = step[1]
          value = step[2]

          next if key.id.nil?

          records[key.klass]         ||= {}
          records[key.klass][key.id] ||= {}

          case op
          when :add
            case key.type
            when :list
              records[key.klass][key.id][key.name] = value
            when :set
              records[key.klass][key.id][key.name] = value.to_a
            # when :sorted_set
            when :hash
              records[key.klass][key.id][key.name] = value
            end
          end
        end

        records.each_pair do |klass, klass_records|
          klass_records.each_pair do |id, data|
            Sandstorm.influxdb.write_point(klass, data.merge(:id => id))
          end
        end

        super
      end

    end

  end

end