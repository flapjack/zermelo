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

      def add(key, value)
        change(:add, key, value)
      end

      def delete(key, value)
        change(:delete, key, value)
      end

      def clear(key)
        change(:clear, key)
      end

      def set(key, value)
        change(:set, key, value)
      end

      def purge(key)
        change(:purge, key)
      end

      # TODO get filter calling this instead of using same logic
      def exists?(key)
        return if key.id.nil?
        Sandstorm.influxdb.query("SELECT id from #{key.klass}")[key.klass].size > 0
      end

      # nb: does lots of queries, should batch, but ensuring single operations are correct
      # for now
      def get_multiple(*attr_keys)
        attr_keys.inject({}) do |memo, attr_key|

          esc_id = if attr_key.id.is_a?(Numeric)
            attr_key.id
          else
            "'" + attr_key.id.gsub(/'/, "\\'").gsub(/\\/, "\\\\'") + "'"
          end

          records = Sandstorm.influxdb.query("select #{attr_key.name} from #{attr_key.klass} where id = #{esc_id} limit 1")[attr_key.klass]
          value = (records && !records.empty?) ? records.first[attr_key.name.to_s] : nil

          memo[attr_key.klass] ||= {}
          memo[attr_key.klass][attr_key.id] ||= {}

          memo[attr_key.klass][attr_key.id][attr_key.name.to_s] = if value.nil?
            nil
          else

            case attr_key.type
            when :string
              value.to_s
            when :integer
              value.to_i
            when :float
              value.to_f
            when :timestamp
              Time.at(value.to_f)
            when :boolean
              case value
              when TrueClass
                true
              when FalseClass
                false
              when String
                'true'.eql?(value.downcase)
              else
                nil
              end
            when :list, :hash
              value
            when :set
              Set.new(value)
            end
          end
          memo
        end
      end

      def lock(*klasses)
        # no-op
      end

      def begin_transaction
        @in_transaction = true
        @changes = []
      end

      def commit_transaction
        apply_changes(@changes)
        @in_transaction = false
        @changes = []
      end

      def abort_transaction
        @in_transaction = false
        @changes = []
      end

      private

      def change(op, key, value = nil)
        ch = [op, key, value]
        if @in_transaction
          @changes << ch
        else
          apply_changes([ch])
        end
      end

      # composite all new changes into records, and then into influxdb
      # query statements
      def apply_changes(changes)
        records = {}

        changes.each do |ch|
          op    = ch[0]
          key   = ch[1]
          value = ch[2]

          next if key.id.nil?

          records[key.klass]         ||= {}
          records[key.klass][key.id] ||= {}

          unless [:set, :add].include?(op)
            raise "Record updating, deletion not supported by InfluxDB backend"
          end

          records[key.klass][key.id][key.name] = case op
          when :set
            case key.type
            when :string, :integer
              value.to_s
            when :timestamp
              value.to_f
            when :boolean
              (!!value).to_s
            when :list, :hash
              value
            when :set
              value.to_a
            end
          when :add
            case key.type
            when :list, :hash
              value
            when :set
              value.to_a
            end
          end

        end

        records.each_pair do |klass, klass_records|
          klass_records.each_pair do |id, data|
            Sandstorm.influxdb.write_point(klass, data.merge(:id => id))
          end
        end
      end

    end

  end

end