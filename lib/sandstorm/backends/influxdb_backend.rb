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
        Sandstorm.influxdb.query("SELECT id FROM /#{key.klass}\\/.*/ LIMIT 1").size > 0
      end

      # nb: does lots of queries, should batch, but ensuring single operations are correct
      # for now
      def get_multiple(*attr_keys)
        attr_keys.inject({}) do |memo, attr_key|
          begin
          records = Sandstorm.influxdb.query("SELECT #{attr_key.name} FROM " +
            "\"#{attr_key.klass}/#{attr_key.id}\" LIMIT 1")["#{attr_key.klass}/#{attr_key.id}"]
          rescue InfluxDB::Error => ide
            raise unless
              /^Field #{attr_key.name} doesn't exist in series #{attr_key.klass}\/#{attr_key.id}$/ === ide.message

            records = []
          end
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

      def begin_transaction
        return false if @in_transaction
        @in_transaction = true
        @changes = []
      end

      def commit_transaction
        return false unless @in_transaction
        apply_changes(@changes)
        @in_transaction = false
        @changes = []
      end

      def abort_transaction
        return false unless @in_transaction
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

        purges = []

        changes.each do |ch|
          op    = ch[0]
          key   = ch[1]
          value = ch[2]

          next if key.id.nil?

          records[key.klass]         ||= {}
          records[key.klass][key.id] ||= {}

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
          when :purge
            purges << "\"#{key.klass}/#{key.id}\""
          end

        end

        records.each_pair do |klass, klass_records|
          klass_records.each_pair do |id, data|
            begin
              prior = Sandstorm.influxdb.query("SELECT * FROM \"#{klass}/#{id}\" LIMIT 1")["#{klass}/#{id}"]
            rescue InfluxDB::Error => ide
              raise unless
                (/^Couldn't look up columns for series: #{klass}\/#{id}$/ === ide.message) ||
                (/^Couldn't look up columns$/ === ide.message)

              prior = nil
            end
            record = prior.nil? ? {} : prior.first.delete_if {|k,v| ["time", "sequence_number"].include?(k) }
            Sandstorm.influxdb.write_point("#{klass}/#{id}", record.merge(data).merge('id' => id))
          end
        end

        purges.each {|purge| Sandstorm.influxdb.query("DROP SERIES #{purge}") }
      end

    end

  end

end