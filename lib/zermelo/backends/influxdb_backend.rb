require 'zermelo/backends/base'

require 'zermelo/filters/influxdb_filter'

# NB influxdb doesn't support individually addressable deletes, so
# this backend only works to write new records
# (it could just write the new state of the record, and query id by newest limit 1,
#  but for the moment, YAGNI)

module Zermelo

  module Backends

    class InfluxDBBackend

      include Zermelo::Backends::Base

      def default_sorted_set_key
        :time
      end

      def filter(ids_key, record, callback_target = nil, callbacks = nil)
        Zermelo::Filters::InfluxDBFilter.new(self, ids_key, record, callback_target, callbacks)
      end

      # TODO get filter calling this instead of using same logic
      def exists?(key)
        return if key.id.nil?
        class_key = attr_key.klass.send(:class_key)
        Zermelo.influxdb.query("SELECT id FROM /#{class_key}\\/.*/ LIMIT 1").size > 0
      end

      # nb: does lots of queries, should batch, but ensuring single operations are correct
      # for now
      def get_multiple(*attr_keys)
        attr_keys.inject({}) do |memo, attr_key|
          class_key = attr_key.klass.send(:class_key)
          begin
          records = Zermelo.influxdb.query("SELECT #{attr_key.name} FROM " +
            "\"#{class_key}/#{attr_key.id}\" LIMIT 1")["#{class_key}/#{attr_key.id}"]
          rescue InfluxDB::Error => ide
            raise unless
              /^Field #{attr_key.name} doesn't exist in series #{class_key}\/#{attr_key.id}$/ === ide.message

            records = []
          end
          value = (records && !records.empty?) ? records.first[attr_key.name.to_s] : nil

          memo[class_key] ||= {}
          memo[class_key][attr_key.id] ||= {}

          memo[class_key][attr_key.id][attr_key.name.to_s] = if value.nil?
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

          class_key = key.klass.send(:class_key)

          records[class_key]         ||= {}
          records[class_key][key.id] ||= {}

          records[class_key][key.id][key.name] = case op
          when :set
            case key.type
            when :string, :integer
              value.nil? ? nil : value.to_s
            when :timestamp
              value.nil? ? nil : value.to_f
            when :boolean
              value.nil? ? nil : (!!value).to_s
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
            when :sorted_set
              (1...value.size).step(2).collect {|i| value[i] }
            end
          when :purge
            purges << "\"#{class_key}/#{key.id}\""
          end

        end

        records.each_pair do |class_key, klass_records|
          klass_records.each_pair do |id, data|
            begin
              prior = Zermelo.influxdb.query("SELECT * FROM \"#{class_key}/#{id}\" LIMIT 1")["#{class_key}/#{id}"]
            rescue InfluxDB::Error => ide
              raise unless
                (/^Couldn't look up columns for series: #{class_key}\/#{id}$/ === ide.message) ||
                (/^Couldn't look up columns$/ === ide.message) ||
                (/^Couldn't find series: #{class_key}\/#{id}$/ === ide.message)

              prior = nil
            end
            record = prior.nil? ? {} : prior.first.delete_if {|k,v| ["time", "sequence_number"].include?(k) }
            data.delete('time') if data.has_key?('time') && data['time'].nil?
            Zermelo.influxdb.write_point("#{class_key}/#{id}", record.merge(data).merge('id' => id))
          end
        end

        purges.each {|purge| Zermelo.influxdb.query("DROP SERIES #{purge}") }
      end

    end

  end

end