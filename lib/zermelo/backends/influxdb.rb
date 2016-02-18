require 'zermelo/backend'

require 'zermelo/filters/influxdb'

require 'zermelo/ordered_set'

# NB influxdb doesn't support individually addressable deletes, so
# this backend only works to write new records
# (it could just write the new state of the record, and query id by newest limit 1,
#  but for the moment, YAGNI)

module Zermelo

  module Backends

    class InfluxDB

      include Zermelo::Backend

      def key_to_backend_key(key)
        "TODO"
      end

      def filter(ids_key, associated_class, callback_target_class = nil,
                 callback_target_id = nil, callbacks = nil, sort_order = nil)

        Zermelo::Filters::InfluxDB.new(self, ids_key, associated_class,
          callback_target_class, callback_target_id, callbacks, sort_order)
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
          rescue ::InfluxDB::Error => ide
            raise unless
              /^Field #{attr_key.name} doesn't exist in series #{class_key}\/#{attr_key.id}$/ === ide.message

            records = []
          end
          value = (records && !records.empty?) ? records.first[attr_key.name.to_s] : nil

          memo[class_key] ||= {}
          memo[class_key][attr_key.id] ||= {}

          memo[class_key][attr_key.id][attr_key.name.to_s] = if value.nil?
            case attr_key.type
            when :list
              []
            when :hash
              {}
            when :set
              Set.new
            else nil
            end
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

          if records[class_key][key.id].nil?
            begin
              result = Zermelo.influxdb.query("SELECT * FROM \"#{class_key}/#{key.id}\" LIMIT 1")["#{class_key}/#{key.id}"]
              if result.nil?
                records[class_key][key.id] = {}
              else
                records[class_key][key.id] = result.first
                records[class_key][key.id].delete_if {|k,v| ["time", "sequence_number"].include?(k) }
              end
            rescue ::InfluxDB::Error => ide
              raise unless
                (/^Couldn't look up columns for series: #{class_key}\/#{key.id}$/ === ide.message) ||
                (/^Couldn't look up columns$/ === ide.message) ||
                (/^Couldn't find series: #{class_key}\/#{key.id}$/ === ide.message)

              records[class_key][key.id] = {}
            end
          end

          case op
          when :set
            records[class_key][key.id][key.name] = case key.type
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
            when :hash
              if records[class_key][key.id][key.name].nil?
                records[class_key][key.id][key.name] = value
              else
                records[class_key][key.id][key.name].update(value)
              end
            when :list
              if records[class_key][key.id][key.name].nil?
                records[class_key][key.id][key.name] = value
              else
                records[class_key][key.id][key.name] += value
              end
            when :set
              v = value.to_a
              if records[class_key][key.id][key.name].nil?
                records[class_key][key.id][key.name] = v
              else
                records[class_key][key.id][key.name] += v
              end
            when :sorted_set
              v = (1...value.size).step(2).collect {|i| value[i] }
              if records[class_key][key.id][key.name].nil?
                records[class_key][key.id][key.name] = v
              else
                records[class_key][key.id][key.name] += v
              end
            end
          when :delete
            case key.type
            when :hash
              unless records[class_key][key.id][key.name].nil?
                # FIXME
              end
            when :list
              unless records[class_key][key.id][key.name].nil?
                records[class_key][key.id][key.name] -= value
              end
            when :set
              unless records[class_key][key.id][key.name].nil?
                records[class_key][key.id][key.name] -= value.to_a
              end
            when :sorted_set
              unless records[class_key][key.id][key.name].nil?
                records[class_key][key.id][key.name] -= (1...value.size).step(2).collect {|i| value[i] }
              end
            end
          when :purge
            purges << "\"#{class_key}/#{key.id}\""
          end

        end

        records.each_pair do |class_key, klass_records|
          klass_records.each_pair do |id, data|
            data.delete('time') if data.has_key?('time') && data['time'].nil?
            Zermelo.influxdb.write_point("#{class_key}/#{id}", data.merge('id' => id))
          end
        end

        purges.each {|purge| Zermelo.influxdb.query("DROP SERIES #{purge}") }
      end

    end

  end

end