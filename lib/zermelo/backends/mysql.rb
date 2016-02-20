require 'zermelo/backend'

require 'zermelo/filters/mysql'
require 'zermelo/ordered_set'
# require 'zermelo/locks/mysql_lock'

module Zermelo

  module Backends

    class MySQL

      include Zermelo::Backend

      def initialize
        @transaction_mysql = nil
        @changes = nil
      end

      def key_to_backend_key(key)
        key.klass.send(:class_key)
      end

      def filter(ids_key, associated_class, callback_target_class = nil,
        callback_target_id = nil, callbacks = nil, sort_order = nil)

        Zermelo::Filters::MySQL.new(self, ids_key, associated_class,
                                    callback_target_class, callback_target_id,
                                    callbacks, sort_order)
      end

      # TODO this is clunky and slow, optimize later
      def get_multiple(*attr_keys)
        ids_by_class = attr_keys.inject({}) do |memo, attr_key|
          class_key = attr_key.klass.send(:class_key)

          memo[class_key] ||= Set.new
          memo[class_key] << attr_key.id
          memo
        end

        ids_by_class.inject({}) do |memo, (key, ids)|
          escaped_key = Zermelo.mysql.escape(key)
          select_sql = %Q[
SELECT *
FROM `#{escaped_key}`
WHERE `#{escaped_key}`.`id` IN (#{(['?'] * ids.size).join(', ')})
]

          select_stmt = Zermelo.mysql.prepare(select_sql)
          select_results = select_stmt.execute(*ids.to_a.map {|i| Zermelo.mysql.escape(i)})

          select_results.each do |row|

            memo[key] ||= {}
            memo[key][row['id']] = row
            memo[key][row['id']].delete('_id') # delete internal id
          end

          select_stmt.close
          memo
        end
      end

      def begin_transaction
        return false unless @transaction_mysql.nil?
        @transaction_mysql = Zermelo.mysql
        @transaction_mysql.query('BEGIN')
        @changes = []
        true
      end

      def commit_transaction
        return false if @transaction_mysql.nil?
        apply_changes(@changes)
        @transaction_mysql.query('COMMIT')
        @transaction_mysql = nil
        @changes = []
        true
      end

      def abort_transaction
        return false if @transaction_mysql.nil?
        @changes = []
        @transaction_mysql.query('ABORT')
        @transaction_mysql = nil
        true
      end

  #     def temp_key_wrap
  #       return unless block_given?
  #       temp_keys = []
  #       begin
  #         yield(temp_keys)
  #       rescue
  #         raise
  #       ensure
  #         unless temp_keys.empty?
  #           Zermelo.redis.del(*temp_keys.collect {|tk| key_to_backend_key(tk)})
  #           temp_keys.clear
  #         end
  #       end
  #     end

  #     def index_lookup(att, associated_class, type, idx_class, value,
  #       attr_type, temp_keys)

  #       if (idx_class == Zermelo::Associations::RangeIndex) && !value.is_a?(Zermelo::Filters::IndexRange)
  #         raise "Range index must be passed a range"
  #       end

  #       case value
  #       when Regexp
  #         raise "Can't query non-string values via regexp" unless :string.eql?(attr_type)

  #         idx_key = associated_class.send(:temp_key, type)
  #         temp_keys << idx_key
  #         idx_result = key_to_backend_key(idx_key)

  #         starts_with_string_re = /^string:/
  #         case idx_class.name
  #         when 'Zermelo::Associations::UniqueIndex'
  #           index_key = key_to_backend_key(Zermelo::Records::Key.new(
  #             :klass  => associated_class,
  #             :name   => "by_#{att}",
  #             :type   => :hash,
  #             :object => :index
  #           ))
  #           candidates = Zermelo.redis.hgetall(index_key)
  #           matching_ids = candidates.values_at(*candidates.keys.select {|k|
  #             (starts_with_string_re === k) &&
  #               (value === unescape_key_name(k.sub(starts_with_string_re, '')))
  #           })

  #           unless matching_ids.empty?
  #             case type
  #             when :set
  #               Zermelo.redis.sadd(idx_result, matching_ids)
  #             when :sorted_set
  #               Zermelo.redis.zadd(idx_result, matching_ids.map {|m| [1, m]})
  #             end
  #           end
  #         when 'Zermelo::Associations::Index'
  #           key_root = key_to_backend_key(Zermelo::Records::Key.new(
  #             :klass  => associated_class,
  #             :name   => "by_#{att}:string",
  #             :type   => :set,
  #             :object => :index
  #           ))

  #           key_pat = "#{key_root}:?*"

  #           matching_sets = if (Zermelo.redis_version.split('.') <=> ['2', '8', '0']) == 1
  #             # lock will be subsumed by outer lock if present -- required to
  #             # know that scan is getting consistent results
  #             associated_class.lock do
  #               Zermelo.redis.scan_each(:match => key_pat).to_a
  #             end
  #           else
  #             # SCAN is only supported in Redis >= 2.8.0
  #             Zermelo.redis.keys(key_pat)
  #           end

  #           matching_sets.select! do |k|
  #             k =~ /^#{key_root}:(.+)$/
  #             value === $1
  #           end

  #           unless matching_sets.empty?
  #             case type
  #             when :set
  #               Zermelo.redis.sunionstore(idx_result, matching_sets)
  #             when :sorted_set
  #               Zermelo.redis.zunionstore(idx_result, matching_sets)
  #             end
  #           end
  #         end
  #         idx_key
  #       else
  #         index = associated_class.send("#{att}_index")

  #         case index
  #         when Zermelo::Associations::RangeIndex
  #           range_lookup(index.key, value, type, attr_type, associated_class, temp_keys)
  #         when Zermelo::Associations::UniqueIndex
  #           idx_key = associated_class.send(:temp_key, type)
  #           temp_keys << idx_key

  #           val = Zermelo.redis.hget(key_to_backend_key(index.key),
  #                              index_keys(attr_type, value).join(':'))

  #           case type
  #           when :set
  #             Zermelo.redis.sadd(key_to_backend_key(idx_key), val)
  #           when :sorted_set
  #             Zermelo.redis.zadd(key_to_backend_key(idx_key), [1, val])
  #           end
  #           idx_key
  #         when Zermelo::Associations::Index
  #           index.key(value)
  #         end
  #       end
  #     end

  #     def range_lookup(key, range, type, attr_type, associated_class, temp_keys)
  #       r_key = key_to_backend_key(key)
  #       opts = case type
  #       when :set
  #         {}
  #       when :sorted_set
  #         {:with_scores => true}
  #       end
  #       result = if range.by_score
  #         range_start  = range.start.nil?  ? '-inf' : safe_value(attr_type, range.start)
  #         range_finish = range.finish.nil? ? '+inf' : safe_value(attr_type, range.finish)
  #         Zermelo.redis.zrangebyscore(r_key, range_start, range_finish, opts)
  #       else
  #         range_start  = range.start  ||  0
  #         range_finish = range.finish || -1
  #         Zermelo.redis.zrange(r_key, range_start, range_finish, opts)
  #       end

  #       # TODO another way for index_lookup to indicate 'empty result', rather
  #       # than creating & returning an empty key
  #       ret_key = associated_class.send(:temp_key, key.type)
  #       temp_keys << ret_key
  #       unless result.empty?
  #         r_key = key_to_backend_key(ret_key)
  #         case type
  #         when :set
  #           Zermelo.redis.sadd(r_key, result)
  #         when :sorted_set
  #           Zermelo.redis.zadd(r_key, result.map {|r| [r.last, r.first]})
  #         end
  #       end
  #       ret_key
  #     end

      private

      def change(op, key, value = nil, key_to = nil, value_to = nil)
        ch = [op, key, value, key_to, value_to]
        if @transaction_mysql.nil?
          apply_changes([ch])
          return
        end
        @changes << ch
      end

      def apply_changes(changes)
        simple_attrs  = {}

        purges = {}

        changes.each do |ch|
          op       = ch[0]
          key      = ch[1]
          value    = ch[2]
          key_to   = ch[3]
          value_to = ch[4]

          # TODO check that collection types handle nil value for whole thing
          if Zermelo::COLLECTION_TYPES.has_key?(key.type)

  #           complex_attr_key = key_to_backend_key(key)

  #           case op
  #           when :add, :set
  #             case key.type
  #             when :list
  #               Zermelo.redis.del(complex_attr_key) if :set.eql?(op)
  #               Zermelo.redis.rpush(complex_attr_key, value)
  #             when :set
  #               Zermelo.redis.del(complex_attr_key) if :set.eql?(op)
  #               case value
  #               when Set
  #                 Zermelo.redis.sadd(complex_attr_key, value.to_a) unless value.empty?
  #               when Array
  #                 Zermelo.redis.sadd(complex_attr_key, value) unless value.empty?
  #               else
  #                 Zermelo.redis.sadd(complex_attr_key, value)
  #               end
  #             when :hash
  #               Zermelo.redis.del(complex_attr_key) if :set.eql?(op)
  #               unless value.nil?
  #                 kv = value.inject([]) do |memo, (k, v)|
  #                   memo += [k, v]
  #                   memo
  #                 end
  #                 Zermelo.redis.hmset(complex_attr_key, *kv)
  #               end
  #             when :sorted_set
  #               Zermelo.redis.zadd(complex_attr_key, value)
  #             end
  #           when :move
  #             case key.type
  #             when :set
  #               Zermelo.redis.smove(complex_attr_key, key_to_backend_key(key_to), value)
  #             when :list
  #               # TODO via sort 'nosort', or the workaround required prior to
  #               # https://github.com/antirez/redis/issues/2079
  #               raise "Not yet implemented"
  #             when :hash
  #               Zermelo.redis.hdel(complex_attr_key, *value.keys)
  #               Zermelo.redis.hset(key_to_backend_key(key_to), *value_to.to_a.flatten)
  #             when :sorted_set
  #               Zermelo.redis.zadd(complex_attr_key, value_to)
  #             end
  #           when :delete
  #             case key.type
  #             when :list
  #               Zermelo.redis.lrem(complex_attr_key, value, 0)
  #             when :set
  #               Zermelo.redis.srem(complex_attr_key, value)
  #             when :hash
  #               Zermelo.redis.hdel(complex_attr_key, value)
  #             when :sorted_set
  #               Zermelo.redis.zrem(complex_attr_key, value)
  #             end
  #           when :clear
  #             Zermelo.redis.del(complex_attr_key)
  #           end

          elsif :purge.eql?(op)
            ck = key.klass.send(:class_key)
            purges[ck] ||= Set.new
            purges[ck] << key.id
          else
            simple_attr_key = key_to_backend_key(key)
            simple_attrs[simple_attr_key] ||= {'id' => key.id}

            case op
            when :set
              simple_attrs[simple_attr_key][key.name] = if value.nil?
                nil
              else
                case key.type
                when :string, :integer
                  value.to_s
                when :float, :timestamp
                  value.to_f
                when :boolean
                  (!!value).to_s
                end
              end
            when :clear
              simple_attrs[simple_attr_key][key.name] = nil
            end
          end
        end

        unless simple_attrs.empty?
          update_stmts = []
          simple_attrs.each_pair do |simple_attr_key, values|
            id = values.delete('id')
            value_set = values.keys.map {|k| "`#{k}` = ?" }.join(', ')

            # FIXME escape everything properly
            count_sql = %Q[
SELECT COUNT(*)
FROM `#{simple_attr_key}`
WHERE `#{simple_attr_key}`.`id` = ?
]

            count_stmt = Zermelo.mysql.prepare(count_sql)
            count_result = count_stmt.execute(id)
            record_exists = count_result.first['COUNT(*)'] > 0
            count_stmt.close

            if record_exists

              # FIXME escape everything properly
              update_sql = %Q[
UPDATE `#{simple_attr_key}`
SET #{value_set}
WHERE `#{simple_attr_key}`.`id` = "#{id}"
]

              update_stmt = Zermelo.mysql.prepare(update_sql)
              update_stmt.execute(*values.values)
              update_stmt.close
            else

              # FIXME escape everything properly
              insert_sql = %Q[
INSERT INTO `#{simple_attr_key}` (id, #{values.keys.join(', ')})
VALUES (?, #{(['?'] * values.size).join(', ')})
]

              insert_stmt = Zermelo.mysql.prepare(insert_sql)
              insert_stmt.execute(id, *values.values)
              insert_stmt.close
            end
          end
        end

        purges.each_pair do |ck, ids|
          escaped_ck = Zermelo.mysql.escape(ck)
          delete_sql = %Q[
DELETE
FROM `#{escaped_ck}`
WHERE `#{escaped_ck}`.`id` IN (#{(['?'] * ids.size).join(', ')})
]

          delete_stmt = Zermelo.mysql.prepare(delete_sql)
          delete_stmt.execute(*ids.to_a.map {|i| Zermelo.mysql.escape(i)})
          delete_stmt.close
        end
      end

    end

  end

end