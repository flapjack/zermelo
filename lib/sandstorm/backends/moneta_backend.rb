require 'sandstorm/backends/base'

require 'sandstorm/filters/moneta_filter'

module Sandstorm

  module Backends

    class MonetaBackend

      include Sandstorm::Backends::Base

      # TODO implement all sets, lists, sorted sets as specific
      # keys under, e.g. 'sandstorm::sets::[name]', 'sandstorm::lists::[name]'
      # TODO will need to store attribute definitions too

      def filter(ids_key, record)
        Sandstorm::Filters::MonetaFilter.new(self, ids_key, record)
      end

      # def hash_set_values(hash_name, keys_values)
      #   keys_values.each_pair do |k, v|
      #     Sandstorm.moneta["sandstorm:hashes:#{hash_name}:#{k}"] = v
      #   end
      # end

      # def hash_get_value(hash_name, key)
      #   Sandstorm.moneta["sandstorm:hashes:#{hash_name}:#{k}"]
      # end

      # def hash_delete_values(hash_name, *keys)
      #   keys.each do |k|
      #     Sandstorm.moneta.delete("sandstorm:hashes:#{hash_name}:#{k}")
      #   end
      # end

      # only relevant for :sorted_set
      def get_at(key, index)
        raise "Invalid data type for #get_at ('#{key.type})'" unless :sorted_set.eql?(key.type)
        # TODO
      end

      # only relevant for :sorted_set (e.g. redis by_score) & :hash
      def get(key, value)
        raise "Invalid data type for #get ('#{key.type})'" unless [:sorted_set, :hash].include?(key.type)
      end

      def get_all(key)
        case key.type
        when :list
        when :set
        # when :sorted_set
        when :hash
        end
      end

      def set(key, value)
        case key.type
        when :list
        when :set
        # when :sorted_set
        when :hash
        end
      end

      def item_exists?(record_name)
        raise "Not yet implemented"
      end

      def lock(klasses, &block)
        # no-op
      end

      def commit_transaction
        puts "TODO save"
        p @steps

        @steps.each do |step|
          op  = step[0]
          key = step[1]
          case key.type
          when :list
            case op
            when :add
            when :delete
            when :clear
            end
          when :set
            case op
            when :add
            when :delete
            when :clear
            end
          # when :sorted_set
            case op
            when :add
            when :delete
            when :clear
            end
          when :hash
            case op
            when :add
            when :delete
            when :clear
            end
          end
        end

        super
      end

    end

  end

end