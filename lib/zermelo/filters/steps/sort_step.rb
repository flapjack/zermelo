require 'zermelo/filters/steps/base_step'

module Zermelo
  module Filters
    class Steps
      class SortStep < Zermelo::Filters::Steps::BaseStep
        def self.accepted_types
          [:set, :sorted_set]
        end

        def self.returns_type
          :list
        end

        def resolve(backend, associated_class, opts = {})
          case backend
          when Zermelo::Backends::Redis
            source = opts[:source]
            # idx_attrs = opts[:index_attrs]
            attr_types = opts[:attr_types]
            temp_keys = opts[:temp_keys]

            dest_list = associated_class.send(:temp_key, :list)
            temp_keys << dest_list
            r_dest_list = backend.key_to_backend_key(dest_list)

            # TODO raise error in step construction if keys not
            # passed as expected below
            sort_attrs_and_orders = case options[:keys]
            when String, Symbol
              {options[:keys].to_s => options[:desc].is_a?(TrueClass) ? :desc : :asc}
            when Array
              options[:keys].each_with_object({}) do |k, memo|
                memo[k.to_sym] = (options[:desc].is_a?(TrueClass) ? :desc : :asc)
              end
            when Hash
              options[:keys]
            end

            # TODO check if complex attribute types or associations
            # can be used for sorting

            r_source = backend.key_to_backend_key(source)

            # this set will be overwritten by the result list
            case source.type
            when :set
              Zermelo.redis.sunionstore(r_dest_list, r_source)
            when :sorted_set
              Zermelo.redis.zunionstore(r_dest_list, [r_source])
            end

            class_key = associated_class.send(:class_key)

            sort_attrs_and_orders.keys.reverse.each_with_index do |sort_attr, idx|

              order = sort_attrs_and_orders[sort_attr]

              sort_opts = {}

              unless 'id'.eql?(sort_attr.to_s)
                sort_opts.update(:by => "#{class_key}:*:attrs->#{sort_attr}")
              end

              if (idx + 1) == sort_attrs_and_orders.size
                # only apply offset & limit on the last sort
                o = options[:offset]
                l = options[:limit]

                if !(l.nil? && o.nil?)
                  o = o.nil? ? 0 : o.to_i
                  l = (l.nil? || (l.to_i < 1)) ? (Zermelo.redis.llen(dest_list) - o) : l
                  sort_opts.update(:limit => [o, l])
                end
              end

              order_parts = []
              sort_attr_type = attr_types[sort_attr.to_sym]
              unless [:integer, :float, :timestamp].include?(sort_attr_type)
                order_parts << 'alpha'
              end
              order_parts << 'desc' if 'desc'.eql?(order.to_s)

              unless order_parts.empty?
                sort_opts.update(:order => order_parts.join(' '))
              end

              sort_opts.update(:store => r_dest_list)
              Zermelo.redis.sort(r_dest_list, sort_opts)
            end

            shortcut = opts[:shortcut]

            return dest_list if shortcut.nil?
            Zermelo::Filters::Redis::SHORTCUTS[:list][shortcut].
              call(*([r_dest_list] + opts[:shortcut_args]))
          end
        end
      end
    end
  end
end
