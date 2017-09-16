require 'zermelo/filters/steps/base_step'

module Zermelo
  module Filters
    class Steps
      class SortStep < Zermelo::Filters::Steps::BaseStep
        def self.accepted_types
          %i[set sorted_set]
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

            sort_attrs_and_orders = attrs_and_orders(keys: options[:keys], desc: options[:desc])

            # TODO: check if complex attribute types or associations
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
              sort_opts = { store: r_dest_list }

              sort_opts.update(options_order(sort_attr_type: attr_types[sort_attr.to_sym],
                                             order: sort_attrs_and_orders[sort_attr]))

              unless 'id'.eql?(sort_attr.to_s)
                sort_opts.update(by: "#{class_key}:*:attrs->#{sort_attr}")
              end

              if (idx + 1) == sort_attrs_and_orders.size
                sort_opts.update(options_position(dest_list: dest_list,
                                                  offset: options[:offset],
                                                  limit: options[:limit]))
              end

              Zermelo.redis.sort(r_dest_list, sort_opts)
            end

            shortcut = opts[:shortcut]

            return dest_list if shortcut.nil?
            Zermelo::Filters::Redis::SHORTCUTS[:list][shortcut].
              call(*([r_dest_list] + opts[:shortcut_args]))
          end
        end

        private

          # TODO: raise error in step construction if keys not
          # passed as expected below
          def attrs_and_orders(keys:, desc:)
            case keys
            when String, Symbol
              { keys.to_s => desc.is_a?(TrueClass) ? :desc : :asc }
            when Array
              keys.each_with_object({}) do |k, memo|
                memo[k.to_sym] = (desc.is_a?(TrueClass) ? :desc : :asc)
              end
            when Hash
              keys
            end
          end

          def options_position(dest_list:, offset:, limit:)
            # only apply offset & limit on the last sort
            return {} if limit.nil? && offset.nil?
            offset = offset.nil? ? 0 : offset.to_i
            limit = limit.nil? || (limit.to_i < 1) ? (Zermelo.redis.llen(dest_list) - offset) : limit
            { limit: [offset, limit] }
          end

          def options_order(sort_attr_type:, order:)
            order_parts = []
            order_parts << 'alpha' unless %i[integer float timestamp].include?(sort_attr_type)
            order_parts << 'desc' if 'desc'.eql?(order.to_s)

            return {} if order_parts.empty?
            { order: order_parts.join(' ') }
          end
      end
    end
  end
end
