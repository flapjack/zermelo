require 'zermelo/filters/steps/base_step'

# NB: temp keys for now are bare redis keys, should be full Key objects
module Zermelo
  module Filters
    class Steps
      class EmptyStep < Zermelo::Filters::Steps::BaseStep
        def self.accepted_types
          [:set, :sorted_set, :list]
        end

        def self.returns_type
          nil # same as the source type
        end

        def resolve(backend, associated_class, opts = {})
          case backend
          when Zermelo::Backends::Redis
            source    = opts[:source]
            temp_keys = opts[:temp_keys]
            shortcut  = opts[:shortcut]

            dest_set = associated_class.send(:temp_key, source.type)
            r_dest_set = backend.key_to_backend_key(dest_set)
            temp_keys << dest_set

            return dest_set if shortcut.nil?

            shortcut_params = case source.type
            when :sorted_set
              [r_dest_set, order] + opts[:shortcut_args]
            else
              [r_dest_set] + opts[:shortcut_args]
            end

            Zermelo::Filters::Redis::SHORTCUTS[source.type][shortcut].
              call(*shortcut_params)
          when Zermelo::Backends::InfluxDB
            # FIXME
          end
        end
      end
    end
  end
end
