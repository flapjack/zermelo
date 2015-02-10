require 'zermelo/filters/steps/base_step'

module Zermelo
  module Filters
    class Steps
      class PageStep < Zermelo::Filters::Steps::BaseStep
        def self.accepted_types
          [:list]
        end

        def self.returns_type
          :list
        end

        def resolve(backend, associated_class, source, idx_attrs, attr_types,
          temp_keys, opts = {})

          offset = @options[:offset]
          limit = @options[:limit]

          o = offset.to_i
          l = limit.to_i

          case backend
          when Zermelo::Backends::RedisBackend

          # TODO apply these transformations via a subset?
          # TODO need a guaranteed non-existing key for non-sorting 'sort'

            # TODO check if source is in temp_keys, use a generated temp_key instead if not
            r_source = backend.key_to_redis_key(source)

            l = (Zermelo.redis.llen(r_source) - o) if (l < 1)

            opts = {:by => 'no_sort', :limit => [o, l]}

            # https://github.com/antirez/redis/issues/2079, fixed in redis 2.8.19
            if (Zermelo.redis_version.split('.') <=> ['2', '8', '18']) == 1
              opts.update(:store => r_source)
              Zermelo.redis.sort(r_source, opts)
              source
            else
              data = Zermelo.redis.sort(r_source, opts)

              if data.empty?
                # TODO fix
              else
                limited = associated_class.send(:temp_key, :list)
                temp_keys << limited

                Zermelo.redis.rpush(backend.key_to_redis_key(limited), data)

                limited
              end
            end

          end

        end

      end

    end
  end
end
