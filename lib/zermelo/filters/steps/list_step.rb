require 'zermelo/filters/steps/base_step'

module Zermelo
  module Filters
    class Steps
      class ListStep < Zermelo::Filters::Steps::BaseStep
        def self.accepted_types
          [:sorted_set, :list]
        end

        def self.returns_type
          nil # same as the source type
        end

        def resolve(backend, associated_class, opts = {})
          shortcut = opts[:shortcut]

          offset = @options[:offset]
          limit = @options[:limit]

          o = offset.to_i
          l = limit.to_i

          case backend
          when Zermelo::Backends::Redis

            source = opts[:source]
            # idx_attrs = opts[:index_attrs]
            # attr_types = opts[:attr_types]
            temp_keys = opts[:temp_keys]

            # TODO apply these transformations via a subset?
            # TODO need a guaranteed non-existing key for non-sorting 'sort'

            # TODO check if source is in temp_keys, use a generated temp_key instead if not
            r_source = backend.key_to_backend_key(source)

            result, r_result = case source.type
            when :sorted_set
              limited = associated_class.send(:temp_key, :sorted_set)
              temp_keys << limited
              r_limited = backend.key_to_backend_key(limited)

              lim = Zermelo.redis.send(
                :desc.eql?(opts[:sort_order]) ? :zrevrange : :zrange,
                r_source, o, (o + l), :with_scores => true
              )

              Zermelo.redis.zadd(r_limited, lim.collect {|li| [li[1], li[0]]} )

              [limited, r_limited]
            when :list

              l = (Zermelo.redis.llen(r_source) - o) if (l < 1)

              sort_opts = {:by => 'no_sort', :limit => [o, l]}

              # https://github.com/antirez/redis/issues/2079, fixed in redis 2.8.19
              result, r_result = if (Zermelo.redis_version.split('.') <=> ['2', '8', '18']) == 1
                sort_opts.update(:store => r_source)
                Zermelo.redis.sort(r_source, sort_opts)
                [source, r_source]
              else
                data = Zermelo.redis.sort(r_source, sort_opts)

                if data.empty?
                  # TODO fix
                else
                  limited = associated_class.send(:temp_key, :list)
                  temp_keys << limited
                  r_limited = backend.key_to_backend_key(limited)

                  Zermelo.redis.rpush(r_limited, data)

                  [limited, r_limited]
                end
              end
            end

            return result if shortcut.nil?

            extra_args = case source.type
            when :sorted_set
              [opts[:sort_order]]
            when :list
              []
            end

            Zermelo::Filters::Redis::SHORTCUTS[source.type][shortcut].
              call(*([r_result] + extra_args + opts[:shortcut_args]))
          end
        end
      end

    end
  end
end
