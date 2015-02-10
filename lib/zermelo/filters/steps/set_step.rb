require 'zermelo/filters/steps/base_step'

# NB: temp keys for now are bare redis keys, should be full Key objects
module Zermelo
  module Filters
    class Steps
      class SetStep < Zermelo::Filters::Steps::BaseStep
        def self.accepted_types
          [:set, :sorted_set] # TODO should allow :list as well
        end

        def self.returns_type
          :set
        end

        def resolve(backend, associated_class, opts = {})

          case backend
          when Zermelo::Backends::RedisBackend
            source = opts[:source]
            idx_attrs = opts[:index_attrs]
            attr_types = opts[:attr_types]
            temp_keys = opts[:temp_keys]

            source_keys = @attributes.inject([]) do |memo, (att, value)|

              val = value.is_a?(Set) ? value.to_a : value

              if :id.eql?(att)
                ts = associated_class.send(:temp_key, :set)
                temp_keys << ts
                Zermelo.redis.sadd(backend.key_to_redis_key(ts), val)
                memo << ts
              else
                idx_class = idx_attrs[att.to_s]
                raise "'#{att}' property is not indexed" if idx_class.nil?

                if val.is_a?(Enumerable)
                  conditions_set = associated_class.send(:temp_key, :set)
                  r_conditions_set = backend.key_to_redis_key(conditions_set)

                  backend.temp_key_wrap do |conditions_temp_keys|
                    index_keys = val.collect {|v|
                      il = backend.index_lookup(att, associated_class,
                        idx_class, v, attr_types[att], conditions_temp_keys)
                      backend.key_to_redis_key(il)
                    }

                    Zermelo.redis.sunionstore(r_conditions_set, *index_keys)
                  end
                  memo << conditions_set
                else
                  memo << backend.index_lookup(att, associated_class,
                            idx_class, val, attr_types[att], temp_keys)
                end
              end

              memo
            end

            case source.type
            when :sorted_set
              Zermelo::Filters::Steps::SortedSetStep.evaluate(backend,
                @options[:op], associated_class, source, source_keys, temp_keys, opts)
            when :set
              self.class.evaluate(backend, @options[:op], associated_class,
                source, source_keys, temp_keys, opts)
            end

          when Zermelo::Backends::InfluxDBBackend
            query = ''

            unless opts[:first].is_a?(TrueClass)
              case @options[:op]
              when :intersect, :diff
                query += ' AND '
              when :union
                query += ' OR '
              end
            end

            case @options[:op]
            when :intersect, :union
              query += @attributes.collect {|k, v|
                op, value = case v
                when String
                  ["=~", "/^#{Regexp.escape(v).gsub(/\\\\/, "\\")}$/"]
                else
                  ["=",  "'#{v}'"]
                end

               "#{k} #{op} #{value}"
              }.join(' AND ')

            when :diff
              query += @attributes.collect {|k, v|
                op, value = case v
                when String
                  ["!~", "/^#{Regexp.escape(v).gsub(/\\\\/, "\\")}$/"]
                else
                  ["!=",  "'#{v}'"]
                end

                "#{k} #{op} #{value}"
              }.join(' AND ')
            else
              raise "Unhandled filter operation '#{@options[:op]}'"
            end

            query += ")"

            query
          end
        end

        def self.evaluate(backend, op, associated_class, source, source_keys, temp_keys, opts = {})
          shortcuts = opts[:shortcuts]

          last_step_and_smembers = !shortcuts.nil? && :smembers.eql?(shortcuts[:set])

          r_source_key  = backend.key_to_redis_key(source)
          r_source_keys = source_keys.collect {|sk| backend.key_to_redis_key(sk) }

          if last_step_and_smembers
            case op
            when :union
              backend.temp_key_wrap do |shortcut_temp_keys|
                dest_set = associated_class.send(:temp_key, :set)
                shortcut_temp_keys << dest_set
                r_dest_set = backend.key_to_redis_key(dest_set)

                Zermelo.redis.sinterstore(r_dest_set, *r_source_keys)
                Zermelo.redis.sunion(r_dest_set, r_source_key)
              end
            when :intersect
              Zermelo.redis.sinter(r_source_key, *r_source_keys)
            when :diff
              backend.temp_key_wrap do |shortcut_temp_keys|
                dest_set = associated_class.send(:temp_key, :set)
                shortcut_temp_keys << dest_set
                r_dest_set = backend.key_to_redis_key(dest_set)

                Zermelo.redis.sinterstore(r_dest_set, *r_source_keys)
                Zermelo.redis.sdiff(r_source_key, r_dest_set)
              end
            end
          else
            dest_set = associated_class.send(:temp_key, :set)
            r_dest_set = backend.key_to_redis_key(dest_set)
            temp_keys << dest_set

            case op
            when :union
              Zermelo.redis.sinterstore(r_dest_set, *r_source_keys)
              Zermelo.redis.sunionstore(r_dest_set, r_source_key, r_dest_set)
            when :intersect
              Zermelo.redis.sinterstore(r_dest_set, *r_source_keys)
            when :diff
              Zermelo.redis.sinterstore(r_dest_set, *r_source_keys)
              Zermelo.redis.sdiffstore(r_dest_set, r_source_key, r_dest_set)
            end

            dest_set
          end

        end

      end
    end
  end
end
