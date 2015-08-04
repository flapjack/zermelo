require 'zermelo/filters/steps/base_step'

# NB: temp keys for now are bare redis keys, should be full Key objects
module Zermelo
  module Filters
    class Steps
      class SetStep < Zermelo::Filters::Steps::BaseStep
        def self.accepted_types
          [:set, :sorted_set] # TODO should allow :list as well?
        end

        def self.returns_type
          nil # same as the source type
        end

        def resolve(backend, associated_class, opts = {})

          case backend
          when Zermelo::Backends::Redis
            initial_key = opts[:initial_key]
            source      = opts[:source]
            idx_attrs   = opts[:index_attrs]
            attr_types  = opts[:attr_types]
            temp_keys   = opts[:temp_keys]
            order       = opts[:sort_order]

            source_keys = @attributes.each_with_object([]) do |(att, value), memo|

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
                  conditions_set = associated_class.send(:temp_key, source.type)
                  temp_keys << conditions_set
                  r_conditions_set = backend.key_to_redis_key(conditions_set)

                  backend.temp_key_wrap do |conditions_temp_keys|
                    index_keys = val.collect {|v|
                      il = backend.index_lookup(att, associated_class,
                        idx_class, v, attr_types[att], conditions_temp_keys)
                      backend.key_to_redis_key(il)
                    }

                    case source.type
                    when :set
                      Zermelo.redis.sunionstore(r_conditions_set, *index_keys)
                    when :sorted_set
                      Zermelo.redis.zunionstore(r_conditions_set, index_keys)
                    end
                  end
                  memo << conditions_set
                else
                  memo << backend.index_lookup(att, associated_class,
                            idx_class, val, attr_types[att], temp_keys)
                end
              end
            end

            r_source_key  = backend.key_to_redis_key(source)
            r_source_keys = source_keys.collect {|sk| backend.key_to_redis_key(sk) }

            op = @options[:op]
            shortcut = opts[:shortcut]

            if :ids.eql?(shortcut) && (source.type == :set)
              case op
              when :union
                backend.temp_key_wrap do |shortcut_temp_keys|
                  dest_set = associated_class.send(:temp_key, :set)
                  shortcut_temp_keys << dest_set
                  r_dest_set = backend.key_to_redis_key(dest_set)

                  Zermelo.redis.sinterstore(r_dest_set, *r_source_keys)
                  Zermelo.redis.sunion(r_source_key, r_dest_set)
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
              dest_set = associated_class.send(:temp_key, source.type)
              r_dest_set = backend.key_to_redis_key(dest_set)
              temp_keys << dest_set

              case op
              when :union
                r_initial_key = backend.key_to_redis_key(initial_key)

                if source.type == :sorted_set
                  Zermelo.redis.zinterstore(r_dest_set,
                    [r_initial_key] + r_source_keys,
                    :weights => [1.0] + ([0.0] * source_keys.length), :aggregate => 'max')

                  Zermelo.redis.zunionstore(r_dest_set, [r_source_key, r_dest_set], :aggregate => 'max')
                else
                  Zermelo.redis.sinterstore(r_dest_set, r_initial_key, *r_source_keys)
                  Zermelo.redis.sunionstore(r_dest_set, r_dest_set, r_source_key)
                end
              when :intersect
                if source.type == :sorted_set
                  Zermelo.redis.zinterstore(r_dest_set, [r_source_key] + r_source_keys, :aggregate => 'max')
                else
                  Zermelo.redis.sinterstore(r_dest_set, r_source_key, *r_source_keys)
                end
              when :diff
                if source.type == :sorted_set
                  Zermelo.redis.zinterstore(r_dest_set, r_source_keys, :aggregate => 'max')
                  Zermelo.redis.zunionstore(r_dest_set, [r_source_key, r_dest_set], :weights => [1.0, 0.0], :aggregate => 'min')
                  Zermelo.redis.zremrangebyscore(r_dest_set, "0", "0")
                else
                  Zermelo.redis.sinterstore(r_dest_set, *r_source_keys)
                  Zermelo.redis.sdiffstore(r_dest_set, r_dest_set, r_source_key)
                end
              end

              return dest_set if shortcut.nil?

              shortcut_params = if source.type == :sorted_set
                [r_dest_set, order] + opts[:shortcut_args]
              else
                [r_dest_set] + opts[:shortcut_args]
              end

              Zermelo::Filters::Redis::SHORTCUTS[source.type][shortcut].
                call(*shortcut_params)
            end

          when Zermelo::Backends::InfluxDB
            query = ''

            attr_types = opts[:attr_types]

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

                attr_type = attr_types[k]

                if v.is_a?(Enumerable)
                  qq = v.each_with_object([]) do |vv, memo|
                    ov = case vv
                    when Regexp
                      raise "Can't query non-string values via regexp" unless :string.eql?(attr_type)
                      "=~ /#{vv.source.gsub(/\\\\/, "\\")}/"
                    when String
                      "=~ /^#{Regexp.escape(vv).gsub(/\\\\/, "\\")}$/"
                    else
                      "= '#{vv}'"
                    end
                    memo << "#{k} #{ov}"
                  end
                  "((#{qq.join(') OR (')}))"
                else
                  op_value = case v
                  when Regexp
                    raise "Can't query non-string values via regexp" unless :string.eql?(attr_type)
                    "=~ /#{v.source.gsub(/\\\\/, "\\")}/"
                  when String
                    "=~ /^#{Regexp.escape(v).gsub(/\\\\/, "\\")}$/"
                  else
                    "= '#{v}'"
                  end
                  "(#{k} #{op_value})"
                end
              }.join(' AND ')

            when :diff
              query += @attributes.collect {|k, v|
                if v.is_a?(Enumerable)
                  qq = v.each_with_object([]) do |vv, memo|
                    ov = case vv
                    when Regexp
                      raise "Can't query non-string values via regexp" unless :string.eql?(attr_type)
                      "!~ /#{vv.source.gsub(/\\\\/, "\\")}/"
                    when String
                      "!~ /^#{Regexp.escape(vv).gsub(/\\\\/, "\\")}$/"
                    else
                      "<> '#{vv}'"
                    end
                    memo << "#{k} #{ov}"
                  end
                  "((#{qq.join(') OR (')}))"
                else
                  op_value = case v
                  when Regexp
                    raise "Can't query non-string values via regexp" unless :string.eql?(attr_type)
                    "!~ /#{v.source.gsub(/\\\\/, "\\")}/"
                  when String
                    "!~ /^#{Regexp.escape(v).gsub(/\\\\/, "\\")}$/"
                  else
                    "<> '#{v}'"
                  end

                  "(#{k} #{op_value})"
                end
              }.join(' AND ')

            else
              raise "Unhandled filter operation '#{@options[:op]}'"
            end

            query += ")"

            query
          end
        end

      end
    end
  end
end
