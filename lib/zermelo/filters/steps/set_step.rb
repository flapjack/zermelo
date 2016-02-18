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
              idx_class = nil
              use_sort_attr = false
              unless :id.eql?(att)
                idx_class = idx_attrs[att.to_s]
                if idx_class.nil?
                  use_sort_attr = :sorted_set.eql?(source.type) &&
                    att.eql?(associated_class.instance_variable_get('@sort_attribute'))
                  raise "'#{att}' property is not indexed" unless use_sort_attr
                end
              end

              if [Set, Array].any? {|t| value.is_a?(t) }
                conditions_set = associated_class.send(:temp_key, source.type)
                temp_keys << conditions_set
                r_conditions_set = backend.key_to_backend_key(conditions_set)

                backend.temp_key_wrap do |conditions_temp_keys|
                  if use_sort_attr
                    range_keys = value.collect {|v|
                      rl = backend.range_lookup(associated_class.ids_key, v,
                        source_type, attr_types[att], associated_class, conditions_temp_keys)
                      backend.key_to_backend_key(rl)
                    }

                    case source.type
                    when :set
                      Zermelo.redis.sunionstore(r_conditions_set, *range_keys)
                    when :sorted_set
                      Zermelo.redis.zunionstore(r_conditions_set, range_keys)
                    end
                  elsif idx_class.nil?
                    # query against the :id field
                    cond_objects, cond_ids = value.partition do |v|
                      [Zermelo::Filter, Zermelo::Associations::Multiple].any? {|c| v.is_a?(c)}
                    end

                    unless cond_objects.empty?
                      cond_keys = cond_objects.collect do |co|
                        k = case co
                        when Zermelo::Filter
                          co.send(:resolve_steps)
                        when Zermelo::Associations::Multiple
                          co.instance_variable_get('@record_ids_key')
                        end
                        backend.key_to_backend_key(k)
                      end

                      case source.type
                      when :set
                        Zermelo.redis.sunionstore(r_conditions_set, *cond_keys)
                      when :sorted_set
                        Zermelo.redis.zunionstore(r_conditions_set, cond_keys)
                      end
                    end
                    unless cond_ids.empty?
                      case source.type
                      when :set
                        s_ids = cond_ids.map {|ci| ci.is_a?(Zermelo::Associations::Singular) ? ci.id : ci }
                        Zermelo.redis.sadd(r_conditions_set, s_ids)
                      when :sorted_set
                        z_ids = cond_ids.map do |ci|
                          # is 1 a valid sort value? what's happening with it?
                          if ci.is_a?(Zermelo::Associations::Singular)
                            [1, ci.id]
                          else
                            [1, ci]
                          end
                        end
                        Zermelo.redis.zadd(r_conditions_set, z_ids)
                      end
                    end
                  else
                    index_keys = value.collect {|v|
                      il = backend.index_lookup(att, associated_class, source.type,
                        idx_class, v, attr_types[att], conditions_temp_keys)
                      backend.key_to_backend_key(il)
                    }

                    case source.type
                    when :set
                      Zermelo.redis.sunionstore(r_conditions_set, *index_keys)
                    when :sorted_set
                      Zermelo.redis.zunionstore(r_conditions_set, index_keys)
                    end
                  end
                end
                memo << conditions_set
              elsif use_sort_attr
                memo << backend.range_lookup(associated_class.ids_key, value,
                  source.type, attr_types[att], associated_class, temp_keys)
              elsif idx_class.nil?
                case value
                when Zermelo::Filter
                  ts = value.send(:resolve_steps)
                  temp_keys << ts
                  memo << ts
                when Zermelo::Associations::Multiple
                  memo << value.instance_variable_get('@record_ids_key')
                else
                  ts = associated_class.send(:temp_key, source.type)
                  temp_keys << ts
                  r_ts = backend.key_to_backend_key(ts)
                  case source.type
                  when :set
                    s_id = value.is_a?(Zermelo::Associations::Singular) ? value.id : value
                    Zermelo.redis.sadd(r_ts, s_id)
                  when :sorted_set
                    # is 1 a valid sort value? what's happening with it?
                    z_id = [1, (value.is_a?(Zermelo::Associations::Singular) ? value.id : value)]
                    Zermelo.redis.zadd(r_ts, z_id)
                  end
                  memo << ts
                end
              else
                memo << backend.index_lookup(att, associated_class, source.type,
                          idx_class, value, attr_types[att], temp_keys)
              end
            end

            r_source_key  = backend.key_to_backend_key(source)
            r_source_keys = source_keys.collect {|sk| backend.key_to_backend_key(sk) }

            op = @options[:op]
            shortcut = opts[:shortcut]

            if :ids.eql?(shortcut) && (source.type == :set)
              case op
              when :union
                backend.temp_key_wrap do |shortcut_temp_keys|
                  dest_set = associated_class.send(:temp_key, :set)
                  shortcut_temp_keys << dest_set
                  r_dest_set = backend.key_to_backend_key(dest_set)

                  Zermelo.redis.sinterstore(r_dest_set, *r_source_keys)
                  Set.new(Zermelo.redis.sunion(r_source_key, r_dest_set))
                end
              when :intersect
                Set.new(Zermelo.redis.sinter(r_source_key, *r_source_keys))
              when :diff
                backend.temp_key_wrap do |shortcut_temp_keys|
                  dest_set = associated_class.send(:temp_key, :set)
                  shortcut_temp_keys << dest_set
                  r_dest_set = backend.key_to_backend_key(dest_set)

                  Zermelo.redis.sinterstore(r_dest_set, *r_source_keys)
                  Set.new(Zermelo.redis.sdiff(r_source_key, r_dest_set))
                end
              end
            else
              dest_set = associated_class.send(:temp_key, source.type)
              r_dest_set = backend.key_to_backend_key(dest_set)
              temp_keys << dest_set

              case op
              when :union
                r_initial_key = backend.key_to_backend_key(initial_key)

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

              shortcut_params = case source.type
              when :sorted_set
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
