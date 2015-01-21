require 'sandstorm/version'

require 'time'

module Sandstorm

  # backport for Ruby 1.8
  unless Enumerable.instance_methods.include?(:each_with_object)
    module ::Enumerable
      def each_with_object(memo)
        return to_enum :each_with_object, memo unless block_given?
          each do |element|
            yield element, memo
          end
        memo
      end
    end
  end

  # acceptable class types, which will be normalized on a per-backend basis
  ATTRIBUTE_TYPES  = {:string     => [String],
                      :integer    => [Integer],
                      :float      => [Float],
                      :id         => [String],
                      :timestamp  => [Integer, Time, DateTime],
                      :boolean    => [TrueClass, FalseClass],
                     }

  COLLECTION_TYPES = {:list       => [Enumerable],
                      :set        => [Set],
                      :hash       => [Hash],
                      :sorted_set => [Enumerable]
                     }

  ALL_TYPES = ATTRIBUTE_TYPES.merge(COLLECTION_TYPES)

  class << self

    def valid_type?(type)
      ALL_TYPES.keys.include?(type)
    end

    # Thread and fiber-local
    [:redis, :influxdb].each do |backend|
      define_method(backend) do
        Thread.current["sandstorm_#{backend.to_s}".to_sym]
      end
    end

    def redis=(connection)
      Thread.current[:sandstorm_redis] = connection.nil? ? nil :
        Sandstorm::ConnectionProxy.new(connection)
      Thread.current[:sandstorm_redis_version] = nil
    end

    def influxdb=(connection)
      Thread.current[:sandstorm_influxdb] = Sandstorm::ConnectionProxy.new(connection)
    end

    def redis_version
      return nil if Sandstorm.redis.nil?
      rv = Thread.current[:sandstorm_redis_version]
      return rv unless rv.nil?
      Thread.current[:sandstorm_redis_version] = Sandstorm.redis.info['redis_version']
    end

    def logger=(l)
      Thread.current[:sandstorm_logger] = l
    end

    def logger
      Thread.current[:sandstorm_logger]
    end
  end

  class ConnectionProxy
    def initialize(connection)
      @proxied_connection = connection
    end

    def method_missing(name, *args, &block)
      unless Sandstorm.logger.nil?
        Sandstorm.logger.debug {
          debug_str = "#{name}"
          debug_str += " #{args.inspect}" unless args.empty?
          debug_str
        }
      end
      @proxied_connection.send(name, *args, &block)
    end
  end

end
