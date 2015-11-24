require 'date'
require 'set'
require 'time'

require 'zermelo/version'

module Zermelo

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
        Thread.current["zermelo_#{backend.to_s}".to_sym]
      end
    end

    def redis=(connection)
      Thread.current[:zermelo_redis] = connection.nil? ? nil :
        Zermelo::ConnectionProxy.new(connection)
      Thread.current[:zermelo_redis_version] = nil
    end

    def influxdb=(connection)
      Thread.current[:zermelo_influxdb] = Zermelo::ConnectionProxy.new(connection)
    end

    def redis_version
      return nil if Zermelo.redis.nil?
      rv = Thread.current[:zermelo_redis_version]
      return rv unless rv.nil?
      Thread.current[:zermelo_redis_version] = Zermelo.redis.info['redis_version']
    end

    def logger=(l)
      Thread.current[:zermelo_logger] = l
    end

    def logger
      Thread.current[:zermelo_logger]
    end
  end

  class ConnectionProxy
    def initialize(connection)
      @proxied_connection = connection
    end

    # need to override Kernel.exec
    def exec
      @proxied_connection.exec
    end

    def respond_to?(name, include_private = false)
      @proxied_connection.respond_to?(name, include_private)
    end

    def method_missing(name, *args, &block)
      unless Zermelo.logger.nil?
        Zermelo.logger.debug {
          debug_str = "#{name}"
          debug_str += " #{args.inspect}" unless args.empty?
          debug_str
        }
      end
      result = @proxied_connection.send(name, *args, &block)
      unless Zermelo.logger.nil?
        Zermelo.logger.debug {
          debug_str = "#{name}"
          debug_str += " result: #{result}"
          debug_str
        }
      end
      result
    end
  end

end
