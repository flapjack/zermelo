require "sandstorm/version"

module Sandstorm

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
                     }

  ALL_TYPES = ATTRIBUTE_TYPES.merge(COLLECTION_TYPES)

  class << self
    def valid_type?(type)
      ALL_TYPES.keys.include?(type)
    end

    # Thread and fiber-local
    def redis
      Thread.current[:sandstorm_redis]
    end

    def redis=(connection)
      Thread.current[:sandstorm_redis] = connection
    end
  end

end
