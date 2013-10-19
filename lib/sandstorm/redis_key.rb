
module Sandstorm

  class RedisKey

    attr_reader :key, :type

    def initialize(k, t)
      @key = k
      @type = t
    end

  end

end