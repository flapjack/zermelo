require 'active_support/concern'

require 'zermelo/record'

module Zermelo
  module Records
    module RedisSet
      extend ActiveSupport::Concern

      include Zermelo::Record
      include Zermelo::Records::Unordered

      included do
        set_backend :redis
      end
    end

    module RedisSortedSet
      extend ActiveSupport::Concern

      include Zermelo::Record
      include Zermelo::Records::Ordered

      included do
        set_backend :redis
      end
    end
  end
end