require 'active_support/concern'

require 'zermelo/records/base'

# TODO check escaping of ids and index_keys -- shouldn't allow bare :, ' '

# TODO callbacks on before/after add/delete on association?

module Zermelo

  module Records

    module RedisRecord

      extend ActiveSupport::Concern

      include Zermelo::Records::Base

      included do
        set_backend :redis
      end

    end

  end

end