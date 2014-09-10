require 'active_support/concern'

require 'sandstorm/records/base'

# TODO escape ids and index_keys -- shouldn't allow bare :

# TODO callbacks on before/after add/delete on association?

# TODO optional sort via Redis SORT, first/last for has_many via those

# TODO get DIFF working for exclusion case against ZSETs

module Sandstorm

  module Records

    module RedisRecord

      extend ActiveSupport::Concern

      include Sandstorm::Records::Base

      included do
        set_backend :redis
      end

    end

  end

end