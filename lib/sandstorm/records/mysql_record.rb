require 'active_support/concern'

require 'sandstorm/records/base'

module Sandstorm

  module Records

    module MySQLRecord

      extend ActiveSupport::Concern

      include Sandstorm::Records::Base

      included do
        set_backend :mysql
      end

    end

  end

end