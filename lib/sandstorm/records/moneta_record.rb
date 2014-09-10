require 'active_support/concern'

require 'sandstorm/records/base'

module Sandstorm

  module Records

    module MonetaRecord

      extend ActiveSupport::Concern

      include Sandstorm::Records::Base

      included do
        set_backend :moneta
      end

    end

  end

end