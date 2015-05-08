require 'active_support/concern'

require 'zermelo/record'

module Zermelo
  module Records
    module Redis
      extend ActiveSupport::Concern

      include Zermelo::Record

      included do
        set_backend :redis
      end
    end
  end
end