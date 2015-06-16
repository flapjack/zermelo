require 'active_support/concern'

require 'zermelo/record'

module Zermelo
  module Records
    module Stub
      extend ActiveSupport::Concern

      include Zermelo::Record

      included do
        set_backend :stub
      end
    end
  end
end