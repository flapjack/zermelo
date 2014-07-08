require 'active_support/concern'

module Sandstorm

  module Backends

    module Base

      extend ActiveSupport::Concern

      def get(attr_key)
        get_multiple(attr_key)[attr_key.klass][attr_key.id][attr_key.name.to_s]
      end

    end

  end

end