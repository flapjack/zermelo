require 'active_support/concern'

require 'zermelo/record'

module Zermelo
  module Records
    module MySQLSet
      extend ActiveSupport::Concern

      include Zermelo::Record
      include Zermelo::Records::Unordered

      included do
        set_backend :mysql
      end
    end

    module MySQLSortedSet
      extend ActiveSupport::Concern

      include Zermelo::Record
      include Zermelo::Records::Ordered

      included do
        set_backend :mysql
      end
    end
  end
end