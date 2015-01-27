require 'zermelo/filters/steps/base_step'

module Zermelo
  module Filters
    class Steps
      class OffsetStep < Zermelo::Filters::Steps::BaseStep
        def self.accepted_types
          [:list]
        end

        def self.returns_type
          :list
        end
      end
    end
  end
end
