require 'zermelo/filters/steps/base_step'

module Zermelo
  module Filters
    class Steps
      class UnionRangeStep < Zermelo::Filters::Steps::BaseStep
        def self.accepted_types
          [:sorted_set]
        end

        def self.returns_type
          :sorted_set
        end
      end
    end
  end
end
