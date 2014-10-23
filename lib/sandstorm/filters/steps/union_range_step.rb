require 'sandstorm/filters/steps/base_step'

module Sandstorm
  module Filters
    class Steps
      class UnionRangeStep < Sandstorm::Filters::Steps::BaseStep
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
