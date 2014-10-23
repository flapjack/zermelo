require 'sandstorm/filters/steps/base_step'

module Sandstorm
  module Filters
    class Steps
      class SortStep < Sandstorm::Filters::Steps::BaseStep
        def self.accepted_types
          [:set, :sorted_set]
        end

        def self.returns_type
          :list
        end
      end
    end
  end
end
