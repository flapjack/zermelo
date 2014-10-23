require 'sandstorm/filters/steps/base_step'

module Sandstorm
  module Filters
    class Steps
      class OffsetStep < Sandstorm::Filters::Steps::BaseStep
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
