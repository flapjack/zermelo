require 'zermelo/filters/steps/base_step'

module Zermelo
  module Filters
    class Steps
      class UnionStep < Zermelo::Filters::Steps::BaseStep
        def self.accepted_types
          [:set, :sorted_set]
        end

        def self.returns_type
          :set
        end
      end
    end
  end
end
