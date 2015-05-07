module Zermelo
  module Filters
    class IndexRange

      attr_reader :start, :finish

      def initialize(start, finish)
        @start  = start
        @finish = finish
      end

    end
  end
end