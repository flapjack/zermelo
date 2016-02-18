module Zermelo
  module Filters
    class IndexRange

      attr_reader :start, :finish, :by_score

      def initialize(start, finish, opts = {})
        value_types = opts[:by_score] ? [Float, Date, Time, DateTime] : [Integer]
        [start, finish].each do |v|
          raise "Values must be #{value_types.join('/')}" unless v.nil? || value_types.any? {|vt| v.is_a?(vt)}
        end
        if !start.nil? && !finish.nil? && (start > finish)
          raise "Start of range must be <= finish"
        end
        @start    = start
        @finish   = finish
        @by_score = opts[:by_score]
      end

    end
  end
end