require 'active_support/ordered_hash'

module Zermelo
  class OrderedSet < Set

    def initialize(enum = nil, &block)
      @hash = ActiveSupport::OrderedHash.new
      super
    end
  end
end