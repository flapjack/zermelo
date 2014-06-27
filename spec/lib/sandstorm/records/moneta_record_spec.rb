require 'spec_helper'
require 'sandstorm/records/moneta_record'

describe Sandstorm::Records::MonetaRecord, :moneta => true do

  module Sandstorm
    class MonetaExample
      include Sandstorm::Records::MonetaRecord

      define_attributes :name   => :string,
                        :email  => :string,
                        :active => :boolean

      validates :name, :presence => true
    end
  end

end
