require 'monitor'

require 'active_support/concern'
require 'active_support/core_ext/object/blank'
require 'active_support/inflector'
require 'active_model'

require 'zermelo/associations/class_methods'

require 'zermelo/records/instance_methods'
require 'zermelo/records/class_methods'
require 'zermelo/records/type_validator'

module Zermelo

  module Record

    extend ActiveSupport::Concern

    include Zermelo::Records::InstMethods

    included do
      include ActiveModel::AttributeMethods
      extend ActiveModel::Callbacks
      include ActiveModel::Dirty
      include ActiveModel::Validations
      include ActiveModel::Validations::Callbacks

      # include ActiveModel::MassAssignmentSecurity

      @lock = Monitor.new

      extend Zermelo::Records::ClassMethods
      extend Zermelo::Associations::ClassMethods

      attr_accessor :attributes

      define_model_callbacks :create, :update, :destroy

      attribute_method_suffix  "="  # attr_writers
      # attribute_method_suffix  ""   # attr_readers # DEPRECATED

      validates_with Zermelo::Records::TypeValidator

      define_attributes :id => :string
    end

  end

end