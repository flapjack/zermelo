require 'monitor'

require 'active_support/concern'
require 'active_support/core_ext/object/blank'
require 'active_support/inflector'
require 'active_model'

require 'sandstorm/associations/class_methods'

require 'sandstorm/records/instance_methods'
require 'sandstorm/records/class_methods'
require 'sandstorm/records/type_validator'

# TODO escape ids and index_keys -- shouldn't allow bare :

# TODO callbacks on before/after add/delete on association?

# TODO optional sort via Redis SORT, first/last for has_many via those

# TODO get DIFF working for exclusion case against ZSETs

module Sandstorm

  module Records

    module Base

      extend ActiveSupport::Concern

      include Sandstorm::Records::InstMethods

      included do
        include ActiveModel::AttributeMethods
        extend ActiveModel::Callbacks
        include ActiveModel::Dirty
        include ActiveModel::Validations
        # include ActiveModel::MassAssignmentSecurity

        @lock = Monitor.new

        extend Sandstorm::Records::ClassMethods
        extend Sandstorm::Associations::ClassMethods

        attr_accessor :attributes

        define_model_callbacks :create, :update, :destroy

        attribute_method_suffix  "="  # attr_writers
        # attribute_method_suffix  ""   # attr_readers # DEPRECATED

        validates_with Sandstorm::Records::TypeValidator

        define_attributes :id => :string
      end

    end

  end

end