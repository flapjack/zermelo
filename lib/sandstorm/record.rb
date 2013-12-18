require 'monitor'

require 'active_support/concern'
require 'active_support/core_ext/object/blank'
require 'active_support/inflector'
require 'active_model'

require 'sandstorm/associations'
require 'sandstorm/filter'
require 'sandstorm/redis_key'
require 'sandstorm/type_validator'

require 'sandstorm/record/class_methods'
require 'sandstorm/record/instance_methods'

# TODO escape ids and index_keys -- shouldn't allow bare :

# TODO callbacks on before/after add/delete on association?

# TODO optional sort via Redis SORT, first/last for has_many via those

# TODO get DIFF working for exclusion case against ZSETs

module Sandstorm

  module Record

    extend ActiveSupport::Concern

    included do
      include ActiveModel::AttributeMethods
      extend ActiveModel::Callbacks
      include ActiveModel::Dirty
      include ActiveModel::Validations
      # include ActiveModel::MassAssignmentSecurity

      @lock = Monitor.new

      # # including classes can do this instead
      # include ActiveModel::Serializers::JSON
      # self.include_root_in_json = false

      extend Sandstorm::Associations

      attr_accessor :attributes

      define_model_callbacks :create, :update, :destroy

      attribute_method_suffix  "="  # attr_writers
      # attribute_method_suffix  ""   # attr_readers # DEPRECATED

      validates_with Sandstorm::TypeValidator

      define_attributes :id => :string
    end

    include Sandstorm::Record::InstMethods

  end

end