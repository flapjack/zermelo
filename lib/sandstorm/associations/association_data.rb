module Sandstorm
  module Associations
    class AssociationData
      attr_writer   :data_klass_name
      attr_accessor :name, :type_klass, :inverse, :sort_key, :callbacks

      def initialize(opts = {})
        [:name, :type_klass, :inverse, :sort_key, :callbacks, :data_klass_name].each do |a|
          send("#{a}=".to_sym, opts[a])
        end
      end

      def data_klass
        @data_klass ||= @data_klass_name.constantize
      end
    end
  end
end