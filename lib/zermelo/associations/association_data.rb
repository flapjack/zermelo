module Zermelo
  module Associations
    class AssociationData
      attr_writer   :data_klass_name, :related_klass_names
      attr_accessor :name, :type_klass, :data_type, :inverse, :sort_key,
        :sort_order, :callbacks

      def initialize(opts = {})
        [:name, :type_klass, :data_type, :inverse, :sort_key, :sort_order,
         :callbacks, :data_klass_name, :related_klass_names].each do |a|

          send("#{a}=".to_sym, opts[a])
        end
      end

      def data_klass
        @data_klass ||= @data_klass_name.constantize
      end

      def related_klasses
        @related_klasses ||= (@related_klass_names || []).map(&:constantize)
      end
    end
  end
end