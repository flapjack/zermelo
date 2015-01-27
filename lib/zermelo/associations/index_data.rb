module Zermelo
  module Associations
    class IndexData
      attr_writer  :data_klass_name
      attr_accessor :name, :type, :index_klass

      def initialize(opts = {})
        [:name, :type, :index_klass].each do |a|
          send("#{a}=".to_sym, opts[a])
        end
      end

      def data_klass
        @data_klass ||= @data_klass_name.constantize
      end
    end
  end
end