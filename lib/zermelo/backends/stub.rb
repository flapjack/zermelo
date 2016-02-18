require 'zermelo/backend'

module Zermelo

  module Backends

    class Stub

      include Zermelo::Backend

      def key_to_backend_key(key)
        raise "Not supported"
      end

      def filter(ids_key, associated_class, callback_target_class = nil,
        callback_target_id = nil, callbacks = nil, sort_order = nil)

        raise "Not supported"
      end

      def get_multiple(*attr_keys)
        raise "Not supported"
      end

      def begin_transaction
        raise "Not supported"
      end

      def commit_transaction
        raise "Not supported"
      end

      def abort_transaction
        raise "Not supported"
      end

      private

      def change(op, key, value = nil, key_to = nil, value_to = nil)
        # no-op
      end

    end

  end

end