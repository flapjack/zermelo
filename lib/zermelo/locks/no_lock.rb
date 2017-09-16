module Zermelo
  module Locks
    class NoLock
      def lock(*_record_klasses, &_block)
        yield
      end
    end
  end
end
