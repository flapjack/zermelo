
module Zermelo

  module Locks

    class NoLock

      def lock(*record_klasses, &block)
        yield
      end

    end

  end

end