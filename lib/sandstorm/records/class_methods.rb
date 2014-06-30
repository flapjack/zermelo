require 'forwardable'

require 'sandstorm/backends/influxdb_backend'
require 'sandstorm/backends/moneta_backend'
require 'sandstorm/backends/redis_backend'

require 'sandstorm/records/key'

module Sandstorm

  module Records

    module ClassMethods

      extend Forwardable

      def_delegators :filter, :intersect, :union, :diff,
                       :find_by_id, :all, :each, :collect, :select, :find_all,
                       :reject, :destroy_all,
                       :ids, :count, :empty?, :exists?

      def add_id(id)
        backend.add(ids_key, id.to_s)
      end

      def delete_id(id)
        backend.delete(ids_key, id.to_s)
      end

      def attribute_types
        ret = nil
        @lock.synchronize do
          ret = (@attribute_types ||= {}).dup
        end
        ret
      end

      def transaction(&block)
        failed = false

        backend.begin_transaction

        begin
          yield
        rescue Exception => e
          backend.abort_transaction
          p e.message
          puts e.backtrace.join("\n")
          failed = true
        ensure
          backend.commit_transaction unless failed
        end

        # TODO include exception info
        raise "Transaction failed" if failed
      end

      protected

      def define_attributes(options = {})
        options.each_pair do |key, value|
          raise "Unknown attribute type ':#{value}' for ':#{key}'" unless
            Sandstorm::ALL_TYPES.include?(value)
          self.define_attribute_methods([key])
        end
        @lock.synchronize do
          (@attribute_types ||= {}).update(options)
        end
      end

      def set_backend(backend_type)
        @backend ||= case backend_type.to_sym
        when :redis
          Sandstorm::Backends::RedisBackend.new
        when :moneta
          Sandstorm::Backends::MonetaBackend.new
        when :influxdb
          Sandstorm::Backends::InfluxDBBackend.new
        end
      end

      def backend
        raise "No data storage backend set for #{self.name}" if @backend.nil?
        @backend
      end

      private

      def lock(*klasses, &block)
        klasses |= [self]
        ret = nil
        # doesn't handle re-entrant case for influxdb, which has no locking yet
        locking = Thread.current[:sandstorm_locking]
        if locking.nil?
          lock_proc = proc do
            begin
              Thread.current[:sandstorm_locking] = klasses
              ret = block.call
            ensure
              Thread.current[:sandstorm_locking] = nil
            end
          end
          if backend_lock = backend.lock(*klasses)
            backend_lock.lock(&lock_proc)
          else
            lock_proc.call
          end
        else
          # accepts any subset of 'locking'
          unless (klasses - locking).empty?
            raise "Currently locking #{locking.map(&:name)}, cannot lock different set #{klasses.map(&:name)}"
          end
          ret = block.call
        end
        ret
      end

      def ids_key
        @ids_key ||= Sandstorm::Records::Key.new(:class => class_key, :name => 'ids',
          :type => :set)
      end

      def class_key
        self.name.demodulize.underscore
      end

      def load(id)
        object = self.new
        object.load(id)
        object
      end

      def filter
        backend.filter(ids_key, self)
      end

    end

  end

end