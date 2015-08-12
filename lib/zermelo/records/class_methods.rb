require 'forwardable'
require 'securerandom'

require 'zermelo'

require 'zermelo/backends/influxdb'
require 'zermelo/backends/redis'
require 'zermelo/backends/stub'

require 'zermelo/records/attributes'
require 'zermelo/records/key'

module Zermelo

  module Records

    module ClassMethods

      include Zermelo::Records::Attributes

      extend Forwardable

      def_delegators :filter,
        :intersect, :union, :diff, :sort, :offset, :page, :empty,
        :find_by_id, :find_by_ids, :find_by_id!, :find_by_ids!,
        :all, :each, :collect, :map,
        :select, :find_all, :reject, :destroy_all,
        :ids, :count, :empty?, :exists?,
        :associated_ids_for, :associations_for

      def generate_id
        return SecureRandom.uuid if SecureRandom.respond_to?(:uuid)
        # from 1.9 stdlib
        ary = SecureRandom.random_bytes(16).unpack("NnnnnN")
        ary[2] = (ary[2] & 0x0fff) | 0x4000
        ary[3] = (ary[3] & 0x3fff) | 0x8000
        "%08x-%04x-%04x-%04x-%04x%08x" % ary
      end

      def add_id(id)
        backend.add(ids_key, id.to_s)
      end

      def delete_id(id)
        backend.delete(ids_key, id.to_s)
      end

      def lock(*klasses, &block)
        klasses += [self] unless klasses.include?(self)
        backend.lock(*klasses, &block)
      end

      def transaction(&block)
        failed = false

        backend.begin_transaction

        begin
          yield
        rescue Exception => e
          backend.abort_transaction
          # p e.message
          # puts e.backtrace.join("\n")
          failed = true
        ensure
          backend.commit_transaction unless failed
        end

        # TODO include exception info
        raise "Transaction failed" if failed
      end

      def backend
        raise "No data storage backend set for #{self.name}" if @backend.nil?
        @backend
      end

      protected

      def set_backend(backend_type)
        @backend ||= case backend_type.to_sym
        when :influxdb
          Zermelo::Backends::InfluxDB.new
        when :redis
          Zermelo::Backends::Redis.new
        when :stub
          Zermelo::Backends::Stub.new
        end
      end

      private

      def class_key
        self.name.demodulize.underscore
      end

      def ids_key
        @ids_key ||= Zermelo::Records::Key.new(
                       :klass => self, :name => 'ids',
                       :type => :set,
                       :object => :attribute
                     )
      end

      def temp_key(type)
        Zermelo::Records::Key.new(
          :klass  => self,
          :name   => SecureRandom.hex(16),
          :type   => type,
          :object => :temporary
        )
      end

      def load(id)
        object = self.new
        object.load(id) ? object : nil
      end

      def filter
        backend.filter(ids_key, self)
      end

    end

  end

end