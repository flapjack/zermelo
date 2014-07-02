require 'active_support/concern'

module Sandstorm

  module Filters

    module Base

      extend ActiveSupport::Concern

      attr_reader :backend

      # initial set         a Sandstorm::Record::Key object
      # associated_class    the class of the result record
      def initialize(data_backend, initial_set, associated_class)
        @backend          = data_backend
        @initial_set      = initial_set
        @associated_class = associated_class
        @steps            = []
      end

      def intersect(opts = {})
        @steps += [:intersect, {}, opts]
        self
      end

      def union(opts = {})
        @steps += [:union, {}, opts]
        self
      end

      def diff(opts = {})
        @steps += [:diff, {}, opts]
        self
      end

      # TODO split data and arguments in step construction
      def intersect_range(start, finish, opts = {})
        @steps += [:intersect_range, {:start => start, :finish => finish,
          :order => opts.delete(:order), :by_score => opts.delete(:by_score)}, opts]
        self
      end

      def union_range(start, finish, opts = {})
        @steps += [:union_range, {:start => start, :finish => finish,
          :order => opts.delete(:order), :by_score => opts.delete(:by_score)}, opts]
        self
      end

      # step users
      def exists?(id)
        lock(false) { _exists?(id) }
      end

      def find_by_id(id)
        lock { _find_by_id(id) }
      end

      protected

      def _find_by_id(id)
        if !id.nil? && _exists?(id)
          _load(id.to_s)
        else
          nil
        end
      end

      def _load(id)
        object = @associated_class.new
        object.load(id)
        object
      end

    end

  end

end
