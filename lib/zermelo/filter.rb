require 'active_support/concern'

require 'zermelo/records/errors'

require 'zermelo/filters/steps/list_step'
require 'zermelo/filters/steps/set_step'
require 'zermelo/filters/steps/empty_step'
require 'zermelo/filters/steps/sort_step'

module Zermelo
  module Filter # rubocop:disable Metrics/ModuleLength
    extend ActiveSupport::Concern

    attr_reader :backend, :steps

    # initial set         a Zermelo::Record::Key object
    # associated_class    the class of the result record
    # TODO hash or object for these params as it's getting too long
    # rubocop:disable Metrics/ParameterLists
    def initialize(data_backend, initial_key, associated_class,
                   callback_target_class = nil, callback_target_id = nil,
                   callbacks = nil, sort_order = nil,
                   ancestor = nil, step = nil)
      @backend = data_backend
      @initial_key = initial_key
      @associated_class = associated_class
      @callback_target_class = callback_target_class
      @callback_target_id = callback_target_id
      @callbacks = callbacks
      @sort_order = sort_order
      @steps = ancestor.nil? ? [] : ancestor.steps.dup
      @steps << step unless step.nil?
    end
    # rubocop:enable Metrics/ParameterLists

    def intersect(attrs = {}) # rubocop:disable Metrics/MethodLength
      self.class.new(
        @backend,
        @initial_key,
        @associated_class,
        @callback_target_class,
        @callback_target_id,
        @callbacks,
        @sort_order,
        self,
        ::Zermelo::Filters::Steps::SetStep.new({ op: :intersect }, attrs)
      )
    end

    def union(attrs = {}) # rubocop:disable Metrics/MethodLength
      self.class.new(
        @backend,
        @initial_key,
        @associated_class,
        @callback_target_class,
        @callback_target_id,
        @callbacks,
        @sort_order,
        self,
        ::Zermelo::Filters::Steps::SetStep.new({ op: :union }, attrs)
      )
    end

    def diff(attrs = {}) # rubocop:disable Metrics/MethodLength
      self.class.new(
        @backend,
        @initial_key,
        @associated_class,
        @callback_target_class,
        @callback_target_id,
        @callbacks,
        @sort_order,
        self,
        ::Zermelo::Filters::Steps::SetStep.new({ op: :diff }, attrs)
      )
    end

    def sort(keys, opts = {}) # rubocop:disable Metrics/MethodLength
      self.class.new(
        @backend,
        @initial_key,
        @associated_class,
        @callback_target_class,
        @callback_target_id,
        @callbacks,
        @sort_order,
        self,
        ::Zermelo::Filters::Steps::SortStep.new(
          {
            keys: keys,
            desc: opts[:desc],
            limit: opts[:limit],
            offset: opts[:offset]
          },
          {}
        )
      )
    end

    def offset(amount, opts = {}) # rubocop:disable Metrics/MethodLength
      self.class.new(
        @backend,
        @initial_key,
        @associated_class,
        @callback_target_class,
        @callback_target_id,
        @callbacks,
        @sort_order,
        self,
        ::Zermelo::Filters::Steps::ListStep.new({ offset: amount, limit: opts[:limit] }, {})
      )
    end

    # (a different syntax to the above)
    def page(num, opts = {}) # rubocop:disable Metrics/MethodLength
      per_page = opts[:per_page].to_i || 20
      start = per_page * (num - 1)

      self.class.new(
        @backend,
        @initial_key,
        @associated_class,
        @callback_target_class,
        @callback_target_id,
        @callbacks,
        @sort_order,
        self,
        ::Zermelo::Filters::Steps::ListStep.new({ offset: start, limit: per_page }, {})
      )
    end

    def empty # rubocop:disable Metrics/MethodLength
      self.class.new(
        @backend,
        @initial_key,
        @associated_class,
        @callback_target_class,
        @callback_target_id,
        @callbacks,
        @sort_order,
        self,
        ::Zermelo::Filters::Steps::EmptyStep.new({}, {})
      )
    end

    # step users
    def exists?(e_id)
      lock(false) { _exists?(e_id) }
    end

    def find_by_id(f_id)
      lock { _find_by_id(f_id) }
    end

    def find_by_id!(f_id)
      ret = find_by_id(f_id)
      raise ::Zermelo::Records::Errors::RecordNotFound.new(@associated_class, f_id) if ret.nil?
      ret
    end

    def find_by_ids(*f_ids)
      lock { f_ids.collect { |f_id| _find_by_id(f_id) } }
    end

    def find_by_ids!(*f_ids)
      ret = find_by_ids(*f_ids)
      if ret.any?(&:nil?)
        raise ::Zermelo::Records::Errors::RecordsNotFound.new(@associated_class, f_ids - ret.compact.map(&:id))
      end
      ret
    end

    def ids
      lock(false) { _ids }
    end

    def count
      lock(false) { _count }
    end

    def empty?
      lock(false) { _count.zero? }
    end

    def all
      lock { _all }
    end

    def collect
      lock { _ids.collect { |id| yield(_load(id)) } }
    end
    alias map collect

    def each
      lock { _ids.each { |id| yield(_load(id)) } }
    end

    def select
      lock { _all.select { |obj| yield(obj) } }
    end
    alias find_all select

    def reject
      lock { _all.reject { |obj| yield(obj) } }
    end

    def destroy_all
      lock(*@associated_class.send(:associated_classes)) do
        _all.each(&:destroy)
      end
    end

    def associated_ids_for(name, options = {}) # rubocop:disable Metrics/MethodLength
      data_type, type_klass = @associated_class.send(:with_association_data, name.to_sym) do |data|
        [data.data_type, data.type_klass]
      end

      lock do
        case data_type
        when :belongs_to, :has_one
          type_klass.send(
            :associated_ids_for,
            @backend,
            data_type,
            @associated_class,
            name,
            options[:inversed].is_a?(TrueClass),
            *_ids
          )
        else
          type_klass.send(
            :associated_ids_for,
            @backend,
            data_type,
            @associated_class,
            name,
            *_ids
          )
        end
      end
    end

    def associations_for(name) # rubocop:disable Metrics/MethodLength
      data_type = @associated_class.send(:with_association_data, name.to_sym, &:data_type)

      lock do
        case data_type
        when :belongs_to, :has_one
          raise "'associations_for' only supports multiple associations"
        else
          _ids.each_with_object({}) do |this_id, memo|
            memo[this_id] =
              ::Zermelo::Associations::Multiple.new(
                data_type, @associated_class, this_id, name
              )
          end
        end
      end
    end

    protected

      def lock(when_steps_empty = true, *klasses, &block)
        return(yield) if !when_steps_empty && @steps.empty?
        klasses += [@associated_class] unless klasses.include?(@associated_class)
        @backend.lock(*klasses, &block)
      end

    private

      def _find_by_id(id)
        return unless !id.nil? && _exists?(id)
        _load(id.to_s)
      end

      def _load(id)
        object = @associated_class.new
        object.load(id)
        object
      end

      def _all
        _ids.map! { |id| _load(id) }
      end
  end
end
