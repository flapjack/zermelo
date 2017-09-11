require 'spec_helper'
require 'zermelo/records/redis'
require 'zermelo/records/influxdb'

describe Zermelo::Records::ClassMethods do # rubocop:disable Metrics/BlockLength
  context 'redis', redis: true, class_methods: true do
    module ZermeloExamples
      class ClassMethodsRedis
        include Zermelo::Records::RedisSet

        # NB: not currently used
        define_attributes name: :string
        validates :name, presence: true

        # NB: not currently used
        before_create :fail_if_not_saving
        def fail_if_not_saving
          !'not_saving'.eql?(name)
        end
      end
    end

    let(:redis) { Zermelo.redis } # NOTE not currently used

    let(:example_class) { ZermeloExamples::ClassMethodsRedis }

    let(:ek) { 'class_methods_redis' } # NOTE not currently used

    # SMELL These should probably be in a shared set of specs, if this spec
    # file is ever extended
    context 'class_key' do
      it 'returns correct class_key' do
        class_key = ZermeloExamples::ClassMethodsRedis.send(:class_key)
        expect(class_key).to eq('class_methods_redis')
      end

      it 'returns correct class_key for subclass' do
        class SubClassMethodsRedis < ZermeloExamples::ClassMethodsRedis; end

        class_key = SubClassMethodsRedis.send(:class_key)
        expect(class_key).to eq('sub_class_methods_redis')
      end
    end
  end
end
