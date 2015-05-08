require 'spec_helper'
require 'zermelo/records/redis'
require 'zermelo/records/influxdb'

describe Zermelo::Records::InstMethods do

  # TODO shared context with different example classes

  shared_examples "it supports ActiveModel instance methods", :instance_methods => true do

    it "is invalid without a name" do
      example = example_class.new(:id => '1')
      expect(example).not_to be_valid

      errs = example.errors
      expect(errs).not_to be_nil
      expect(errs[:name]).to eq(["can't be blank"])
    end

    it 'saves a record' do
      example = example_class.new(:id => '1', :name => 'John Smith')
      expect(example).to be_valid
      expect(example_class.count).to eq(0)
      expect(example.save).to be true
      expect(example_class.count).to eq(1)
    end

    it 'raises an RecordInvalid exception if validation fails while saving' do
      example = example_class.new(:id => '1')

      expect {
        example.save!
      }.to raise_error(Zermelo::Records::Errors::RecordInvalid)
    end

    it 'raises a RecordNotSaved exception if a callback blocks saving' do
      example = example_class.new(:id => '1', :name => 'not_saving')

      expect {
        example.save!
      }.to raise_error(Zermelo::Records::Errors::RecordNotSaved)
    end

    it 'destroys a record'

  end

  context 'redis', :redis => true, :instance_methods => true do

    module ZermeloExamples
      class InstanceMethodsRedis
        include Zermelo::Records::Redis

        define_attributes :name => :string
        validates :name, :presence => true

        before_create :fail_if_not_saving
        def fail_if_not_saving; !('not_saving'.eql?(self.name)); end
      end
    end

    let(:redis) { Zermelo.redis }

    let(:example_class) { ZermeloExamples::InstanceMethodsRedis }

    it 'creates data on record save' do
      example = example_class.new(:id => '1', :name => 'John Smith')
      expect(example).to be_valid
      expect(example.save).to be true

      expect(redis.keys('*')).to match_array([
        'instance_methods_redis::attrs:ids',
        'instance_methods_redis:1:attrs'
      ])
      expect(redis.smembers('instance_methods_redis::attrs:ids')).to eq(['1'])
      expect(redis.hgetall('instance_methods_redis:1:attrs')).to eq(
        'name' => 'John Smith'
      )
    end

  end

  context 'influxdb', :influxdb => true, :instance_methods => true do

    module ZermeloExamples
      class InstanceMethodsInfluxDB
        include Zermelo::Records::InfluxDB

        define_attributes :name   => :string
        validates :name, :presence => true

        before_create :fail_if_not_saving
        def fail_if_not_saving; !('not_saving'.eql?(self.name)); end
      end
    end

    let(:influxdb) { Zermelo.influxdb }

    let(:example_class) { ZermeloExamples::InstanceMethodsInfluxDB }

    it 'creates data on record save' do
      begin
        data = Zermelo.influxdb.query("select * from /instance_methods_influxdb\\/1/")['instance_methods_influx_db/1']
        expect(data).to be_nil
      rescue InfluxDB::Error => ide
        # only happens occasionally, with an empty time series by that name
        raise unless /^Couldn't look up columns$/ === ide.message
      end

      example = example_class.new(:id => '1', :name => 'John Smith')
      expect(example).to be_valid
      expect(example.save).to be true

      data = Zermelo.influxdb.query("select * from /instance_methods_influx_db\\/1/")['instance_methods_influx_db/1']
      expect(data).to be_an(Array)
      expect(data.size).to eql(1)
      record = data.first
      expect(record).to be_a(Hash)
      expect(record).to include("name"=>"John Smith", "id"=>"1")
    end
  end
end
