require 'spec_helper'
require 'zermelo/records/redis'
require 'zermelo/records/influxdb'

describe Zermelo::Records::InstMethods do

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

    it "updates a value" do
      create_example(:id => '1', :name => 'Jane Doe')

      example = example_class.find_by_id('1')
      expect(example).not_to be_nil

      example.name = 'John Smith'
      example.save

      other_example = example_class.find_by_id('1')
      expect(other_example).not_to be_nil
      expect(other_example.name).to eq('John Smith')
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

    it "resets changed state on refresh" do
      create_example(:id => '8', :name => 'John Jones')
      example = example_class.find_by_id('8')

      example.name = "King Henry VIII"
      expect(example.changed).to include('name')
      expect(example.changes).to eq({'name' => ['John Jones', 'King Henry VIII']})

      example.refresh
      expect(example.changed).to be_empty
      expect(example.changes).to be_empty
    end

    it "stores a string as an attribute value"
    it "stores an integer as an attribute value"
    it "stores a timestamp as an attribute value"
    it "stores a boolean as an attribute value"

    it "stores a list as an attribute value"
    it "stores a set as an attribute value"
    it "stores a hash as an attribute value"

    it 'destroys a record' do
      create_example(:id => '1', :name => 'Jane Doe')

      example = example_class.find_by_id('1')
      example.destroy
      example_chk = example_class.find_by_id('1')
      expect(example_chk).to be_nil
    end
  end

  context 'redis', :redis => true, :instance_methods => true do

    module ZermeloExamples
      class InstanceMethodsRedis
        include Zermelo::Records::RedisSet

        define_attributes :name => :string
        validates :name, :presence => true

        before_create :fail_if_not_saving
        def fail_if_not_saving; !('not_saving'.eql?(self.name)); end
      end
    end

    let(:redis) { Zermelo.redis }

    let(:example_class) { ZermeloExamples::InstanceMethodsRedis }

    let(:ek) { 'instance_methods_redis' }

    def create_example(attrs = {})
      redis.hmset("#{ek}:#{attrs[:id]}:attrs", {'name' => attrs[:name]}.to_a.flatten)
      redis.sadd("#{ek}::attrs:ids", attrs[:id])
    end

    it 'creates data on record save' do
      example = example_class.new(:id => '1', :name => 'John Smith')
      expect(example).to be_valid
      expect(example.save).to be true

      expect(redis.keys('*')).to match_array([
        "#{ek}::attrs:ids",
        "#{ek}:1:attrs"
      ])
      expect(redis.smembers("#{ek}::attrs:ids")).to eq(['1'])
      expect(redis.hgetall("#{ek}:1:attrs")).to eq(
        'name' => 'John Smith'
      )
    end

    it "updates a record's attributes" do
      create_example(:id => '8', :name => 'John Jones')

      example = example_class.find_by_id('8')
      example.name = 'Jane Janes'
      expect(example.save).to be true

      expect(redis.keys('*')).to match_array([
        "#{ek}::attrs:ids",
        "#{ek}:8:attrs"
      ])
      expect(redis.smembers("#{ek}::attrs:ids")).to eq(['8'])
      expect(redis.hgetall("#{ek}:8:attrs")).to eq(
        'name' => 'Jane Janes'
      )
    end

    it "deletes a record's attributes" do
      create_example(:id => '8', :name => 'John Jones')

      expect(redis.keys('*')).to match_array([
        "#{ek}::attrs:ids",
        "#{ek}:8:attrs",
      ])

      example = example_class.find_by_id('8')
      example.destroy

      expect(redis.keys('*')).to eq([])
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

    let(:ek) { 'instance_methods_influx_db' }

    def create_example(attrs = {})
      Zermelo.influxdb.write_point("#{ek}/#{attrs[:id]}", attrs)
    end

    it 'creates data on record save' do
      begin
        data = Zermelo.influxdb.query("select * from /#{ek}\\/1/")["#{ek}/1"]
        expect(data).to be_nil
      rescue InfluxDB::Error => ide
        # only happens occasionally, with an empty time series by that name
        raise unless /^Couldn't look up columns$/ === ide.message
      end

      example = example_class.new(:id => '1', :name => 'John Smith')
      expect(example).to be_valid
      expect(example.save).to be true

      data = Zermelo.influxdb.query("select * from /#{ek}\\/1/")["#{ek}/1"]
      expect(data).to be_an(Array)
      expect(data.size).to eql(1)
      record = data.first
      expect(record).to be_a(Hash)
      expect(record).to include("name"=>"John Smith", "id"=>"1")
    end
  end
end
