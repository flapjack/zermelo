require 'spec_helper'
require 'sandstorm/records/influxdb_record'

describe Sandstorm::Records::InfluxDBRecord, :influxdb => true do

  module Sandstorm
    class InfluxDBExample
      include Sandstorm::Records::InfluxDBRecord

      define_attributes :name   => :string,
                        :email  => :string,
                        :active => :boolean

      validates :name, :presence => true
    end
  end


  let(:influxdb) { Sandstorm.influxdb }

  it "is invalid without a name" do
    example = Sandstorm::InfluxDBExample.new(:id => '1', :email => 'jsmith@example.com')
    expect(example).not_to be_valid

    errs = example.errors
    expect(errs).not_to be_nil
    expect(errs[:name]).to eq(["can't be blank"])
  end

  it "adds a record's attributes to influxdb" do
    example = Sandstorm::InfluxDBExample.new(:id => '1', :name => 'John Smith',
      :email => 'jsmith@example.com', :active => true)
    expect(example).to be_valid
    expect(example.save).to be_truthy

    data = Sandstorm.influxdb.query("select * from influx_db_example")['influx_db_example']
    expect(data).to be_an(Array)
    expect(data.size).to eql(1)
    record = data.first
    expect(record).to be_a(Hash)
    # FIXME boolean is stringified as redis needs it to be like that --
    # should probably make this backend-dependent
    expect(record).to include("attrs"=>{"name"=>"John Smith",
      "email"=>"jsmith@example.com", "active"=>"true"}, "id"=>"1")
  end

  it "finds a record by id in influxdb" do
    Sandstorm.influxdb.write_point('influx_db_example', :id => '1',
      :attrs => {:name => 'Jane Doe', :email => 'jdoe@example.com', :active => 'true'})

    example = Sandstorm::InfluxDBExample.find_by_id('1')
    expect(example).not_to be_nil

    expect(example).to respond_to(:name)
    expect(example.name).to eql('Jane Doe')
    expect(example).to respond_to(:email)
    expect(example.email).to eql('jdoe@example.com')
    expect(example).to respond_to(:active)
    expect(example.active).to be true
  end

  it "cannot update a value in influxdb" do
    Sandstorm.influxdb.write_point('influx_db_example', :id => '1',
      :attrs => {:name => 'Jane Doe', :email => 'jdoe@example.com', :active => 'true'})

    example = Sandstorm::InfluxDBExample.find_by_id('1')
    expect(example).not_to be_nil

    example.name = 'John Smith'
    expect {example.save}.to raise_error
  end

  it "cannot write a point with an id that already exists for that time series" do
    Sandstorm.influxdb.write_point('influx_db_example', :id => '1',
      :attrs => {:name => 'Jane Doe', :email => 'jdoe@example.com', :active => 'true'})

    example = example = Sandstorm::InfluxDBExample.new(:id => '1', :name => 'John Smith',
      :email => 'jsmith@example.com', :active => true)
    expect {example.save}.to raise_error
  end

end
