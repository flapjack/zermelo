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

      has_many :children, :class_name => 'Sandstorm::InfluxDBExampleChild'

    end

    class InfluxDBExampleChild
      include Sandstorm::Records::InfluxDBRecord

      define_attributes :name => :string,
                        :important => :boolean

      belongs_to :example, :class_name => 'Sandstorm::InfluxDBExample', :inverse_of => :children

      validates :name, :presence => true
    end
  end

  def create_example(attrs = {})
    Sandstorm.influxdb.write_point("influx_db_example/#{attrs[:id]}", attrs)
  end

  let(:influxdb) { Sandstorm.influxdb }

  it "is invalid without a name" do
    example = Sandstorm::InfluxDBExample.new(:id => '1',
      :email => 'jsmith@example.com', :active => true)
    expect(example).not_to be_valid

    errs = example.errors
    expect(errs).not_to be_nil
    expect(errs[:name]).to eq(["can't be blank"])
  end

  it "adds a record's attributes to influxdb" do
    data = Sandstorm.influxdb.query("select * from /influx_db_example\\/1/")['influx_db_example/1']
    expect(data).to be_nil

    example = Sandstorm::InfluxDBExample.new(:id => '1', :name => 'John Smith',
      :email => 'jsmith@example.com', :active => true)
    expect(example).to be_valid
    expect(example.save).to be_truthy

    data = Sandstorm.influxdb.query("select * from /influx_db_example\\/1/")['influx_db_example/1']
    expect(data).to be_an(Array)
    expect(data.size).to eql(1)
    record = data.first
    expect(record).to be_a(Hash)
    # FIXME boolean is stringified as redis needs it to be like that --
    # should probably make this backend-dependent
    expect(record).to include("name"=>"John Smith",
      "email"=>"jsmith@example.com", "active"=>"true", "id"=>"1")
  end

  it "finds a record by id in influxdb" do
    create_example(:id => '1', :name => 'Jane Doe', :email => 'jdoe@example.com',
      :active => 'true')

    example = Sandstorm::InfluxDBExample.find_by_id('1')
    expect(example).not_to be_nil

    expect(example).to respond_to(:name)
    expect(example.name).to eql('Jane Doe')
    expect(example).to respond_to(:email)
    expect(example.email).to eql('jdoe@example.com')
    expect(example).to respond_to(:active)
    expect(example.active).to be true
  end

  it "can update a value in influxdb" do
    create_example(:id => '1', :name => 'Jane Doe', :email => 'jdoe@example.com',
      :active => 'true')

    example = Sandstorm::InfluxDBExample.find_by_id('1')
    expect(example).not_to be_nil

    example.name = 'John Smith'
    example.save

    other_example = Sandstorm::InfluxDBExample.find_by_id('1')
    expect(other_example).not_to be_nil
    expect(other_example.name).to eq('John Smith')
  end

  it "destroys a single record from influxdb" do
    create_example(:id => '1', :name => 'Jane Doe', :email => 'jdoe@example.com',
      :active => 'true')

    example = Sandstorm::InfluxDBExample.find_by_id('1')
    example.destroy
    example_chk = Sandstorm::InfluxDBExample.find_by_id('1')
    expect(example_chk).to be_nil
  end

  it "resets changed state on refresh" do
    create_example(:id => '1', :name => 'Jane Doe', :email => 'jdoe@example.com',
      :active => 'true')
    example = Sandstorm::InfluxDBExample.find_by_id('1')

    example.name = "King Henry VIII"
    expect(example.changed).to include('name')
    expect(example.changes).to eq({'name' => ['Jane Doe', 'King Henry VIII']})

    example.refresh
    expect(example.changed).to be_empty
    expect(example.changes).to be_empty
  end

  context 'filters' do

    it "returns all record ids" do
      create_example(:id => '1', :name => 'Jane Doe', :email => 'jdoe@example.com',
        :active => 'true')
      create_example(:id => '2', :name => 'John Smith',
        :email => 'jsmith@example.com', :active => 'false')

      examples = Sandstorm::InfluxDBExample.ids
      expect(examples).not_to be_nil
      expect(examples).to be_an(Array)
      expect(examples.size).to eq(2)
      expect(examples).to contain_exactly('2', '1')
    end

    it "returns a count of records" do
      create_example(:id => '1', :name => 'Jane Doe', :email => 'jdoe@example.com',
        :active => 'true')
      create_example(:id => '2', :name => 'John Smith',
        :email => 'jsmith@example.com', :active => 'false')

      example_count = Sandstorm::InfluxDBExample.count
      expect(example_count).not_to be_nil
      expect(example_count).to be_an(Integer)
      expect(example_count).to eq(2)
    end

    it "returns all records" do
      create_example(:id => '1', :name => 'Jane Doe', :email => 'jdoe@example.com',
        :active => 'true')
      create_example(:id => '2', :name => 'John Smith',
        :email => 'jsmith@example.com', :active => 'false')

      examples = Sandstorm::InfluxDBExample.all
      expect(examples).not_to be_nil
      expect(examples).to be_an(Array)
      expect(examples.size).to eq(2)
      expect(examples.map(&:id)).to contain_exactly('2', '1')
    end

    it "filters all class records by attribute values" do
      create_example(:id => '1', :name => 'Jane Doe', :email => 'jdoe@example.com',
        :active => 'true')
      create_example(:id => '2', :name => 'John Smith',
        :email => 'jsmith@example.com', :active => 'false')

      example = Sandstorm::InfluxDBExample.intersect(:active => true).all
      expect(example).not_to be_nil
      expect(example).to be_an(Array)
      expect(example.size).to eq(1)
      expect(example.map(&:id)).to eq(['1'])
    end

    it "chains two intersect filters together" do
      create_example(:id => '1', :name => 'Jane Doe', :email => 'jdoe@example.com',
        :active => 'true')
      create_example(:id => '2', :name => 'John Smith',
        :email => 'jsmith@example.com', :active => 'false')
      create_example(:id => '3', :name => 'Fred Bloggs',
        :email => 'fbloggs@example.com', :active => 'true')

      example = Sandstorm::InfluxDBExample.intersect(:active => true).
        intersect(:name => 'Jane Doe').all
      expect(example).not_to be_nil
      expect(example).to be_an(Array)
      expect(example.size).to eq(1)
      expect(example.map(&:id)).to eq(['1'])
    end

    it "chains an intersect and a union filter together" do
      create_example(:id => '1', :name => 'Jane Doe', :email => 'jdoe@example.com',
        :active => 'true')
      create_example(:id => '2', :name => 'John Smith',
        :email => 'jsmith@example.com', :active => 'false')
      create_example(:id => '3', :name => 'Fred Bloggs',
        :email => 'fbloggs@example.com', :active => 'false')

      example = Sandstorm::InfluxDBExample.intersect(:active => true).union(:name => 'Fred Bloggs').all
      expect(example).not_to be_nil
      expect(example).to be_an(Array)
      expect(example.size).to eq(2)
      expect(example.map(&:id)).to contain_exactly('3', '1')
    end

    it "chains an intersect and a diff filter together" do
      create_example(:id => '1', :name => 'Jane Doe', :email => 'jdoe@example.com',
        :active => 'true')
      create_example(:id => '2', :name => 'John Smith',
        :email => 'jsmith@example.com', :active => 'false')
      create_example(:id => '3', :name => 'Fred Bloggs',
        :email => 'fbloggs@example.com', :active => 'false')

      example = Sandstorm::InfluxDBExample.intersect(:active => false).diff(:name => 'Fred Bloggs').all
      expect(example).not_to be_nil
      expect(example).to be_an(Array)
      expect(example.size).to eq(1)
      expect(example.map(&:id)).to eq(['2'])
    end

  end

  context 'has_many association' do

    # def create_child(attrs = {})
    #   Sandstorm.influxdb.write_point('influx_db_example_child', attrs)
    # end

    it "sets a parent/child has_many relationship between two records in influxdb" do
      create_example(:id => '8', :name => 'John Jones',
                     :email => 'jjones@example.com', :active => 'true')

      child = Sandstorm::InfluxDBExampleChild.new(:id => '3', :name => 'Abel Tasman')
      expect(child.save).to be_truthy

      example = Sandstorm::InfluxDBExample.find_by_id('8')

      children = example.children.all

      expect(children).to be_an(Array)
      expect(children).to be_empty

      example.children << child

      children = example.children.all

      expect(children).to be_an(Array)
      expect(children.size).to eq(1)
    end

  end

end
