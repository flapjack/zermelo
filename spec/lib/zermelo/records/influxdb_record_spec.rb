require 'spec_helper'
require 'zermelo/records/influxdb_record'

describe Zermelo::Records::InfluxDBRecord, :influxdb => true do

  module Zermelo
    class InfluxDBExample
      include Zermelo::Records::InfluxDBRecord

      define_attributes :name   => :string,
                        :email  => :string,
                        :active => :boolean

      validates :name, :presence => true

      has_many :children, :class_name => 'Zermelo::InfluxDBChild'
      # has_sorted_set :sorted, :class_name => 'Zermelo::InfluxDBSorted'
    end

    class InfluxDBChild
      include Zermelo::Records::InfluxDBRecord

      define_attributes :name => :string,
                        :important => :boolean

      belongs_to :example, :class_name => 'Zermelo::InfluxDBExample', :inverse_of => :children

      validates :name, :presence => true
    end

    class InfluxDBSorted
      include Zermelo::Records::InfluxDBRecord

      define_attributes :name => :string,
                        :important => :boolean

      belongs_to :example, :class_name => 'Zermelo::InfluxDBExample', :inverse_of => :sorted

      validates :name, :presence => true
    end
  end

  def create_example(attrs = {})
    Zermelo.influxdb.write_point("influx_db_example/#{attrs[:id]}", attrs)
  end

  let(:influxdb) { Zermelo.influxdb }

  it "is invalid without a name" do
    example = Zermelo::InfluxDBExample.new(:id => '1',
      :email => 'jsmith@example.com', :active => true)
    expect(example).not_to be_valid

    errs = example.errors
    expect(errs).not_to be_nil
    expect(errs[:name]).to eq(["can't be blank"])
  end

  it "adds a record's attributes to influxdb" do
    begin
      data = Zermelo.influxdb.query("select * from /influx_db_example\\/1/")['influx_db_example/1']
      expect(data).to be_nil
    rescue InfluxDB::Error => ide
      # only happens occasionally, with an empty time series by that name
      raise unless /^Couldn't look up columns$/ === ide.message
    end

    example = Zermelo::InfluxDBExample.new(:id => '1', :name => 'John Smith',
      :email => 'jsmith@example.com', :active => true)
    expect(example).to be_valid
    expect(example.save).to be_truthy

    data = Zermelo.influxdb.query("select * from /influx_db_example\\/1/")['influx_db_example/1']
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

    example = Zermelo::InfluxDBExample.find_by_id('1')
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

    example = Zermelo::InfluxDBExample.find_by_id('1')
    expect(example).not_to be_nil

    example.name = 'John Smith'
    example.save

    other_example = Zermelo::InfluxDBExample.find_by_id('1')
    expect(other_example).not_to be_nil
    expect(other_example.name).to eq('John Smith')
  end

  it "destroys a single record from influxdb" do
    create_example(:id => '1', :name => 'Jane Doe', :email => 'jdoe@example.com',
      :active => 'true')

    example = Zermelo::InfluxDBExample.find_by_id('1')
    example.destroy
    example_chk = Zermelo::InfluxDBExample.find_by_id('1')
    expect(example_chk).to be_nil
  end

  it "resets changed state on refresh" do
    create_example(:id => '1', :name => 'Jane Doe', :email => 'jdoe@example.com',
      :active => 'true')
    example = Zermelo::InfluxDBExample.find_by_id('1')

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

      examples = Zermelo::InfluxDBExample.ids
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

      example_count = Zermelo::InfluxDBExample.count
      expect(example_count).not_to be_nil
      expect(example_count).to be_an(Integer)
      expect(example_count).to eq(2)
    end

    it "returns all records" do
      create_example(:id => '1', :name => 'Jane Doe', :email => 'jdoe@example.com',
        :active => 'true')
      create_example(:id => '2', :name => 'John Smith',
        :email => 'jsmith@example.com', :active => 'false')

      examples = Zermelo::InfluxDBExample.all
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

      example = Zermelo::InfluxDBExample.intersect(:active => true).all
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

      example = Zermelo::InfluxDBExample.intersect(:active => true).
        intersect(:name => 'Jane Doe').all
      expect(example).not_to be_nil
      expect(example).to be_an(Array)
      expect(example.size).to eq(1)
      expect(example.map(&:id)).to eq(['1'])
    end

    it "allows multiple attributes in an intersect filter" do
      create_example(:id => '1', :name => 'Jane Doe', :email => 'jdoe@example.com',
        :active => 'true')
      create_example(:id => '2', :name => 'John Smith',
        :email => 'jsmith@example.com', :active => 'false')
      create_example(:id => '3', :name => 'Fred Bloggs',
        :email => 'fbloggs@example.com', :active => 'true')

      example = Zermelo::InfluxDBExample.intersect(:active => true,
        :name => 'Jane Doe').all
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

      example = Zermelo::InfluxDBExample.intersect(:active => true).union(:name => 'Fred Bloggs').all
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

      example = Zermelo::InfluxDBExample.intersect(:active => false).diff(:name => 'Fred Bloggs').all
      expect(example).not_to be_nil
      expect(example).to be_an(Array)
      expect(example.size).to eq(1)
      expect(example.map(&:id)).to eq(['2'])
    end

  end

  context 'has_many association' do

    it "sets a parent/child has_many relationship between two records in influxdb" do
      create_example(:id => '8', :name => 'John Jones',
                     :email => 'jjones@example.com', :active => 'true')
      example = Zermelo::InfluxDBExample.find_by_id('8')

      child = Zermelo::InfluxDBChild.new(:id => '3', :name => 'Abel Tasman')
      expect(child.save).to be_truthy

      children = example.children.all

      expect(children).to be_an(Array)
      expect(children).to be_empty

      example.children << child

      children = example.children.all

      expect(children).to be_an(Array)
      expect(children.size).to eq(1)
    end

    it "applies an intersect filter to a has_many association" do
      create_example(:id => '8', :name => 'John Jones',
                     :email => 'jjones@example.com', :active => 'true')
      example = Zermelo::InfluxDBExample.find_by_id('8')

      child_1 = Zermelo::InfluxDBChild.new(:id => '3', :name => 'John Smith')
      expect(child_1.save).to be_truthy

      child_2 = Zermelo::InfluxDBChild.new(:id => '4', :name => 'Jane Doe')
      expect(child_2.save).to be_truthy

      example.children.add(child_1, child_2)
      expect(example.children.count).to eq(2)

      result = example.children.intersect(:name => 'John Smith').all
      expect(result).to be_an(Array)
      expect(result.size).to eq(1)
      expect(result.map(&:id)).to eq(['3'])
    end

    it "applies chained intersect and union filters to a has_many association" do
      create_example(:id => '8', :name => 'John Jones',
                     :email => 'jjones@example.com', :active => 'true')
      example = Zermelo::InfluxDBExample.find_by_id('8')

      child_1 = Zermelo::InfluxDBChild.new(:id => '3', :name => 'John Smith')
      expect(child_1.save).to be_truthy

      child_2 = Zermelo::InfluxDBChild.new(:id => '4', :name => 'Jane Doe')
      expect(child_2.save).to be_truthy

      example.children.add(child_1, child_2)
      expect(example.children.count).to eq(2)

      result = example.children.intersect(:name => 'John Smith').union(:id => '4').all
      expect(result).to be_an(Array)
      expect(result.size).to eq(2)
      expect(result.map(&:id)).to eq(['3', '4'])
    end

  end

  context 'has_sorted_set association' do

    before do
      skip "broken"
    end

    let(:time_i) { Time.now.to_i }

    it "sets a parent/child has_sorted_set relationship between two records in influxdb" do
      create_example(:id => '8', :name => 'John Jones',
                     :email => 'jjones@example.com', :active => 'true')
      example = Zermelo::InfluxDBExample.find_by_id('8')

      sorted_1 = Zermelo::InfluxDBSorted.new(:id => '3', :name => 'Abel Tasman', :time => time_i)
      expect(sorted_1.save).to be_truthy

      sorted_2 = Zermelo::InfluxDBSorted.new(:id => '4', :name => 'Joe Smith', :time => time_i - 60)
      expect(sorted_2.save).to be_truthy

      sorted = example.sorted.all

      expect(sorted).to be_an(Array)
      expect(sorted).to be_empty

      example.sorted.add(sorted_1, sorted_2)

      sorted = example.sorted.all

      expect(sorted).to be_an(Array)
      expect(sorted.size).to eq(2)
      expect(sorted.map(&:id)).to eq(['3', '4'])
    end

    it "applies an intersect filter to a has_sorted_set association" do
      create_example(:id => '8', :name => 'John Jones',
                     :email => 'jjones@example.com', :active => 'true')
      example = Zermelo::InfluxDBExample.find_by_id('8')

      sorted_1 = Zermelo::InfluxDBSorted.new(:id => '3', :name => 'Abel Tasman', :time => time_i)
      expect(sorted_1.save).to be_truthy

      sorted_2 = Zermelo::InfluxDBSorted.new(:id => '4', :name => 'Joe Smith', :time => time_i - 60)
      expect(sorted_2.save).to be_truthy

      example.sorted.add(sorted_1, sorted_2)

      result = example.sorted.intersect(:id => '3').all
      expect(result).to be_an(Array)
      expect(result.size).to eq(1)
      expect(result.map(&:id)).to eq(['3'])
    end

    it "applies an intersect_range filter to a has_sorted_set association" do
      skip "broken"

      create_example(:id => '8', :name => 'John Jones',
                     :email => 'jjones@example.com', :active => 'true')
      example = Zermelo::InfluxDBExample.find_by_id('8')

      sorted_1 = Zermelo::InfluxDBSorted.new(:id => '3', :name => 'Abel Tasman', :time => time_i)
      expect(sorted_1.save).to be_truthy

      sorted_2 = Zermelo::InfluxDBSorted.new(:id => '4', :name => 'Joe Smith', :time => time_i - 60)
      expect(sorted_2.save).to be_truthy

      example.sorted.add(sorted_1, sorted_2)

      result = example.sorted.intersect_range(time_i - 30, time_i + 30).all
      expect(result).to be_an(Array)
      expect(result.size).to eq(1)
      expect(result.map(&:id)).to eq(['3'])
    end

    it "applies chained intersect and union filters to a has_sorted_set association" do
      create_example(:id => '8', :name => 'John Jones',
                     :email => 'jjones@example.com', :active => 'true')
      example = Zermelo::InfluxDBExample.find_by_id('8')

      sorted_1 = Zermelo::InfluxDBSorted.new(:id => '3', :name => 'Abel Tasman', :time => time_i)
      expect(sorted_1.save).to be_truthy

      sorted_2 = Zermelo::InfluxDBSorted.new(:id => '4', :name => 'Joe Smith', :time => time_i - 60)
      expect(sorted_2.save).to be_truthy

      sorted_3 = Zermelo::InfluxDBSorted.new(:id => '5', :name => 'John Trugg', :time => time_i - 90)
      expect(sorted_3.save).to be_truthy

      example.sorted.add(sorted_1, sorted_2, sorted_3)

      result = example.sorted.intersect(:name => 'Abel Tasman').union(:id => '4').all
      expect(result).to be_an(Array)
      expect(result.size).to eq(2)
      expect(result.map(&:id)).to eq(['3', '4'])
    end

    # # See https://github.com/flapjack/zermelo/issues/15
    # it "applies chained intersect_range and union filters to a has_sorted_set association" do
    #   create_example(:id => '8', :name => 'John Jones',
    #                  :email => 'jjones@example.com', :active => 'true')
    #   example = Zermelo::InfluxDBExample.find_by_id('8')

    #   sorted_1 = Zermelo::InfluxDBSorted.new(:id => '3', :name => 'Abel Tasman', :time => time_i - 30)
    #   expect(sorted_1.save).to be_truthy

    #   sorted_2 = Zermelo::InfluxDBSorted.new(:id => '4', :name => 'Joe Smith', :time => time_i - 60)
    #   expect(sorted_2.save).to be_truthy

    #   sorted_3 = Zermelo::InfluxDBSorted.new(:id => '5', :name => 'John Trugg', :time => time_i - 90)
    #   expect(sorted_3.save).to be_truthy

    #   example.sorted.add(sorted_1, sorted_2, sorted_3)

    #   result = example.sorted.intersect_range(time_i - 45, time_i - 15).union(:id => '4').all
    #   expect(result).to be_an(Array)
    #   expect(result.size).to eq(2)
    #   expect(result.map(&:id)).to eq(['3', '4'])
    # end

  end

end
