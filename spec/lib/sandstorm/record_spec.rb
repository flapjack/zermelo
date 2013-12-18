require 'spec_helper'
require 'sandstorm/record'

# NB: also covers associations.rb, which is mixed in to Sandstorm::Record

describe Sandstorm::Record, :redis => true do

  module Sandstorm
    class Example
      include Sandstorm::Record

      define_attributes :name   => :string,
                        :email  => :string,
                        :active => :boolean

      validates :name, :presence => true

      has_many :children, :class_name => 'Sandstorm::ExampleChild'

      has_sorted_set :data, :class_name => 'Sandstorm::ExampleDatum',
        :key => :timestamp

      has_and_belongs_to_many :templates, :class_name => 'Sandstorm::Template',
        :inverse_of => :examples

      index_by :active
      unique_index_by :name
    end

    class ExampleChild
      include Sandstorm::Record

      define_attributes :name => :string,
                        :important => :boolean

      index_by :important

      belongs_to :example, :class_name => 'Sandstorm::Example', :inverse_of => :children

      validates :name, :presence => true
    end

    class ExampleDatum
      include Sandstorm::Record

      define_attributes :timestamp => :timestamp,
                        :summary => :string,
                        :emotion => :string

      belongs_to :example, :class_name => 'Sandstorm::Example', :inverse_of => :data

      index_by :emotion

      validates :timestamp, :presence => true
    end

    class Template
      include Sandstorm::Record

      define_attributes :name => :string

      has_and_belongs_to_many :examples, :class_name => 'Sandstorm::Example',
        :inverse_of => :templates

      validates :name, :presence => true
    end
  end

  let(:redis) { Sandstorm.redis }

  def create_example(attrs = {})
    redis.hmset("example:#{attrs[:id]}:attrs",
      {'name' => attrs[:name], 'email' => attrs[:email], 'active' => attrs[:active]}.to_a.flatten)
    redis.sadd("example::by_active:#{!!attrs[:active]}", attrs[:id])
    redis.hset("example::by_name", attrs[:name], attrs[:id])
    redis.sadd('example::ids', attrs[:id])
  end

  it "is invalid without a name" do
    example = Sandstorm::Example.new(:id => '1', :email => 'jsmith@example.com')
    expect(example).not_to be_valid

    errs = example.errors
    expect(errs).not_to be_nil
    expect(errs[:name]).to eq(["can't be blank"])
  end

  it "adds a record's attributes to redis" do
    example = Sandstorm::Example.new(:id => '1', :name => 'John Smith',
      :email => 'jsmith@example.com', :active => true)
    expect(example).to be_valid
    expect(example.save).to be_truthy

    expect(redis.keys('*')).to match_array(['example::ids',
                               'example:1:attrs',
                               'example::by_name',
                               'example::by_active:true'])
    expect(redis.smembers('example::ids')).to eq(['1'])
    expect(redis.hgetall('example:1:attrs')).to eq(
      {'name' => 'John Smith', 'email' => 'jsmith@example.com', 'active' => 'true'}
    )
    expect(redis.hgetall('example::by_name')).to eq({'John Smith' => '1'})
    expect(redis.smembers('example::by_active:true')).to eq(
      ['1']
    )
  end

  it "finds a record by id in redis" do
    create_example(:id => '8', :name => 'John Jones',
                   :email => 'jjones@example.com', :active => 'true')

    example = Sandstorm::Example.find_by_id('8')
    expect(example).not_to be_nil
    expect(example.id).to eq('8')
    expect(example.name).to eq('John Jones')
    expect(example.email).to eq('jjones@example.com')
  end

  it "finds records by an indexed value in redis" do
    create_example(:id => '8', :name => 'John Jones',
                   :email => 'jjones@example.com', :active => 'true')

    examples = Sandstorm::Example.intersect(:active => true).all
    expect(examples).not_to be_nil
    expect(examples).to be_an(Array)
    expect(examples.size).to eq(1)
    example = examples.first
    expect(example.id).to eq('8')
    expect(example.name).to eq('John Jones')
    expect(example.email).to eq('jjones@example.com')
  end

  it "finds records by a uniquely indexed value in redis" do
    create_example(:id => '8', :name => 'John Jones',
                   :email => 'jjones@example.com', :active => 'true')

    examples = Sandstorm::Example.intersect(:name => 'John Jones').all
    expect(examples).not_to be_nil
    expect(examples).to be_an(Array)
    expect(examples.size).to eq(1)
    example = examples.first
    expect(example.id).to eq('8')
    expect(example.name).to eq('John Jones')
    expect(example.email).to eq('jjones@example.com')
  end

  it "updates a record's attributes in redis" do
    create_example(:id => '8', :name => 'John Jones',
                   :email => 'jjones@example.com', :active => 'true')

    example = Sandstorm::Example.find_by_id('8')
    example.name = 'Jane Janes'
    example.email = 'jjanes@example.com'
    expect(example.save).to be_truthy

    expect(redis.keys('*')).to match_array(['example::ids',
                               'example:8:attrs',
                               'example::by_name',
                               'example::by_active:true'])
    expect(redis.smembers('example::ids')).to eq(['8'])
    expect(redis.hgetall('example:8:attrs')).to eq(
      {'name' => 'Jane Janes', 'email' => 'jjanes@example.com', 'active' => 'true'}
    )
    expect(redis.smembers('example::by_active:true')).to eq(
      ['8']
    )
  end

  it "deletes a record's attributes from redis" do
    create_example(:id => '8', :name => 'John Jones',
                   :email => 'jjones@example.com', :active => 'true')

    expect(redis.keys('*')).to match_array(['example::ids',
                               'example:8:attrs',
                               'example::by_name',
                               'example::by_active:true'])

    example = Sandstorm::Example.find_by_id('8')
    example.destroy

    expect(redis.keys('*')).to eq([])
  end

  it "resets changed state on refresh" do
    create_example(:id => '8', :name => 'John Jones',
                   :email => 'jjones@example.com', :active => 'true')
    example = Sandstorm::Example.find_by_id('8')

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

  context 'filters' do

    let(:active) {
      create_example(:id => '8', :name => 'John Jones',
                     :email => 'jjones@example.com', :active => true)
    }

    let(:inactive) {
      create_example(:id => '9', :name => 'James Brown',
                     :email => 'jbrown@example.com', :active => false)
    }

    before do
      active; inactive
    end

    it "filters all class records by indexed attribute values" do
      example = Sandstorm::Example.intersect(:active => true).all
      expect(example).not_to be_nil
      expect(example).to be_an(Array)
      expect(example.size).to eq(1)
      expect(example.map(&:id)).to eq(['8'])
    end

    it 'supports sequential intersection and union operations' do
      example = Sandstorm::Example.intersect(:active => true).union(:active => false).all
      expect(example).not_to be_nil
      expect(example).to be_an(Array)
      expect(example.size).to eq(2)
      expect(example.map(&:id)).to match_array(['8', '9'])
    end

    it 'allows intersection operations across multiple values for an attribute'

    it 'allows union operations across multiple values for an attribute'

    it 'excludes particular records' do
      example = Sandstorm::Example.diff(:active => true).all
      expect(example).not_to be_nil
      expect(example).to be_an(Array)
      expect(example.size).to eq(1)
      expect(example.map(&:id)).to eq(['9'])
    end

  end


  context "has_many" do

    def create_child(parent, attrs = {})
      redis.sadd("example:#{parent.id}:children_ids", attrs[:id])

      redis.hmset("example_child:#{attrs[:id]}:attrs",
                  {'name' => attrs[:name], 'important' => !!attrs[:important]}.to_a.flatten)

      redis.hmset("example_child:#{attrs[:id]}:belongs_to",
                  {'example_id' => parent.id}.to_a.flatten)

      redis.sadd("example_child::by_important:#{!!attrs[:important]}", attrs[:id])

      redis.sadd('example_child::ids', attrs[:id])
    end

    it "sets a parent/child has_many relationship between two records in redis" do
      create_example(:id => '8', :name => 'John Jones',
                     :email => 'jjones@example.com', :active => 'true')

      child = Sandstorm::ExampleChild.new(:id => '3', :name => 'Abel Tasman')
      expect(child.save).to be_truthy

      example = Sandstorm::Example.find_by_id('8')
      example.children << child

      expect(redis.keys('*')).to match_array(['example::ids',
                                 'example::by_name',
                                 'example::by_active:true',
                                 'example:8:attrs',
                                 'example:8:children_ids',
                                 'example_child::ids',
                                 'example_child:3:attrs',
                                 'example_child:3:belongs_to'])

      expect(redis.smembers('example::ids')).to eq(['8'])
      expect(redis.smembers('example::by_active:true')).to eq(
        ['8']
      )
      expect(redis.hgetall('example:8:attrs')).to eq(
        {'name' => 'John Jones', 'email' => 'jjones@example.com', 'active' => 'true'}
      )
      expect(redis.smembers('example:8:children_ids')).to eq(['3'])

      expect(redis.smembers('example_child::ids')).to eq(['3'])
      expect(redis.hgetall('example_child:3:attrs')).to eq(
        {'name' => 'Abel Tasman'}
      )
    end

    it "loads a child from a parent's has_many relationship" do
      create_example(:id => '8', :name => 'John Jones',
                     :email => 'jjones@example.com', :active => 'true')
      example = Sandstorm::Example.find_by_id('8')
      create_child(example, :id => '3', :name => 'Abel Tasman')

      children = example.children.all

      expect(children).to be_an(Array)
      expect(children.size).to eq(1)
      child = children.first
      expect(child).to be_a(Sandstorm::ExampleChild)
      expect(child.name).to eq('Abel Tasman')
    end

    it "loads a parent from a child's belongs_to relationship" do
      create_example(:id => '8', :name => 'John Jones',
                     :email => 'jjones@example.com', :active => 'true')
      example = Sandstorm::Example.find_by_id('8')
      create_child(example, :id => '3', :name => 'Abel Tasman')
      child = Sandstorm::ExampleChild.find_by_id('3')

      other_example = child.example
      expect(other_example).not_to be_nil
      expect(other_example).to be_a(Sandstorm::Example)
      expect(other_example.name).to eq('John Jones')
    end

    it "removes a parent/child has_many relationship between two records in redis" do
      create_example(:id => '8', :name => 'John Jones',
                     :email => 'jjones@example.com', :active => 'true')
      example = Sandstorm::Example.find_by_id('8')

      create_child(example, :id => '3', :name => 'Abel Tasman')
      child = Sandstorm::ExampleChild.find_by_id('3')

      expect(redis.smembers('example_child::ids')).to eq(['3'])
      expect(redis.smembers('example:8:children_ids')).to eq(['3'])

      example.children.delete(child)

      expect(redis.smembers('example_child::ids')).to eq(['3'])    # child not deleted
      expect(redis.smembers('example:8:children_ids')).to eq([]) # but association is
    end

    it "filters has_many records by indexed attribute values" do
      create_example(:id => '8', :name => 'John Jones',
                     :email => 'jjones@example.com', :active => 'true')
      example = Sandstorm::Example.find_by_id('8')

      create_child(example, :id => '3', :name => 'Martin Luther King', :important => true)
      create_child(example, :id => '4', :name => 'Julius Caesar', :important => true)
      create_child(example, :id => '5', :name => 'John Smith', :important => false)

      important_kids = example.children.intersect(:important => true).all
      expect(important_kids).not_to be_nil
      expect(important_kids).to be_an(Array)
      expect(important_kids.size).to eq(2)
      expect(important_kids.map(&:id)).to match_array(['3', '4'])
    end

    it "checks whether a record id exists through a has_many filter" do
      create_example(:id => '8', :name => 'John Jones',
                     :email => 'jjones@example.com', :active => 'true')
      example = Sandstorm::Example.find_by_id('8')

      create_child(example, :id => '3', :name => 'Martin Luther King', :important => true)
      create_child(example, :id => '4', :name => 'Julius Caesar', :important => true)
      create_child(example, :id => '5', :name => 'John Smith', :important => false)

      expect(example.children.intersect(:important => true).exists?('3')).to be_truthy
      expect(example.children.intersect(:important => true).exists?('5')).to be_falsey
    end

    it "finds a record through a has_many filter" do
      create_example(:id => '8', :name => 'John Jones',
                     :email => 'jjones@example.com', :active => 'true')
      example = Sandstorm::Example.find_by_id('8')

      create_child(example, :id => '3', :name => 'Martin Luther King', :important => true)
      create_child(example, :id => '4', :name => 'Julius Caesar', :important => true)
      create_child(example, :id => '5', :name => 'John Smith', :important => false)

      martin = example.children.intersect(:important => true).find_by_id('3')
      expect(martin).not_to be_nil
      expect(martin).to be_a(Sandstorm::ExampleChild)
      expect(martin.id).to eq('3')
    end

    it 'clears the belongs_to association when the child record is deleted' do
      create_example(:id => '8', :name => 'John Jones',
                     :email => 'jjones@example.com', :active => 'true')
      example = Sandstorm::Example.find_by_id('8')

      time = Time.now

      create_child(example, :id => '6', :name => 'Martin Luther King', :important => true)
      child = Sandstorm::ExampleChild.find_by_id('6')

      expect(redis.keys).to match_array(['example::ids',
                            'example::by_name',
                            'example::by_active:true',
                            'example:8:attrs',
                            'example:8:children_ids',
                            'example_child::ids',
                            'example_child::by_important:true',
                            'example_child:6:attrs',
                            'example_child:6:belongs_to'])

      child.destroy

      expect(redis.keys).to match_array(['example::ids',
                            'example::by_name',
                            'example::by_active:true',
                            'example:8:attrs'])
    end

    it "clears the belongs_to association when the parent record is deleted" do
      create_example(:id => '8', :name => 'John Jones',
                     :email => 'jjones@example.com', :active => 'true')
      example = Sandstorm::Example.find_by_id('8')

      time = Time.now

      create_child(example, :id => '6', :name => 'Martin Luther King', :important => true)
      child = Sandstorm::ExampleChild.find_by_id('6')

      expect(redis.keys).to match_array(['example::ids',
                            'example::by_name',
                            'example::by_active:true',
                            'example:8:attrs',
                            'example:8:children_ids',
                            'example_child::ids',
                            'example_child::by_important:true',
                            'example_child:6:attrs',
                            'example_child:6:belongs_to'])

      example.destroy

      expect(redis.keys).to match_array(['example_child::ids',
                            'example_child::by_important:true',
                            'example_child:6:attrs'])
    end

  end

  context "has_sorted_set" do

    def create_datum(parent, attrs = {})
      redis.zadd("example:#{parent.id}:data_ids", attrs[:timestamp].to_i.to_f, attrs[:id])

      redis.hmset("example_datum:#{attrs[:id]}:attrs",
                  {'summary' => attrs[:summary], 'timestamp' => attrs[:timestamp].to_i.to_f,
                   'emotion' => attrs[:emotion]}.to_a.flatten)

      redis.sadd("example_datum::by_emotion:#{attrs[:emotion]}", attrs[:id])
      redis.hset("example_datum:#{attrs[:id]}:belongs_to", 'example_id', parent.id)

      redis.sadd('example_datum::ids', attrs[:id])
    end

    it "sets a parent/child has_sorted_set relationship between two records in redis" do
      create_example(:id => '8', :name => 'John Jones',
                     :email => 'jjones@example.com', :active => 'true')

      time = Time.now

      data = Sandstorm::ExampleDatum.new(:id => '4', :timestamp => time,
        :summary => "hello!")
      expect(data.save).to be_truthy

      example = Sandstorm::Example.find_by_id('8')
      example.data << data

      expect(redis.keys('*')).to match_array(['example::ids',
                                 'example::by_name',
                                 'example::by_active:true',
                                 'example:8:attrs',
                                 'example:8:data_ids',
                                 'example_datum::ids',
                                 'example_datum:4:attrs',
                                 'example_datum:4:belongs_to'])

      expect(redis.smembers('example_datum::ids')).to eq(['4'])
      expect(redis.hgetall('example_datum:4:attrs')).to eq(
        {'summary' => 'hello!', 'timestamp' => time.to_f.to_s}
      )
      expect(redis.hgetall('example_datum:4:belongs_to')).to eq(
        {'example_id' => '8'}
      )

      result = redis.zrange('example:8:data_ids', 0, -1,
        :with_scores => true) # .should == [['4', time.to_f]]
      expect(result.size).to eq(1)
      expect(result.first.first).to eq('4')
      expect(result.first.last).to be_within(0.001).of(time.to_f)
    end

    it "loads a child from a parent's has_sorted_set relationship" do
      create_example(:id => '8', :name => 'John Jones',
                     :email => 'jjones@example.com', :active => 'true')
      example = Sandstorm::Example.find_by_id('8')

      time = Time.now

      create_datum(example, :id => '4', :summary => 'well then', :timestamp => time)
      datum = Sandstorm::ExampleDatum.find_by_id('4')

      data = example.data.all

      expect(data).to be_an(Array)
      expect(data.size).to eq(1)
      datum = data.first
      expect(datum).to be_a(Sandstorm::ExampleDatum)
      expect(datum.summary).to eq('well then')
      expect(datum.timestamp).to be_within(1).of(time) # ignore fractional differences
    end

    it "removes a parent/child has_sorted_set relationship between two records in redis" do
      create_example(:id => '8', :name => 'John Jones',
                     :email => 'jjones@example.com', :active => 'true')
      example = Sandstorm::Example.find_by_id('8')

      time = Time.now

      create_datum(example, :id => '4', :summary => 'well then', :timestamp => time)
      datum = Sandstorm::ExampleDatum.find_by_id('4')

      expect(redis.smembers('example_datum::ids')).to eq(['4'])
      expect(redis.zrange('example:8:data_ids', 0, -1)).to eq(['4'])

      example.data.delete(datum)

      expect(redis.smembers('example_datum::ids')).to eq(['4'])        # child not deleted
      expect(redis.zrange('example:8:data_ids', 0, -1)).to eq([]) # but association is
    end

    it "filters has_sorted_set records by indexed attribute values" do
      create_example(:id => '8', :name => 'John Jones',
                     :email => 'jjones@example.com', :active => 'true')
      example = Sandstorm::Example.find_by_id('8')

      time = Time.now

      create_datum(example, :id => '4', :summary => 'well then', :timestamp => time,
        :emotion => 'upset')
      create_datum(example, :id => '5', :summary => 'ok', :timestamp => time.to_i + 10,
        :emotion => 'happy')
      create_datum(example, :id => '6', :summary => 'aaargh', :timestamp => time.to_i + 20,
        :emotion => 'upset')

      upset_data = example.data.intersect(:emotion => 'upset').all
      expect(upset_data).not_to be_nil
      expect(upset_data).to be_an(Array)
      expect(upset_data.size).to eq(2)
      expect(upset_data.map(&:id)).to eq(['4', '6'])
    end

    it "retrieves a subset of a sorted set by index" do
      create_example(:id => '8', :name => 'John Jones',
                     :email => 'jjones@example.com', :active => 'true')
      example = Sandstorm::Example.find_by_id('8')

      time = Time.now

      create_datum(example, :id => '4', :summary => 'well then', :timestamp => time,
        :emotion => 'upset')
      create_datum(example, :id => '5', :summary => 'ok', :timestamp => time.to_i + 10,
        :emotion => 'happy')
      create_datum(example, :id => '6', :summary => 'aaargh', :timestamp => time.to_i + 20,
        :emotion => 'upset')

      data = example.data.intersect_range(0, 1).all
      expect(data).not_to be_nil
      expect(data).to be_an(Array)
      expect(data.size).to eq(2)
      expect(data.map(&:id)).to eq(['4', '5'])
    end

    it "retrieves a reversed subset of a sorted set by index" do
      create_example(:id => '8', :name => 'John Jones',
                     :email => 'jjones@example.com', :active => 'true')
      example = Sandstorm::Example.find_by_id('8')

      time = Time.now

      create_datum(example, :id => '4', :summary => 'well then', :timestamp => time,
        :emotion => 'upset')
      create_datum(example, :id => '5', :summary => 'ok', :timestamp => time.to_i + 10,
        :emotion => 'happy')
      create_datum(example, :id => '6', :summary => 'aaargh', :timestamp => time.to_i + 20,
        :emotion => 'upset')

      data = example.data.intersect_range(0, 1, :order => 'desc').all
      expect(data).not_to be_nil
      expect(data).to be_an(Array)
      expect(data.size).to eq(2)
      expect(data.map(&:id)).to eq(['6', '5'])
    end

    it "retrieves a subset of a sorted set by score" do
      create_example(:id => '8', :name => 'John Jones',
                     :email => 'jjones@example.com', :active => 'true')
      example = Sandstorm::Example.find_by_id('8')

      time = Time.now

      create_datum(example, :id => '4', :summary => 'well then', :timestamp => time,
        :emotion => 'upset')
      create_datum(example, :id => '5', :summary => 'ok', :timestamp => time.to_i + 10,
        :emotion => 'happy')
      create_datum(example, :id => '6', :summary => 'aaargh', :timestamp => time.to_i + 20,
        :emotion => 'upset')

      data = example.data.intersect_range(time.to_i - 1, time.to_i + 15, :by_score => true).all
      expect(data).not_to be_nil
      expect(data).to be_an(Array)
      expect(data.size).to eq(2)
      expect(data.map(&:id)).to eq(['4', '5'])
    end

    it "retrieves a reversed subset of a sorted set by score" do
      create_example(:id => '8', :name => 'John Jones',
                     :email => 'jjones@example.com', :active => 'true')
      example = Sandstorm::Example.find_by_id('8')

      time = Time.now

      create_datum(example, :id => '4', :summary => 'well then', :timestamp => time,
        :emotion => 'upset')
      create_datum(example, :id => '5', :summary => 'ok', :timestamp => time.to_i + 10,
        :emotion => 'happy')
      create_datum(example, :id => '6', :summary => 'aaargh', :timestamp => time.to_i + 20,
        :emotion => 'upset')

      data = example.data.intersect_range(time.to_i - 1, time.to_i + 15,
              :order => 'desc', :by_score => true).all
      expect(data).not_to be_nil
      expect(data).to be_an(Array)
      expect(data.size).to eq(2)
      expect(data.map(&:id)).to eq(['5', '4'])
    end

    it "checks whether a record exists through a has_sorted_set filter" do
      create_example(:id => '8', :name => 'John Jones',
                     :email => 'jjones@example.com', :active => 'true')
      example = Sandstorm::Example.find_by_id('8')

      time = Time.now

      create_datum(example, :id => '4', :summary => 'well then', :timestamp => time,
        :emotion => 'upset')
      create_datum(example, :id => '5', :summary => 'ok', :timestamp => time.to_i + 10,
        :emotion => 'happy')
      create_datum(example, :id => '6', :summary => 'aaargh', :timestamp => time.to_i + 20,
        :emotion => 'upset')

      expect(example.data.intersect(:emotion => 'upset').exists?('4')).to be_truthy
      expect(example.data.intersect(:emotion => 'upset').exists?('5')).to be_falsey
    end

    it "finds a record through a has_sorted_set filter" do
      create_example(:id => '8', :name => 'John Jones',
                     :email => 'jjones@example.com', :active => 'true')
      example = Sandstorm::Example.find_by_id('8')

      time = Time.now

      create_datum(example, :id => '4', :summary => 'well then', :timestamp => time,
        :emotion => 'upset')
      create_datum(example, :id => '5', :summary => 'ok', :timestamp => time.to_i + 10,
        :emotion => 'happy')
      create_datum(example, :id => '6', :summary => 'aaargh', :timestamp => time.to_i + 20,
        :emotion => 'upset')

      wellthen = upset_data = example.data.intersect(:emotion => 'upset').find_by_id('4')
      expect(wellthen).not_to be_nil
      expect(wellthen).to be_a(Sandstorm::ExampleDatum)
      expect(wellthen.id).to eq('4')
    end

    it 'clears the belongs_to association when the child record is deleted' do
      create_example(:id => '8', :name => 'John Jones',
                     :email => 'jjones@example.com', :active => 'true')
      example = Sandstorm::Example.find_by_id('8')

      time = Time.now

      create_datum(example, :id => '6', :summary => 'aaargh', :timestamp => time.to_i + 20,
        :emotion => 'upset')
      datum = Sandstorm::ExampleDatum.find_by_id('6')

      expect(redis.keys).to match_array(['example::ids',
                            'example::by_name',
                            'example::by_active:true',
                            'example:8:attrs',
                            'example:8:data_ids',
                            'example_datum::ids',
                            'example_datum::by_emotion:upset',
                            'example_datum:6:attrs',
                            'example_datum:6:belongs_to'])

      datum.destroy

      expect(redis.keys).to match_array(['example::ids',
                            'example::by_name',
                            'example::by_active:true',
                            'example:8:attrs'])
    end

    it "clears the belongs_to association when the parent record is deleted" do
      create_example(:id => '8', :name => 'John Jones',
                     :email => 'jjones@example.com', :active => 'true')
      example = Sandstorm::Example.find_by_id('8')

      time = Time.now

      create_datum(example, :id => '6', :summary => 'aaargh', :timestamp => time.to_i + 20,
        :emotion => 'upset')

      expect(redis.keys).to match_array(['example::ids',
                            'example::by_name',
                            'example::by_active:true',
                            'example:8:attrs',
                            'example:8:data_ids',
                            'example_datum::ids',
                            'example_datum::by_emotion:upset',
                            'example_datum:6:attrs',
                            'example_datum:6:belongs_to'])

      example.destroy

      expect(redis.keys).to match_array(['example_datum::ids',
                            'example_datum::by_emotion:upset',
                            'example_datum:6:attrs'])
    end

  end

  context "has_one" do

    class Sandstorm::ExampleSpecial
      include Sandstorm::Record

      define_attributes :name => :string

      belongs_to :example, :class_name => 'Sandstorm::Example', :inverse_of => :special

      validate :name, :presence => true
    end

    class Sandstorm::Example
      has_one :special, :class_name => 'Sandstorm::ExampleSpecial'
    end

    it "sets and retrives a record via a has_one association" do
      create_example(:id => '8', :name => 'John Jones',
                     :email => 'jjones@example.com', :active => 'true')

      special = Sandstorm::ExampleSpecial.new(:id => '22', :name => 'Bill Smith')
      expect(special.save).to be_truthy

      example = Sandstorm::Example.find_by_id('8')
      example.special = special

      expect(redis.keys('*')).to match_array(['example::ids',
                                 'example::by_name',
                                 'example::by_active:true',
                                 'example:8:attrs',
                                 'example:8:special_id',
                                 'example_special::ids',
                                 'example_special:22:attrs',
                                 'example_special:22:belongs_to'])

      expect(redis.get('example:8:special_id')).to eq('22')

      expect(redis.smembers('example_special::ids')).to eq(['22'])
      expect(redis.hgetall('example_special:22:attrs')).to eq(
        {'name' => 'Bill Smith'}
      )

      expect(redis.hgetall('example_special:22:belongs_to')).to eq(
        {'example_id' => '8'}
      )

      example2 = Sandstorm::Example.find_by_id('8')
      special2 = example2.special
      expect(special2).not_to be_nil
      expect(special2.id).to eq('22')
      expect(special2.example.id).to eq('8')
    end

    def create_special(parent, attrs = {})
      redis.hmset("example_special:#{attrs[:id]}:attrs", {'name' => attrs[:name]}.to_a.flatten)

      redis.hset("example_special:#{attrs[:id]}:belongs_to", 'example_id', parent.id)
      redis.set("example:#{parent.id}:special_id", attrs[:id])

      redis.sadd('example_special::ids', attrs[:id])
    end

    it 'clears the belongs_to association when the child record is deleted' do
      create_example(:id => '8', :name => 'John Jones',
                     :email => 'jjones@example.com', :active => 'true')
      example = Sandstorm::Example.find_by_id('8')
      create_special(example, :id => '3', :name => 'Another Jones')
      special = Sandstorm::ExampleSpecial.find_by_id('3')

      expect(redis.keys).to match_array(['example::ids',
                            'example::by_name',
                            'example::by_active:true',
                            'example:8:attrs',
                            'example:8:special_id',
                            'example_special::ids',
                            'example_special:3:attrs',
                            'example_special:3:belongs_to'])

      special.destroy

      expect(redis.keys).to match_array(['example::ids',
                            'example::by_name',
                            'example::by_active:true',
                            'example:8:attrs'])
    end

    it "clears the belongs_to association when the parent record is deleted" do
      create_example(:id => '8', :name => 'John Jones',
                     :email => 'jjones@example.com', :active => 'true')
      example = Sandstorm::Example.find_by_id('8')
      create_special(example, :id => '3', :name => 'Another Jones')

      expect(redis.keys).to match_array(['example::ids',
                            'example::by_name',
                            'example::by_active:true',
                            'example:8:attrs',
                            'example:8:special_id',
                            'example_special::ids',
                            'example_special:3:attrs',
                            'example_special:3:belongs_to'])

      example.destroy

      expect(redis.keys).to match_array(['example_special::ids',
                            'example_special:3:attrs'])
    end

  end

  context "has_and_belongs_to_many" do

    def create_template(attrs = {})
      redis.hmset("template:#{attrs[:id]}:attrs", {'name' => attrs[:name]}.to_a.flatten)
      redis.sadd('template::ids', attrs[:id])
    end

    before(:each) do
      create_example(:id => '8', :name => 'John Jones',
                     :email => 'jjones@example.com', :active => true)
      create_template(:id => '2', :name => 'Template 1')
    end

    it "sets a has_and_belongs_to_many relationship between two records in redis" do
      example = Sandstorm::Example.find_by_id('8')
      template = Sandstorm::Template.find_by_id('2')

      example.templates << template

      expect(redis.keys('*')).to match_array(['example::ids',
                                 'example::by_name',
                                 'example::by_active:true',
                                 'example:8:attrs',
                                 'example:8:templates_ids',
                                 'template::ids',
                                 'template:2:attrs',
                                 'template:2:examples_ids'])

      expect(redis.smembers('example::ids')).to eq(['8'])
      expect(redis.smembers('example::by_active:true')).to eq(['8'])
      expect(redis.hgetall('example:8:attrs')).to eq(
        {'name' => 'John Jones', 'email' => 'jjones@example.com', 'active' => 'true'}
      )
      expect(redis.smembers('example:8:templates_ids')).to eq(['2'])

      expect(redis.smembers('template::ids')).to eq(['2'])
      expect(redis.hgetall('template:2:attrs')).to eq({'name' => 'Template 1'})
      expect(redis.smembers('template:2:examples_ids')).to eq(['8'])
    end

    it "loads a record from a has_and_belongs_to_many relationship" do
      example = Sandstorm::Example.find_by_id('8')
      template = Sandstorm::Template.find_by_id('2')

      template.examples << example

      templates = example.templates.all

      expect(templates).to be_an(Array)
      expect(templates.size).to eq(1)
      other_template = templates.first
      expect(other_template).to be_a(Sandstorm::Template)
      expect(other_template.id).to eq(template.id)
    end

    it "removes a has_and_belongs_to_many relationship between two records in redis" do
      example = Sandstorm::Example.find_by_id('8')
      template = Sandstorm::Template.find_by_id('2')

      template.examples << example

      expect(redis.smembers('template::ids')).to eq(['2'])
      expect(redis.smembers('example:8:templates_ids')).to eq(['2'])

      example.templates.delete(template)

      expect(redis.smembers('template::ids')).to eq(['2'])        # template not deleted
      expect(redis.smembers('example:8:templates_ids')).to eq([]) # but association is
    end

    it "filters has_and_belongs_to_many records by indexed attribute values" do
      create_example(:id => '9', :name => 'James Smith',
                     :email => 'jsmith@example.com', :active => false)
      create_example(:id => '10', :name => 'Alpha Beta',
                     :email => 'abc@example.com', :active => true)

      example = Sandstorm::Example.find_by_id('8')
      example_2 = Sandstorm::Example.find_by_id('9')
      example_3 = Sandstorm::Example.find_by_id('10')
      template = Sandstorm::Template.find_by_id('2')

      example.templates << template
      example_2.templates << template
      example_3.templates << template

      examples = template.examples.intersect(:active => true).all
      expect(examples).not_to be_nil
      expect(examples).to be_an(Array)
      expect(examples.size).to eq(2)
      expect(examples.map(&:id)).to match_array(['8', '10'])
    end

    it "checks whether a record id exists through a has_and_belongs_to_many filter"  do
      create_example(:id => '9', :name => 'James Smith',
                     :email => 'jsmith@example.com', :active => false)

      example = Sandstorm::Example.find_by_id('8')
      example_2 = Sandstorm::Example.find_by_id('9')
      template = Sandstorm::Template.find_by_id('2')

      example.templates << template
      example_2.templates << template

      expect(template.examples.intersect(:active => false).exists?('9')).to be_truthy
      expect(template.examples.intersect(:active => false).exists?('8')).to be_falsey
    end

    it "finds a record through a has_and_belongs_to_many filter" do
      create_example(:id => '9', :name => 'James Smith',
                     :email => 'jsmith@example.com', :active => false)

      example = Sandstorm::Example.find_by_id('8')
      example_2 = Sandstorm::Example.find_by_id('9')
      template = Sandstorm::Template.find_by_id('2')

      example.templates << template
      example_2.templates << template

      james = template.examples.intersect(:active => false).find_by_id('9')
      expect(james).not_to be_nil
      expect(james).to be_a(Sandstorm::Example)
      expect(james.id).to eq(example_2.id)
    end

    it 'clears a has_and_belongs_to_many association when a record is deleted'

  end

  context 'bad parameters' do

    let(:example) { Sandstorm::Example.find_by_id('8') }

    before(:each) do
      create_example(:id => '8', :name => 'John Jones',
                     :email => 'jjones@example.com', :active => true)
    end

    it 'raises an error when calling add on has_many without an argument' do
      expect {
        example.children.add
      }.to raise_error
    end

    it 'raises an error when calling delete on has_many without an argument' do
      expect {
        example.children.delete
      }.to raise_error
    end

    it 'raises an error when calling add on has_sorted_set without an argument' do
      expect {
        example.data.add
      }.to raise_error
    end

    it 'raises an error when calling delete on has_sorted_set without an argument' do
      expect {
        example.data.delete
      }.to raise_error
    end

    it 'raises an error when calling add on has_and_belongs_to_many without an argument' do
      expect {
        example.templates.add
      }.to raise_error
    end

    it 'raises an error when calling delete on has_and_belongs_to_many without an argument' do
      expect {
        example.templates.delete
      }.to raise_error
    end

    it 'raises an error when trying to filter on a non-indexed value' do
      expect {
        Sandstorm::Example.intersect(:email => 'jjones@example.com').all
      }.to raise_error
    end
  end

end
