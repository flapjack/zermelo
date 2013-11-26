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

      index_by :active
      unique_index_by :name
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
    example.should_not be_valid

    errs = example.errors
    errs.should_not be_nil
    errs[:name].should == ["can't be blank"]
  end

  it "adds a record's attributes to redis" do
    example = Sandstorm::Example.new(:id => '1', :name => 'John Smith',
      :email => 'jsmith@example.com', :active => true)
    example.should be_valid
    example.save.should be_true

    redis.keys('*').should =~ ['example::ids',
                               'example:1:attrs',
                               'example::by_name',
                               'example::by_active:true']
    redis.smembers('example::ids').should == ['1']
    redis.hgetall('example:1:attrs').should ==
      {'name' => 'John Smith', 'email' => 'jsmith@example.com', 'active' => 'true'}
    redis.hgetall('example::by_name').should == {'John Smith' => '1'}
    redis.smembers('example::by_active:true').should ==
      ['1']
  end

  it "finds a record by id in redis" do
    create_example(:id => '8', :name => 'John Jones',
                   :email => 'jjones@example.com', :active => 'true')

    example = Sandstorm::Example.find_by_id('8')
    example.should_not be_nil
    example.id.should == '8'
    example.name.should == 'John Jones'
    example.email.should == 'jjones@example.com'
  end

  it "finds records by an indexed value in redis" do
    create_example(:id => '8', :name => 'John Jones',
                   :email => 'jjones@example.com', :active => 'true')

    examples = Sandstorm::Example.intersect(:active => true).all
    examples.should_not be_nil
    examples.should be_an(Array)
    examples.should have(1).example
    example = examples.first
    example.id.should == '8'
    example.name.should == 'John Jones'
    example.email.should == 'jjones@example.com'
  end

  it "finds records by a uniquely indexed value in redis" do
    create_example(:id => '8', :name => 'John Jones',
                   :email => 'jjones@example.com', :active => 'true')

    examples = Sandstorm::Example.intersect(:name => 'John Jones').all
    examples.should_not be_nil
    examples.should be_an(Array)
    examples.should have(1).example
    example = examples.first
    example.id.should == '8'
    example.name.should == 'John Jones'
    example.email.should == 'jjones@example.com'
  end

  it "updates a record's attributes in redis" do
    create_example(:id => '8', :name => 'John Jones',
                   :email => 'jjones@example.com', :active => 'true')

    example = Sandstorm::Example.find_by_id('8')
    example.name = 'Jane Janes'
    example.email = 'jjanes@example.com'
    example.save.should be_true

    redis.keys('*').should =~ ['example::ids',
                               'example:8:attrs',
                               'example::by_name',
                               'example::by_active:true']
    redis.smembers('example::ids').should == ['8']
    redis.hgetall('example:8:attrs').should ==
      {'name' => 'Jane Janes', 'email' => 'jjanes@example.com', 'active' => 'true'}
    redis.smembers('example::by_active:true').should ==
      ['8']
  end

  it "deletes a record's attributes from redis" do
    create_example(:id => '8', :name => 'John Jones',
                   :email => 'jjones@example.com', :active => 'true')

    redis.keys('*').should =~ ['example::ids',
                               'example:8:attrs',
                               'example::by_name',
                               'example::by_active:true']

    example = Sandstorm::Example.find_by_id('8')
    example.destroy

    redis.keys('*').should == []
  end

  it "resets changed state on refresh" do
    create_example(:id => '8', :name => 'John Jones',
                   :email => 'jjones@example.com', :active => 'true')
    example = Sandstorm::Example.find_by_id('8')

    example.name = "King Henry VIII"
    example.changed.should include('name')
    example.changes.should == {'name' => ['John Jones', 'King Henry VIII']}

    example.refresh
    example.changed.should be_empty
    example.changes.should be_empty
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
      example.should_not be_nil
      example.should be_an(Array)
      example.should have(1).example
      example.map(&:id).should == ['8']
    end

    it 'supports sequential intersection and union operations' do
      example = Sandstorm::Example.intersect(:active => true).union(:active => false).all
      example.should_not be_nil
      example.should be_an(Array)
      example.should have(2).examples
      example.map(&:id).should =~ ['8', '9']
    end

    it 'allows intersection operations across multiple values for an attribute'

    it 'allows union operations across multiple values for an attribute'

    it 'excludes particular records' do
      example = Sandstorm::Example.diff(:active => true).all
      example.should_not be_nil
      example.should be_an(Array)
      example.should have(1).example
      example.map(&:id).should == ['9']
    end

  end


  context "has_many" do

    class Sandstorm::ExampleChild
      include Sandstorm::Record

      define_attributes :name => :string,
                        :important => :boolean

      index_by :important

      belongs_to :example, :class_name => 'Sandstorm::Example', :inverse_of => :children

      validates :name, :presence => true
    end

    class Sandstorm::Example
      has_many :children, :class_name => 'Sandstorm::ExampleChild'
    end

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
      child.save.should be_true

      example = Sandstorm::Example.find_by_id('8')
      example.children << child

      redis.keys('*').should =~ ['example::ids',
                                 'example::by_name',
                                 'example::by_active:true',
                                 'example:8:attrs',
                                 'example:8:children_ids',
                                 'example_child::ids',
                                 'example_child:3:attrs',
                                 'example_child:3:belongs_to']

      redis.smembers('example::ids').should == ['8']
      redis.smembers('example::by_active:true').should ==
        ['8']
      redis.hgetall('example:8:attrs').should ==
        {'name' => 'John Jones', 'email' => 'jjones@example.com', 'active' => 'true'}
      redis.smembers('example:8:children_ids').should == ['3']

      redis.smembers('example_child::ids').should == ['3']
      redis.hgetall('example_child:3:attrs').should ==
        {'name' => 'Abel Tasman'}
    end

    it "loads a child from a parent's has_many relationship" do
      create_example(:id => '8', :name => 'John Jones',
                     :email => 'jjones@example.com', :active => 'true')
      example = Sandstorm::Example.find_by_id('8')
      create_child(example, :id => '3', :name => 'Abel Tasman')

      children = example.children.all

      children.should be_an(Array)
      children.should have(1).child
      child = children.first
      child.should be_a(Sandstorm::ExampleChild)
      child.name.should == 'Abel Tasman'
    end

    it "loads a parent from a child's belongs_to relationship" do
      create_example(:id => '8', :name => 'John Jones',
                     :email => 'jjones@example.com', :active => 'true')
      example = Sandstorm::Example.find_by_id('8')
      create_child(example, :id => '3', :name => 'Abel Tasman')
      child = Sandstorm::ExampleChild.find_by_id('3')

      other_example = child.example
      other_example.should_not be_nil
      other_example.should be_a(Sandstorm::Example)
      other_example.name.should == 'John Jones'
    end

    it "removes a parent/child has_many relationship between two records in redis" do
      create_example(:id => '8', :name => 'John Jones',
                     :email => 'jjones@example.com', :active => 'true')
      example = Sandstorm::Example.find_by_id('8')

      create_child(example, :id => '3', :name => 'Abel Tasman')
      child = Sandstorm::ExampleChild.find_by_id('3')

      redis.smembers('example_child::ids').should == ['3']
      redis.smembers('example:8:children_ids').should == ['3']

      example.children.delete(child)

      redis.smembers('example_child::ids').should == ['3']    # child not deleted
      redis.smembers('example:8:children_ids').should == [] # but association is
    end

    it "filters has_many records by indexed attribute values" do
      create_example(:id => '8', :name => 'John Jones',
                     :email => 'jjones@example.com', :active => 'true')
      example = Sandstorm::Example.find_by_id('8')

      create_child(example, :id => '3', :name => 'Martin Luther King', :important => true)
      create_child(example, :id => '4', :name => 'Julius Caesar', :important => true)
      create_child(example, :id => '5', :name => 'John Smith', :important => false)

      important_kids = example.children.intersect(:important => true).all
      important_kids.should_not be_nil
      important_kids.should be_an(Array)
      important_kids.should have(2).children
      important_kids.map(&:id).should =~ ['3', '4']
    end

    it "checks whether a record id exists through a has_many filter" do
      create_example(:id => '8', :name => 'John Jones',
                     :email => 'jjones@example.com', :active => 'true')
      example = Sandstorm::Example.find_by_id('8')

      create_child(example, :id => '3', :name => 'Martin Luther King', :important => true)
      create_child(example, :id => '4', :name => 'Julius Caesar', :important => true)
      create_child(example, :id => '5', :name => 'John Smith', :important => false)

      example.children.intersect(:important => true).exists?('3').should be_true
      example.children.intersect(:important => true).exists?('5').should be_false
    end

    it "finds a record through a has_many filter" do
      create_example(:id => '8', :name => 'John Jones',
                     :email => 'jjones@example.com', :active => 'true')
      example = Sandstorm::Example.find_by_id('8')

      create_child(example, :id => '3', :name => 'Martin Luther King', :important => true)
      create_child(example, :id => '4', :name => 'Julius Caesar', :important => true)
      create_child(example, :id => '5', :name => 'John Smith', :important => false)

      martin = example.children.intersect(:important => true).find_by_id('3')
      martin.should_not be_nil
      martin.should be_a(Sandstorm::ExampleChild)
      martin.id.should == '3'
    end

    it 'clears the belongs_to association when the child record is deleted' do
      create_example(:id => '8', :name => 'John Jones',
                     :email => 'jjones@example.com', :active => 'true')
      example = Sandstorm::Example.find_by_id('8')

      time = Time.now

      create_child(example, :id => '6', :name => 'Martin Luther King', :important => true)
      child = Sandstorm::ExampleChild.find_by_id('6')

      redis.keys.should =~ ['example::ids',
                            'example::by_name',
                            'example::by_active:true',
                            'example:8:attrs',
                            'example:8:children_ids',
                            'example_child::ids',
                            'example_child::by_important:true',
                            'example_child:6:attrs',
                            'example_child:6:belongs_to']

      child.destroy

      redis.keys.should =~ ['example::ids',
                            'example::by_name',
                            'example::by_active:true',
                            'example:8:attrs']
    end

    it "clears the belongs_to association when the parent record is deleted" do
      create_example(:id => '8', :name => 'John Jones',
                     :email => 'jjones@example.com', :active => 'true')
      example = Sandstorm::Example.find_by_id('8')

      time = Time.now

      create_child(example, :id => '6', :name => 'Martin Luther King', :important => true)
      child = Sandstorm::ExampleChild.find_by_id('6')

      redis.keys.should =~ ['example::ids',
                            'example::by_name',
                            'example::by_active:true',
                            'example:8:attrs',
                            'example:8:children_ids',
                            'example_child::ids',
                            'example_child::by_important:true',
                            'example_child:6:attrs',
                            'example_child:6:belongs_to']

      example.destroy

      redis.keys.should =~ ['example_child::ids',
                            'example_child::by_important:true',
                            'example_child:6:attrs']
    end

  end

  context "has_sorted_set" do

    class Sandstorm::ExampleDatum
      include Sandstorm::Record

      define_attributes :timestamp => :timestamp,
                        :summary => :string,
                        :emotion => :string

      belongs_to :example, :class_name => 'Sandstorm::Example', :inverse_of => :data

      index_by :emotion

      validates :timestamp, :presence => true
    end

    class Sandstorm::Example
      has_sorted_set :data, :class_name => 'Sandstorm::ExampleDatum',
        :key => :timestamp
    end

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
      data.save.should be_true

      example = Sandstorm::Example.find_by_id('8')
      example.data << data

      redis.keys('*').should =~ ['example::ids',
                                 'example::by_name',
                                 'example::by_active:true',
                                 'example:8:attrs',
                                 'example:8:data_ids',
                                 'example_datum::ids',
                                 'example_datum:4:attrs',
                                 'example_datum:4:belongs_to']

      redis.smembers('example_datum::ids').should == ['4']
      redis.hgetall('example_datum:4:attrs').should ==
        {'summary' => 'hello!', 'timestamp' => time.to_f.to_s}
      redis.hgetall('example_datum:4:belongs_to').should ==
        {'example_id' => '8'}

      result = redis.zrange('example:8:data_ids', 0, -1,
        :with_scores => true) # .should == [['4', time.to_f]]
      result.should have(1).pair
      result.first.first.should == '4'
      result.first.last.should be_within(0.001).of(time.to_f)
    end

    it "loads a child from a parent's has_sorted_set relationship" do
      create_example(:id => '8', :name => 'John Jones',
                     :email => 'jjones@example.com', :active => 'true')
      example = Sandstorm::Example.find_by_id('8')

      time = Time.now

      create_datum(example, :id => '4', :summary => 'well then', :timestamp => time)
      datum = Sandstorm::ExampleDatum.find_by_id('4')

      data = example.data.all

      data.should be_an(Array)
      data.should have(1).datum
      datum = data.first
      datum.should be_a(Sandstorm::ExampleDatum)
      datum.summary.should == 'well then'
      datum.timestamp.should be_within(1).of(time) # ignore fractional differences
    end

    it "removes a parent/child has_sorted_set relationship between two records in redis" do
      create_example(:id => '8', :name => 'John Jones',
                     :email => 'jjones@example.com', :active => 'true')
      example = Sandstorm::Example.find_by_id('8')

      time = Time.now

      create_datum(example, :id => '4', :summary => 'well then', :timestamp => time)
      datum = Sandstorm::ExampleDatum.find_by_id('4')

      redis.smembers('example_datum::ids').should == ['4']
      redis.zrange('example:8:data_ids', 0, -1).should == ['4']

      example.data.delete(datum)

      redis.smembers('example_datum::ids').should == ['4']        # child not deleted
      redis.zrange('example:8:data_ids', 0, -1).should == [] # but association is
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
      upset_data.should_not be_nil
      upset_data.should be_an(Array)
      upset_data.should have(2).children
      upset_data.map(&:id).should == ['4', '6']
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
      data.should_not be_nil
      data.should be_an(Array)
      data.should have(2).children
      data.map(&:id).should == ['4', '5']
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
      data.should_not be_nil
      data.should be_an(Array)
      data.should have(2).children
      data.map(&:id).should == ['6', '5']
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
      data.should_not be_nil
      data.should be_an(Array)
      data.should have(2).children
      data.map(&:id).should == ['4', '5']
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
      data.should_not be_nil
      data.should be_an(Array)
      data.should have(2).children
      data.map(&:id).should == ['5', '4']
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

      example.data.intersect(:emotion => 'upset').exists?('4').should be_true
      example.data.intersect(:emotion => 'upset').exists?('5').should be_false
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
      wellthen.should_not be_nil
      wellthen.should be_a(Sandstorm::ExampleDatum)
      wellthen.id.should == '4'
    end

    it 'clears the belongs_to association when the child record is deleted' do
      create_example(:id => '8', :name => 'John Jones',
                     :email => 'jjones@example.com', :active => 'true')
      example = Sandstorm::Example.find_by_id('8')

      time = Time.now

      create_datum(example, :id => '6', :summary => 'aaargh', :timestamp => time.to_i + 20,
        :emotion => 'upset')
      datum = Sandstorm::ExampleDatum.find_by_id('6')

      redis.keys.should =~ ['example::ids',
                            'example::by_name',
                            'example::by_active:true',
                            'example:8:attrs',
                            'example:8:data_ids',
                            'example_datum::ids',
                            'example_datum::by_emotion:upset',
                            'example_datum:6:attrs',
                            'example_datum:6:belongs_to']

      datum.destroy

      redis.keys.should =~ ['example::ids',
                            'example::by_name',
                            'example::by_active:true',
                            'example:8:attrs']
    end

    it "clears the belongs_to association when the parent record is deleted" do
      create_example(:id => '8', :name => 'John Jones',
                     :email => 'jjones@example.com', :active => 'true')
      example = Sandstorm::Example.find_by_id('8')

      time = Time.now

      create_datum(example, :id => '6', :summary => 'aaargh', :timestamp => time.to_i + 20,
        :emotion => 'upset')

      redis.keys.should =~ ['example::ids',
                            'example::by_name',
                            'example::by_active:true',
                            'example:8:attrs',
                            'example:8:data_ids',
                            'example_datum::ids',
                            'example_datum::by_emotion:upset',
                            'example_datum:6:attrs',
                            'example_datum:6:belongs_to']

      example.destroy

      redis.keys.should =~ ['example_datum::ids',
                            'example_datum::by_emotion:upset',
                            'example_datum:6:attrs']
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
      special.save.should be_true

      example = Sandstorm::Example.find_by_id('8')
      example.special = special

      redis.keys('*').should =~ ['example::ids',
                                 'example::by_name',
                                 'example::by_active:true',
                                 'example:8:attrs',
                                 'example:8:special_id',
                                 'example_special::ids',
                                 'example_special:22:attrs',
                                 'example_special:22:belongs_to']

      redis.get('example:8:special_id').should == '22'

      redis.smembers('example_special::ids').should == ['22']
      redis.hgetall('example_special:22:attrs').should ==
        {'name' => 'Bill Smith'}

      redis.hgetall('example_special:22:belongs_to').should ==
        {'example_id' => '8'}

      example2 = Sandstorm::Example.find_by_id('8')
      special2 = example2.special
      special2.should_not be_nil
      special2.id.should == '22'
      special2.example.id.should == '8'
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

      redis.keys.should =~ ['example::ids',
                            'example::by_name',
                            'example::by_active:true',
                            'example:8:attrs',
                            'example:8:special_id',
                            'example_special::ids',
                            'example_special:3:attrs',
                            'example_special:3:belongs_to']

      special.destroy

      redis.keys.should =~ ['example::ids',
                            'example::by_name',
                            'example::by_active:true',
                            'example:8:attrs']
    end

    it "clears the belongs_to association when the parent record is deleted" do
      create_example(:id => '8', :name => 'John Jones',
                     :email => 'jjones@example.com', :active => 'true')
      example = Sandstorm::Example.find_by_id('8')
      create_special(example, :id => '3', :name => 'Another Jones')

      redis.keys.should =~ ['example::ids',
                            'example::by_name',
                            'example::by_active:true',
                            'example:8:attrs',
                            'example:8:special_id',
                            'example_special::ids',
                            'example_special:3:attrs',
                            'example_special:3:belongs_to']

      example.destroy

      redis.keys.should =~ ['example_special::ids',
                            'example_special:3:attrs']
    end

  end

  context "has_and_belongs_to_many" do

    class Sandstorm::Template
      include Sandstorm::Record

      define_attributes :name => :string

      has_and_belongs_to_many :examples, :class_name => 'Sandstorm::Example',
        :inverse_of => :templates

      validates :name, :presence => true
    end

    class Sandstorm::Example
      has_and_belongs_to_many :templates, :class_name => 'Sandstorm::Template',
        :inverse_of => :examples
    end

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

      redis.keys('*').should =~ ['example::ids',
                                 'example::by_name',
                                 'example::by_active:true',
                                 'example:8:attrs',
                                 'example:8:templates_ids',
                                 'template::ids',
                                 'template:2:attrs',
                                 'template:2:examples_ids']

      redis.smembers('example::ids').should == ['8']
      redis.smembers('example::by_active:true').should == ['8']
      redis.hgetall('example:8:attrs').should ==
        {'name' => 'John Jones', 'email' => 'jjones@example.com', 'active' => 'true'}
      redis.smembers('example:8:templates_ids').should == ['2']

      redis.smembers('template::ids').should == ['2']
      redis.hgetall('template:2:attrs').should == {'name' => 'Template 1'}
      redis.smembers('template:2:examples_ids').should == ['8']
    end

    it "loads a record from a has_and_belongs_to_many relationship" do
      example = Sandstorm::Example.find_by_id('8')
      template = Sandstorm::Template.find_by_id('2')

      template.examples << example

      templates = example.templates.all

      templates.should be_an(Array)
      templates.should have(1).template
      other_template = templates.first
      other_template.should be_a(Sandstorm::Template)
      other_template.id.should == template.id
    end

    it "removes a has_and_belongs_to_many relationship between two records in redis" do
      example = Sandstorm::Example.find_by_id('8')
      template = Sandstorm::Template.find_by_id('2')

      template.examples << example

      redis.smembers('template::ids').should == ['2']
      redis.smembers('example:8:templates_ids').should == ['2']

      example.templates.delete(template)

      redis.smembers('template::ids').should == ['2']        # template not deleted
      redis.smembers('example:8:templates_ids').should == [] # but association is
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
      examples.should_not be_nil
      examples.should be_an(Array)
      examples.should have(2).records
      examples.map(&:id).should =~ ['8', '10']
    end

    it "checks whether a record id exists through a has_and_belongs_to_many filter"  do
      create_example(:id => '9', :name => 'James Smith',
                     :email => 'jsmith@example.com', :active => false)

      example = Sandstorm::Example.find_by_id('8')
      example_2 = Sandstorm::Example.find_by_id('9')
      template = Sandstorm::Template.find_by_id('2')

      example.templates << template
      example_2.templates << template

      template.examples.intersect(:active => false).exists?('9').should be_true
      template.examples.intersect(:active => false).exists?('8').should be_false
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
      james.should_not be_nil
      james.should be_a(Sandstorm::Example)
      james.id.should == example_2.id
    end

    it 'clears a has_and_belongs_to_many association when a record is deleted'

  end


end
