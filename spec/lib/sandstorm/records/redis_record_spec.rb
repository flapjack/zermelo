require 'spec_helper'
require 'sandstorm/records/redis_record'

# NB: also covers associations.rb, which is mixed in to Sandstorm::Record

describe Sandstorm::Records::RedisRecord, :redis => true do

  module Sandstorm
    class RedisExample
      include Sandstorm::Records::RedisRecord

      define_attributes :name   => :string,
                        :email  => :string,
                        :active => :boolean

      validates :name, :presence => true

      has_many :children, :class_name => 'Sandstorm::RedisExampleChild',
        :inverse_of => :example, :before_add => :fail_if_roger

      has_sorted_set :data, :class_name => 'Sandstorm::RedisExampleDatum',
        :key => :timestamp, :inverse_of => :example

      has_and_belongs_to_many :templates, :class_name => 'Sandstorm::Template',
        :inverse_of => :examples

      index_by :active
      unique_index_by :name

      def fail_if_roger(*childs)
        raise "Not adding child" if childs.any? {|c| 'Roger'.eql?(c.name) }
      end
    end

    class RedisExampleChild
      include Sandstorm::Records::RedisRecord

      define_attributes :name => :string,
                        :important => :boolean

      index_by :important

      belongs_to :example, :class_name => 'Sandstorm::RedisExample', :inverse_of => :children

      validates :name, :presence => true
    end

    class RedisExampleDatum
      include Sandstorm::Records::RedisRecord

      define_attributes :timestamp => :timestamp,
                        :summary => :string,
                        :emotion => :string

      belongs_to :example, :class_name => 'Sandstorm::RedisExample', :inverse_of => :data

      index_by :emotion

      validates :timestamp, :presence => true
    end

    class Template
      include Sandstorm::Records::RedisRecord

      define_attributes :name => :string

      has_and_belongs_to_many :examples, :class_name => 'Sandstorm::RedisExample',
        :inverse_of => :templates

      validates :name, :presence => true
    end
  end

  let(:redis) { Sandstorm.redis }

  def create_example(attrs = {})
    redis.hmset("redis_example:#{attrs[:id]}:attrs",
      {'name' => attrs[:name], 'email' => attrs[:email], 'active' => attrs[:active]}.to_a.flatten)
    redis.sadd("redis_example::indices:by_active:boolean:#{!!attrs[:active]}", attrs[:id])
    name = attrs[:name].gsub(/%/, '%%').gsub(/ /, '%20').gsub(/:/, '%3A')
    redis.hset('redis_example::indices:by_name', "string:#{name}", attrs[:id])
    redis.sadd('redis_example::attrs:ids', attrs[:id])
  end

  it "is invalid without a name" do
    example = Sandstorm::RedisExample.new(:id => '1', :email => 'jsmith@example.com')
    expect(example).not_to be_valid

    errs = example.errors
    expect(errs).not_to be_nil
    expect(errs[:name]).to eq(["can't be blank"])
  end

  it "adds a record's attributes to redis" do
    example = Sandstorm::RedisExample.new(:id => '1', :name => 'John Smith',
      :email => 'jsmith@example.com', :active => true)
    expect(example).to be_valid
    expect(example.save).to be_truthy

    expect(redis.keys('*')).to match_array(['redis_example::attrs:ids',
                               'redis_example:1:attrs',
                               'redis_example::indices:by_name',
                               'redis_example::indices:by_active:boolean:true'])
    expect(redis.smembers('redis_example::attrs:ids')).to eq(['1'])
    expect(redis.hgetall('redis_example:1:attrs')).to eq(
      {'name' => 'John Smith', 'email' => 'jsmith@example.com', 'active' => 'true'}
    )
    expect(redis.hgetall('redis_example::indices:by_name')).to eq({'string:John%20Smith' => '1'})
    expect(redis.smembers('redis_example::indices:by_active:boolean:true')).to eq(
      ['1']
    )
  end

  it "finds a record by id in redis" do
    create_example(:id => '8', :name => 'John Jones',
                   :email => 'jjones@example.com', :active => 'true')

    example = Sandstorm::RedisExample.find_by_id('8')
    expect(example).not_to be_nil
    expect(example.id).to eq('8')
    expect(example.name).to eq('John Jones')
    expect(example.email).to eq('jjones@example.com')
  end

  it "finds records by a uniquely indexed value in redis" do
    create_example(:id => '8', :name => 'John Jones',
                   :email => 'jjones@example.com', :active => 'true')

    examples = Sandstorm::RedisExample.intersect(:name => 'John Jones').all
    expect(examples).not_to be_nil
    expect(examples).to be_an(Array)
    expect(examples.size).to eq(1)
    example = examples.first
    expect(example.id).to eq('8')
    expect(example.name).to eq('John Jones')
    expect(example.email).to eq('jjones@example.com')
  end

  it 'finds records by regex match against an indexed value in redis'

  it 'finds records by regex match against a uniquely indexed value in redis' do
    create_example(:id => '8', :name => 'John Jones',
                   :email => 'jjones@example.com', :active => 'true')

    examples = Sandstorm::RedisExample.intersect(:name => /hn Jones/).all
    expect(examples).not_to be_nil
    expect(examples).to be_an(Array)
    expect(examples.size).to eq(1)
    example = examples.first
    expect(example.id).to eq('8')
    expect(example.name).to eq('John Jones')
    expect(example.email).to eq('jjones@example.com')
  end

  it 'cannot find records by regex match against non-string values' do
    create_example(:id => '8', :name => 'John Jones',
                   :email => 'jjones@example.com', :active => true)
    create_example(:id => '9', :name => 'James Brown',
                   :email => 'jbrown@example.com', :active => false)

    expect {
      Sandstorm::RedisExample.intersect(:active => /alse/).all
    }.to raise_error
  end

  it "updates a record's attributes in redis" do
    create_example(:id => '8', :name => 'John Jones',
                   :email => 'jjones@example.com', :active => 'true')

    example = Sandstorm::RedisExample.find_by_id('8')
    example.name = 'Jane Janes'
    example.email = 'jjanes@example.com'
    expect(example.save).to be_truthy

    expect(redis.keys('*')).to match_array(['redis_example::attrs:ids',
                               'redis_example:8:attrs',
                               'redis_example::indices:by_name',
                               'redis_example::indices:by_active:boolean:true'])
    expect(redis.smembers('redis_example::attrs:ids')).to eq(['8'])
    expect(redis.hgetall('redis_example:8:attrs')).to eq(
      {'name' => 'Jane Janes', 'email' => 'jjanes@example.com', 'active' => 'true'}
    )
    expect(redis.smembers('redis_example::indices:by_active:boolean:true')).to eq(
      ['8']
    )
  end

  it "deletes a record's attributes from redis" do
    create_example(:id => '8', :name => 'John Jones',
                   :email => 'jjones@example.com', :active => 'true')

    expect(redis.keys('*')).to match_array(['redis_example::attrs:ids',
                               'redis_example:8:attrs',
                               'redis_example::indices:by_name',
                               'redis_example::indices:by_active:boolean:true'])

    example = Sandstorm::RedisExample.find_by_id('8')
    example.destroy

    expect(redis.keys('*')).to eq([])
  end

  it "resets changed state on refresh" do
    create_example(:id => '8', :name => 'John Jones',
                   :email => 'jjones@example.com', :active => 'true')
    example = Sandstorm::RedisExample.find_by_id('8')

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

  context 'pagination' do

    before do
      create_example(:id => '1', :name => 'mno')
      create_example(:id => '2', :name => 'abc')
      create_example(:id => '3', :name => 'jkl')
      create_example(:id => '4', :name => 'ghi')
      create_example(:id => '5', :name => 'def')
    end

    it "returns paginated query responses" do
      expect(Sandstorm::RedisExample.sort(:id).page(1, :per_page => 3).map(&:id)).to eq(['1','2', '3'])
      expect(Sandstorm::RedisExample.sort(:id).page(2, :per_page => 2).map(&:id)).to eq(['3','4'])
      expect(Sandstorm::RedisExample.sort(:id).page(3, :per_page => 2).map(&:id)).to eq(['5'])
      expect(Sandstorm::RedisExample.sort(:id).page(3, :per_page => 3).map(&:id)).to eq([])

      expect(Sandstorm::RedisExample.sort(:name, :order => 'alpha').page(1, :per_page => 3).map(&:id)).to eq(['2','5', '4'])
      expect(Sandstorm::RedisExample.sort(:name, :order => 'alpha').page(2, :per_page => 2).map(&:id)).to eq(['4','3'])
      expect(Sandstorm::RedisExample.sort(:name, :order => 'alpha').page(3, :per_page => 2).map(&:id)).to eq(['1'])
      expect(Sandstorm::RedisExample.sort(:name, :order => 'alpha').page(3, :per_page => 3).map(&:id)).to eq([])
    end

  end

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
      example = Sandstorm::RedisExample.intersect(:active => true).all
      expect(example).not_to be_nil
      expect(example).to be_an(Array)
      expect(example.size).to eq(1)
      expect(example.map(&:id)).to eq(['8'])
    end

    it 'filters by id attribute values' do
      example = Sandstorm::RedisExample.intersect(:id => '9').all
      expect(example).not_to be_nil
      expect(example).to be_an(Array)
      expect(example.size).to eq(1)
      expect(example.map(&:id)).to eq(['9'])
    end

    it 'supports sequential intersection and union operations' do
      examples = Sandstorm::RedisExample.intersect(:active => true).union(:active => false).all
      expect(examples).not_to be_nil
      expect(examples).to be_an(Array)
      expect(examples.size).to eq(2)
      expect(examples.map(&:id)).to match_array(['8', '9'])
    end

    it 'allows intersection operations across multiple values for an attribute' do
      create_example(:id => '10', :name => 'Jay Johns',
                     :email => 'jjohns@example.com', :active => true)

      examples = Sandstorm::RedisExample.intersect(:name => ['Jay Johns', 'James Brown']).all
      expect(examples).not_to be_nil
      expect(examples).to be_an(Array)
      expect(examples.size).to eq(2)
      expect(examples.map(&:id)).to match_array(['9', '10'])
    end

    it 'allows union operations across multiple values for an attribute' do
      create_example(:id => '10', :name => 'Jay Johns',
                     :email => 'jjohns@example.com', :active => true)

      examples = Sandstorm::RedisExample.intersect(:active => false).union(:name => ['Jay Johns', 'James Brown']).all
      expect(examples).not_to be_nil
      expect(examples).to be_an(Array)
      expect(examples.size).to eq(2)
      expect(examples.map(&:id)).to match_array(['9', '10'])
    end

    it 'filters by multiple id attribute values' do
      create_example(:id => '10', :name => 'Jay Johns',
                     :email => 'jjohns@example.com', :active => true)

      example = Sandstorm::RedisExample.intersect(:id => ['8', '10']).all
      expect(example).not_to be_nil
      expect(example).to be_an(Array)
      expect(example.size).to eq(2)
      expect(example.map(&:id)).to eq(['8', '10'])
    end

    it 'excludes particular records' do
      example = Sandstorm::RedisExample.diff(:active => true).all
      expect(example).not_to be_nil
      expect(example).to be_an(Array)
      expect(example.size).to eq(1)
      expect(example.map(&:id)).to eq(['9'])
    end

    it 'sorts records by an attribute' do
      example = Sandstorm::RedisExample.sort(:name, :order => 'alpha').all
      expect(example).not_to be_nil
      expect(example).to be_an(Array)
      expect(example.size).to eq(2)
      expect(example.map(&:id)).to eq(['9', '8'])
    end

  end


  context "has_many" do

    def create_child(parent, attrs = {})
      redis.sadd("redis_example:#{parent.id}:assocs:children_ids", attrs[:id]) unless parent.nil?

      redis.hmset("redis_example_child:#{attrs[:id]}:attrs",
                  {'name' => attrs[:name], 'important' => !!attrs[:important]}.to_a.flatten)

      redis.hmset("redis_example_child:#{attrs[:id]}:assocs:belongs_to",
                  {'example_id' => parent.id}.to_a.flatten) unless parent.nil?

      redis.sadd("redis_example_child::indices:by_important:boolean:#{!!attrs[:important]}", attrs[:id])

      redis.sadd('redis_example_child::attrs:ids', attrs[:id])
    end

    it "sets a parent/child has_many relationship between two records in redis" do
      create_example(:id => '8', :name => 'John Jones',
                     :email => 'jjones@example.com', :active => 'true')

      child = Sandstorm::RedisExampleChild.new(:id => '3', :name => 'Abel Tasman')
      expect(child.save).to be_truthy

      example = Sandstorm::RedisExample.find_by_id('8')
      example.children << child

      expect(redis.keys('*')).to match_array(['redis_example::attrs:ids',
                                 'redis_example::indices:by_name',
                                 'redis_example::indices:by_active:boolean:true',
                                 'redis_example:8:attrs',
                                 'redis_example:8:assocs:children_ids',
                                 'redis_example_child::attrs:ids',
                                 'redis_example_child::indices:by_important:null:null',
                                 'redis_example_child:3:attrs',
                                 'redis_example_child:3:assocs:belongs_to'])

      expect(redis.smembers('redis_example::attrs:ids')).to eq(['8'])
      expect(redis.smembers('redis_example::indices:by_active:boolean:true')).to eq(
        ['8']
      )
      expect(redis.hgetall('redis_example:8:attrs')).to eq(
        {'name' => 'John Jones', 'email' => 'jjones@example.com', 'active' => 'true'}
      )
      expect(redis.smembers('redis_example:8:assocs:children_ids')).to eq(['3'])

      expect(redis.smembers('redis_example_child::attrs:ids')).to eq(['3'])
      expect(redis.hgetall('redis_example_child:3:attrs')).to eq(
        {'name' => 'Abel Tasman'}
      )
    end

    it "loads a child from a parent's has_many relationship" do
      create_example(:id => '8', :name => 'John Jones',
                     :email => 'jjones@example.com', :active => 'true')
      example = Sandstorm::RedisExample.find_by_id('8')
      create_child(example, :id => '3', :name => 'Abel Tasman')

      children = example.children.all

      expect(children).to be_an(Array)
      expect(children.size).to eq(1)
      child = children.first
      expect(child).to be_a(Sandstorm::RedisExampleChild)
      expect(child.name).to eq('Abel Tasman')
    end

    it "loads a parent from a child's belongs_to relationship" do
      create_example(:id => '8', :name => 'John Jones',
                     :email => 'jjones@example.com', :active => 'true')
      example = Sandstorm::RedisExample.find_by_id('8')
      create_child(example, :id => '3', :name => 'Abel Tasman')
      child = Sandstorm::RedisExampleChild.find_by_id('3')

      other_example = child.example
      expect(other_example).not_to be_nil
      expect(other_example).to be_a(Sandstorm::RedisExample)
      expect(other_example.name).to eq('John Jones')
    end

    it "removes a parent/child has_many relationship between two records in redis" do
      create_example(:id => '8', :name => 'John Jones',
                     :email => 'jjones@example.com', :active => 'true')
      example = Sandstorm::RedisExample.find_by_id('8')

      create_child(example, :id => '3', :name => 'Abel Tasman')
      child = Sandstorm::RedisExampleChild.find_by_id('3')

      expect(redis.smembers('redis_example_child::attrs:ids')).to eq(['3'])
      expect(redis.smembers('redis_example:8:assocs:children_ids')).to eq(['3'])

      example.children.delete(child)

      expect(redis.smembers('redis_example_child::attrs:ids')).to eq(['3'])   # child not deleted
      expect(redis.smembers('redis_example:8:assocs:children_ids')).to eq([]) # but association is
    end

    it "filters has_many records by indexed attribute values" do
      create_example(:id => '8', :name => 'John Jones',
                     :email => 'jjones@example.com', :active => 'true')
      example = Sandstorm::RedisExample.find_by_id('8')

      create_child(example, :id => '3', :name => 'Martin Luther King', :important => true)
      create_child(example, :id => '4', :name => 'Julius Caesar', :important => true)
      create_child(example, :id => '5', :name => 'John Smith', :important => false)

      important_kids = example.children.intersect(:important => true).all
      expect(important_kids).not_to be_nil
      expect(important_kids).to be_an(Array)
      expect(important_kids.size).to eq(2)
      expect(important_kids.map(&:id)).to match_array(['3', '4'])
    end

    it "filters has_many records by intersecting ids" do
      create_example(:id => '8', :name => 'John Jones',
                     :email => 'jjones@example.com', :active => 'true')
      example = Sandstorm::RedisExample.find_by_id('8')

      create_child(example, :id => '3', :name => 'Martin Luther King', :important => true)
      create_child(example, :id => '4', :name => 'Julius Caesar', :important => true)
      create_child(example, :id => '5', :name => 'John Smith', :important => false)

      important_kids = example.children.intersect(:important => true, :id => ['4', '5']).all
      expect(important_kids).not_to be_nil
      expect(important_kids).to be_an(Array)
      expect(important_kids.size).to eq(1)
      expect(important_kids.map(&:id)).to match_array(['4'])
    end

    it "checks whether a record id exists through a has_many filter" do
      create_example(:id => '8', :name => 'John Jones',
                     :email => 'jjones@example.com', :active => 'true')
      example = Sandstorm::RedisExample.find_by_id('8')

      create_child(example, :id => '3', :name => 'Martin Luther King', :important => true)
      create_child(example, :id => '4', :name => 'Julius Caesar', :important => true)
      create_child(example, :id => '5', :name => 'John Smith', :important => false)

      expect(example.children.intersect(:important => true).exists?('3')).to be_truthy
      expect(example.children.intersect(:important => true).exists?('5')).to be_falsey
    end

    it "finds a record through a has_many filter" do
      create_example(:id => '8', :name => 'John Jones',
                     :email => 'jjones@example.com', :active => 'true')
      example = Sandstorm::RedisExample.find_by_id('8')

      create_child(example, :id => '3', :name => 'Martin Luther King', :important => true)
      create_child(example, :id => '4', :name => 'Julius Caesar', :important => true)
      create_child(example, :id => '5', :name => 'John Smith', :important => false)

      martin = example.children.intersect(:important => true).find_by_id('3')
      expect(martin).not_to be_nil
      expect(martin).to be_a(Sandstorm::RedisExampleChild)
      expect(martin.id).to eq('3')
    end

    it "does not add a child if the before_add callback raises an exception" do
      create_example(:id => '8', :name => 'John Jones',
                     :email => 'jjones@example.com', :active => 'true')
      example = Sandstorm::RedisExample.find_by_id('8')

      create_child(nil, :id => '6', :name => 'Roger', :important => true)
      child = Sandstorm::RedisExampleChild.find_by_id('6')

      expect(example.children).to be_empty
      expect {
        example.children << child
      }.to raise_error
      expect(example.children).to be_empty
    end

    it 'clears the belongs_to association when the child record is deleted' do
      create_example(:id => '8', :name => 'John Jones',
                     :email => 'jjones@example.com', :active => 'true')
      example = Sandstorm::RedisExample.find_by_id('8')

      time = Time.now

      create_child(example, :id => '6', :name => 'Martin Luther King', :important => true)
      child = Sandstorm::RedisExampleChild.find_by_id('6')

      expect(redis.keys).to match_array(['redis_example::attrs:ids',
                            'redis_example::indices:by_name',
                            'redis_example::indices:by_active:boolean:true',
                            'redis_example:8:attrs',
                            'redis_example:8:assocs:children_ids',
                            'redis_example_child::attrs:ids',
                            'redis_example_child::indices:by_important:boolean:true',
                            'redis_example_child:6:attrs',
                            'redis_example_child:6:assocs:belongs_to'])

      child.destroy

      expect(redis.keys).to match_array(['redis_example::attrs:ids',
                            'redis_example::indices:by_name',
                            'redis_example::indices:by_active:boolean:true',
                            'redis_example:8:attrs'])
    end

    it "clears the belongs_to association when the parent record is deleted" do
      create_example(:id => '8', :name => 'John Jones',
                     :email => 'jjones@example.com', :active => 'true')
      example = Sandstorm::RedisExample.find_by_id('8')

      time = Time.now

      create_child(example, :id => '6', :name => 'Martin Luther King', :important => true)
      child = Sandstorm::RedisExampleChild.find_by_id('6')

      expect(redis.keys).to match_array(['redis_example::attrs:ids',
                            'redis_example::indices:by_name',
                            'redis_example::indices:by_active:boolean:true',
                            'redis_example:8:attrs',
                            'redis_example:8:assocs:children_ids',
                            'redis_example_child::attrs:ids',
                            'redis_example_child::indices:by_important:boolean:true',
                            'redis_example_child:6:attrs',
                            'redis_example_child:6:assocs:belongs_to'])

      example.destroy

      expect(redis.keys).to match_array(['redis_example_child::attrs:ids',
                            'redis_example_child::indices:by_important:boolean:true',
                            'redis_example_child:6:attrs'])
    end

    it 'returns associated ids for multiple parent ids' do
      create_example(:id => '8', :name => 'John Jones',
                     :email => 'jjones@example.com', :active => 'true')
      example_8 = Sandstorm::RedisExample.find_by_id('8')

      create_example(:id => '9', :name => 'Jane Johnson',
                     :email => 'jjohnson@example.com', :active => 'true')
      example_9 = Sandstorm::RedisExample.find_by_id('9')

      create_example(:id => '10', :name => 'Jim Smith',
                     :email => 'jsmith@example.com', :active => 'true')

      create_child(example_8, :id => '3', :name => 'abc', :important => false)
      create_child(example_9, :id => '4', :name => 'abc', :important => false)
      create_child(example_9, :id => '5', :name => 'abc', :important => false)

      assoc_ids = Sandstorm::RedisExample.intersect(:id => [ '8', '9', '10']).
        associated_ids_for(:children)
      expect(assoc_ids).to eq('8'  => Set.new(['3']),
                              '9'  => Set.new(['4', '5']),
                              '10' => Set.new())

      assoc_parent_ids = Sandstorm::RedisExampleChild.intersect(:id => ['3', '4', '5']).
        associated_ids_for(:example)
      expect(assoc_parent_ids).to eq('3' => '8',
                                     '4' => '9',
                                     '5' => '9')
    end

  end

  context "has_sorted_set" do

    def create_datum(parent, attrs = {})
      redis.zadd("redis_example:#{parent.id}:assocs:data_ids", attrs[:timestamp].to_i.to_f, attrs[:id])

      redis.hmset("redis_example_datum:#{attrs[:id]}:attrs",
                  {'summary' => attrs[:summary], 'timestamp' => attrs[:timestamp].to_i.to_f,
                   'emotion' => attrs[:emotion]}.to_a.flatten)

      redis.sadd("redis_example_datum::indices:by_emotion:string:#{attrs[:emotion]}", attrs[:id])
      redis.hset("redis_example_datum:#{attrs[:id]}:assocs:belongs_to", 'example_id', parent.id)

      redis.sadd('redis_example_datum::attrs:ids', attrs[:id])
    end

    it "sets a parent/child has_sorted_set relationship between two records in redis" do
      create_example(:id => '8', :name => 'John Jones',
                     :email => 'jjones@example.com', :active => 'true')

      time = Time.now

      data = Sandstorm::RedisExampleDatum.new(:id => '4', :timestamp => time,
        :summary => "hello!")
      expect(data.save).to be_truthy

      example = Sandstorm::RedisExample.find_by_id('8')
      example.data << data

      expect(redis.keys('*')).to match_array(['redis_example::attrs:ids',
                                 'redis_example::indices:by_name',
                                 'redis_example::indices:by_active:boolean:true',
                                 'redis_example:8:attrs',
                                 'redis_example:8:assocs:data_ids',
                                 'redis_example_datum::attrs:ids',
                                 'redis_example_datum::indices:by_emotion:null:null',
                                 'redis_example_datum:4:attrs',
                                 'redis_example_datum:4:assocs:belongs_to'])

      expect(redis.smembers('redis_example_datum::attrs:ids')).to eq(['4'])
      expect(redis.hgetall('redis_example_datum:4:attrs')).to eq(
        {'summary' => 'hello!', 'timestamp' => time.to_f.to_s}
      )
      expect(redis.hgetall('redis_example_datum:4:assocs:belongs_to')).to eq(
        {'example_id' => '8'}
      )

      result = redis.zrange('redis_example:8:assocs:data_ids', 0, -1,
        :with_scores => true) # .should == [['4', time.to_f]]
      expect(result.size).to eq(1)
      expect(result.first.first).to eq('4')
      expect(result.first.last).to be_within(0.001).of(time.to_f)
    end

    it "loads a child from a parent's has_sorted_set relationship" do
      create_example(:id => '8', :name => 'John Jones',
                     :email => 'jjones@example.com', :active => 'true')
      example = Sandstorm::RedisExample.find_by_id('8')

      time = Time.now

      create_datum(example, :id => '4', :summary => 'well then', :timestamp => time)
      datum = Sandstorm::RedisExampleDatum.find_by_id('4')

      data = example.data.all

      expect(data).to be_an(Array)
      expect(data.size).to eq(1)
      datum = data.first
      expect(datum).to be_a(Sandstorm::RedisExampleDatum)
      expect(datum.summary).to eq('well then')
      expect(datum.timestamp).to be_within(1).of(time) # ignore fractional differences
    end

    it "removes a parent/child has_sorted_set relationship between two records in redis" do
      create_example(:id => '8', :name => 'John Jones',
                     :email => 'jjones@example.com', :active => 'true')
      example = Sandstorm::RedisExample.find_by_id('8')

      time = Time.now

      create_datum(example, :id => '4', :summary => 'well then', :timestamp => time)
      datum = Sandstorm::RedisExampleDatum.find_by_id('4')

      expect(redis.smembers('redis_example_datum::attrs:ids')).to eq(['4'])
      expect(redis.zrange('redis_example:8:assocs:data_ids', 0, -1)).to eq(['4'])

      example.data.delete(datum)

      expect(redis.smembers('redis_example_datum::attrs:ids')).to eq(['4'])    # child not deleted
      expect(redis.zrange('redis_example:8:assocs.data_ids', 0, -1)).to eq([]) # but association is
    end

    it "filters has_sorted_set records by indexed attribute values" do
      create_example(:id => '8', :name => 'John Jones',
                     :email => 'jjones@example.com', :active => 'true')
      example = Sandstorm::RedisExample.find_by_id('8')

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


    it "filters has_sorted_set records by indexed attribute values with a regex search" do
      create_example(:id => '8', :name => 'John Jones',
                     :email => 'jjones@example.com', :active => 'true')
      example = Sandstorm::RedisExample.find_by_id('8')

      time = Time.now

      create_datum(example, :id => '4', :summary => 'well then', :timestamp => time,
        :emotion => 'upset')
      create_datum(example, :id => '5', :summary => 'ok', :timestamp => time.to_i + 10,
        :emotion => 'happy')
      create_datum(example, :id => '6', :summary => 'aaargh', :timestamp => time.to_i + 20,
        :emotion => 'upset')

      upset_data = example.data.intersect(:emotion => /^ups/).all
      expect(upset_data).not_to be_nil
      expect(upset_data).to be_an(Array)
      expect(upset_data.size).to eq(2)
      expect(upset_data.map(&:id)).to eq(['4', '6'])
    end

    it "retrieves a subset of a sorted set by index" do
      create_example(:id => '8', :name => 'John Jones',
                     :email => 'jjones@example.com', :active => 'true')
      example = Sandstorm::RedisExample.find_by_id('8')

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
      example = Sandstorm::RedisExample.find_by_id('8')

      time = Time.now

      create_datum(example, :id => '4', :summary => 'well then', :timestamp => time.to_i,
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
      example = Sandstorm::RedisExample.find_by_id('8')

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
      example = Sandstorm::RedisExample.find_by_id('8')

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
      example = Sandstorm::RedisExample.find_by_id('8')

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

    it "retrieves the union of a sorted set by index"
    it "retrieves a reversed union of a sorted set by index"

    it "retrieves the union of a sorted set by score"
    it "retrieves a reversed union of a sorted set by score"

    it "retrieves the exclusion of a sorted set by index" do
      create_example(:id => '8', :name => 'John Jones',
                     :email => 'jjones@example.com', :active => 'true')
      example = Sandstorm::RedisExample.find_by_id('8')

      time = Time.now

      create_datum(example, :id => '4', :summary => 'well then', :timestamp => time,
        :emotion => 'upset')
      create_datum(example, :id => '5', :summary => 'ok', :timestamp => time.to_i + 10,
        :emotion => 'happy')
      create_datum(example, :id => '6', :summary => 'aaargh', :timestamp => time.to_i + 20,
        :emotion => 'upset')

      data = example.data.diff_range(0, 1).all
      expect(data).not_to be_nil
      expect(data).to be_an(Array)
      expect(data.size).to eq(1)
      expect(data.map(&:id)).to eq(['6'])
    end

    it "retrieves a reversed exclusion of a sorted set by index" do
      create_example(:id => '8', :name => 'John Jones',
                     :email => 'jjones@example.com', :active => 'true')
      example = Sandstorm::RedisExample.find_by_id('8')

      time = Time.now

      create_datum(example, :id => '4', :summary => 'well then', :timestamp => time,
        :emotion => 'upset')
      create_datum(example, :id => '5', :summary => 'ok', :timestamp => time.to_i + 10,
        :emotion => 'happy')
      create_datum(example, :id => '6', :summary => 'aaargh', :timestamp => time.to_i + 20,
        :emotion => 'upset')

      data = example.data.diff_range(0, 0, :order => 'desc').all
      expect(data).not_to be_nil
      expect(data).to be_an(Array)
      expect(data.size).to eq(2)
      expect(data.map(&:id)).to eq(['5', '4'])
    end

    it "retrieves the exclusion of a sorted set by score" do
      create_example(:id => '8', :name => 'John Jones',
                     :email => 'jjones@example.com', :active => 'true')
      example = Sandstorm::RedisExample.find_by_id('8')

      time = Time.now

      create_datum(example, :id => '4', :summary => 'well then', :timestamp => time,
        :emotion => 'upset')
      create_datum(example, :id => '5', :summary => 'ok', :timestamp => time.to_i + 10,
        :emotion => 'happy')
      create_datum(example, :id => '6', :summary => 'aaargh', :timestamp => time.to_i + 20,
        :emotion => 'upset')

      data = example.data.diff_range(time.to_i - 1, time.to_i + 15, :by_score => true).all
      expect(data).not_to be_nil
      expect(data).to be_an(Array)
      expect(data.size).to eq(1)
      expect(data.map(&:id)).to eq(['6'])
    end

    it "retrieves a reversed exclusion of a sorted set by score" do
      create_example(:id => '8', :name => 'John Jones',
                     :email => 'jjones@example.com', :active => 'true')
      example = Sandstorm::RedisExample.find_by_id('8')

      time = Time.now

      create_datum(example, :id => '4', :summary => 'well then', :timestamp => time,
        :emotion => 'upset')
      create_datum(example, :id => '5', :summary => 'ok', :timestamp => time.to_i + 10,
        :emotion => 'happy')
      create_datum(example, :id => '6', :summary => 'aaargh', :timestamp => time.to_i + 20,
        :emotion => 'upset')

      data = example.data.diff_range(time.to_i - 1, time.to_i + 8, :by_score => true, :order => 'desc').all
      expect(data).not_to be_nil
      expect(data).to be_an(Array)
      expect(data.size).to eq(2)
      expect(data.map(&:id)).to eq(['6', '5'])
    end

    it "finds a record through a has_sorted_set filter" do
      create_example(:id => '8', :name => 'John Jones',
                     :email => 'jjones@example.com', :active => 'true')
      example = Sandstorm::RedisExample.find_by_id('8')

      time = Time.now

      create_datum(example, :id => '4', :summary => 'well then', :timestamp => time,
        :emotion => 'upset')
      create_datum(example, :id => '5', :summary => 'ok', :timestamp => time.to_i + 10,
        :emotion => 'happy')
      create_datum(example, :id => '6', :summary => 'aaargh', :timestamp => time.to_i + 20,
        :emotion => 'upset')

      wellthen = upset_data = example.data.intersect(:emotion => 'upset').find_by_id('4')
      expect(wellthen).not_to be_nil
      expect(wellthen).to be_a(Sandstorm::RedisExampleDatum)
      expect(wellthen.id).to eq('4')
    end

    it 'clears the belongs_to association when the child record is deleted' do
      create_example(:id => '8', :name => 'John Jones',
                     :email => 'jjones@example.com', :active => 'true')
      example = Sandstorm::RedisExample.find_by_id('8')

      time = Time.now

      create_datum(example, :id => '6', :summary => 'aaargh', :timestamp => time.to_i + 20,
        :emotion => 'upset')
      datum = Sandstorm::RedisExampleDatum.find_by_id('6')

      expect(redis.keys).to match_array(['redis_example::attrs:ids',
                            'redis_example::indices:by_name',
                            'redis_example::indices:by_active:boolean:true',
                            'redis_example:8:attrs',
                            'redis_example:8:assocs:data_ids',
                            'redis_example_datum::attrs:ids',
                            'redis_example_datum::indices:by_emotion:string:upset',
                            'redis_example_datum:6:attrs',
                            'redis_example_datum:6:assocs:belongs_to'])

      datum.destroy

      expect(redis.keys).to match_array(['redis_example::attrs:ids',
                            'redis_example::indices:by_name',
                            'redis_example::indices:by_active:boolean:true',
                            'redis_example:8:attrs'])
    end

    it "clears the belongs_to association when the parent record is deleted" do
      create_example(:id => '8', :name => 'John Jones',
                     :email => 'jjones@example.com', :active => 'true')
      example = Sandstorm::RedisExample.find_by_id('8')

      time = Time.now

      create_datum(example, :id => '6', :summary => 'aaargh', :timestamp => time.to_i + 20,
        :emotion => 'upset')

      expect(redis.keys).to match_array(['redis_example::attrs:ids',
                            'redis_example::indices:by_name',
                            'redis_example::indices:by_active:boolean:true',
                            'redis_example:8:attrs',
                            'redis_example:8:assocs:data_ids',
                            'redis_example_datum::attrs:ids',
                            'redis_example_datum::indices:by_emotion:string:upset',
                            'redis_example_datum:6:attrs',
                            'redis_example_datum:6:assocs:belongs_to'])

      example.destroy

      expect(redis.keys).to match_array(['redis_example_datum::attrs:ids',
                            'redis_example_datum::indices:by_emotion:string:upset',
                            'redis_example_datum:6:attrs'])
    end

    it 'returns associated ids for multiple parent ids' do
      create_example(:id => '8', :name => 'John Jones',
                     :email => 'jjones@example.com', :active => 'true')
      example_8 = Sandstorm::RedisExample.find_by_id('8')

      create_example(:id => '9', :name => 'Jane Johnson',
                     :email => 'jjohnson@example.com', :active => 'true')

      create_example(:id => '10', :name => 'Jim Smith',
                     :email => 'jsmith@example.com', :active => 'true')
      example_10 = Sandstorm::RedisExample.find_by_id('10')

      time = Time.now.to_i

      create_datum(example_8, :id => '3', :summary => 'aaargh', :timestamp => time.to_i + 20,
        :emotion => 'ok')
      create_datum(example_8, :id => '4', :summary => 'aaargh', :timestamp => time.to_i + 30,
        :emotion => 'ok')
      create_datum(example_10, :id => '5', :summary => 'aaargh', :timestamp => time.to_i + 40,
        :emotion => 'not_ok')

      assoc_ids = Sandstorm::RedisExample.intersect(:id => ['8', '9', '10']).
        associated_ids_for(:data)
      expect(assoc_ids).to eq('8'  => Set.new(['3', '4']),
                              '9'  => Set.new(),
                              '10' => Set.new(['5']))
    end

  end

  context "has_one" do

    class Sandstorm::RedisExampleSpecial
      include Sandstorm::Records::RedisRecord

      define_attributes :name => :string

      belongs_to :example, :class_name => 'Sandstorm::RedisExample', :inverse_of => :special

      validate :name, :presence => true
    end

    class Sandstorm::RedisExample
      has_one :special, :class_name => 'Sandstorm::RedisExampleSpecial', :inverse_of => :example
    end

    it "sets and retrieves a record via a has_one association" do
      create_example(:id => '8', :name => 'John Jones',
                     :email => 'jjones@example.com', :active => 'true')

      special = Sandstorm::RedisExampleSpecial.new(:id => '22', :name => 'Bill Smith')
      expect(special.save).to be_truthy

      example = Sandstorm::RedisExample.find_by_id('8')
      example.special = special

      expect(redis.keys('*')).to match_array(['redis_example::attrs:ids',
                                 'redis_example::indices:by_name',
                                 'redis_example::indices:by_active:boolean:true',
                                 'redis_example:8:attrs',
                                 'redis_example:8:assocs',
                                 'redis_example_special::attrs:ids',
                                 'redis_example_special:22:attrs',
                                 'redis_example_special:22:assocs:belongs_to'])

      expect(redis.hgetall('redis_example:8:assocs')).to eq("special_id" => "22")

      expect(redis.smembers('redis_example_special::attrs:ids')).to eq(['22'])
      expect(redis.hgetall('redis_example_special:22:attrs')).to eq(
        {'name' => 'Bill Smith'}
      )

      expect(redis.hgetall('redis_example_special:22:assocs:belongs_to')).to eq(
        {'example_id' => '8'}
      )

      example2 = Sandstorm::RedisExample.find_by_id('8')
      special2 = example2.special
      expect(special2).not_to be_nil

      expect(special2.id).to eq('22')
      expect(special2.example.id).to eq('8')
    end

    def create_special(parent, attrs = {})
      redis.hmset("redis_example_special:#{attrs[:id]}:attrs", {'name' => attrs[:name]}.to_a.flatten)

      redis.hset("redis_example_special:#{attrs[:id]}:assocs:belongs_to", 'example_id', parent.id)
      redis.hset("redis_example:#{parent.id}:assocs", 'special_id', attrs[:id])

      redis.sadd('redis_example_special::attrs:ids', attrs[:id])
    end

    it 'clears the belongs_to association when the child record is deleted' do
      create_example(:id => '8', :name => 'John Jones',
                     :email => 'jjones@example.com', :active => 'true')
      example = Sandstorm::RedisExample.find_by_id('8')
      create_special(example, :id => '3', :name => 'Another Jones')
      special = Sandstorm::RedisExampleSpecial.find_by_id('3')

      expect(redis.keys).to match_array(['redis_example::attrs:ids',
                            'redis_example::indices:by_name',
                            'redis_example::indices:by_active:boolean:true',
                            'redis_example:8:attrs',
                            'redis_example:8:assocs',
                            'redis_example_special::attrs:ids',
                            'redis_example_special:3:attrs',
                            'redis_example_special:3:assocs:belongs_to'])

      special.destroy

      expect(redis.keys).to match_array(['redis_example::attrs:ids',
                            'redis_example::indices:by_name',
                            'redis_example::indices:by_active:boolean:true',
                            'redis_example:8:attrs'])
    end

    it "clears the belongs_to association when the parent record is deleted" do
      create_example(:id => '8', :name => 'John Jones',
                     :email => 'jjones@example.com', :active => 'true')
      example = Sandstorm::RedisExample.find_by_id('8')
      create_special(example, :id => '3', :name => 'Another Jones')

      expect(redis.keys).to match_array(['redis_example::attrs:ids',
                            'redis_example::indices:by_name',
                            'redis_example::indices:by_active:boolean:true',
                            'redis_example:8:attrs',
                            'redis_example:8:assocs',
                            'redis_example_special::attrs:ids',
                            'redis_example_special:3:attrs',
                            'redis_example_special:3:assocs:belongs_to'])

      example.destroy

      expect(redis.keys).to match_array(['redis_example_special::attrs:ids',
                            'redis_example_special:3:attrs'])
    end

    it 'returns associated ids for multiple parent ids' do
      create_example(:id => '8', :name => 'John Jones',
                     :email => 'jjones@example.com', :active => 'true')

      create_example(:id => '9', :name => 'Jane Johnson',
                     :email => 'jjohnson@example.com', :active => 'true')
      example_9 = Sandstorm::RedisExample.find_by_id('9')

      create_example(:id => '10', :name => 'Jim Smith',
                     :email => 'jsmith@example.com', :active => 'true')
      example_10 = Sandstorm::RedisExample.find_by_id('10')

      time = Time.now.to_i

      create_special(example_9,  :id => '3', :name => 'jkl')
      create_special(example_10, :id => '4', :name => 'pqr')

      assoc_ids = Sandstorm::RedisExample.intersect(:id => ['8', '9', '10']).
        associated_ids_for(:special)
      expect(assoc_ids).to eq('8'  => nil,
                              '9'  => '3',
                              '10' => '4')
    end

  end

  context "has_and_belongs_to_many" do

    def create_template(attrs = {})
      redis.hmset("template:#{attrs[:id]}:attrs", {'name' => attrs[:name]}.to_a.flatten)
      redis.sadd('template::attrs:ids', attrs[:id])
    end

    before(:each) do
      create_example(:id => '8', :name => 'John Jones',
                     :email => 'jjones@example.com', :active => true)
      create_template(:id => '2', :name => 'Template 1')
    end

    it "sets a has_and_belongs_to_many relationship between two records in redis" do
      example = Sandstorm::RedisExample.find_by_id('8')
      template = Sandstorm::Template.find_by_id('2')

      example.templates << template

      expect(redis.keys('*')).to match_array(['redis_example::attrs:ids',
                                 'redis_example::indices:by_name',
                                 'redis_example::indices:by_active:boolean:true',
                                 'redis_example:8:attrs',
                                 'redis_example:8:assocs:templates_ids',
                                 'template::attrs:ids',
                                 'template:2:attrs',
                                 'template:2:assocs:examples_ids'])

      expect(redis.smembers('redis_example::attrs:ids')).to eq(['8'])
      expect(redis.smembers('redis_example::indices:by_active:boolean:true')).to eq(['8'])
      expect(redis.hgetall('redis_example:8:attrs')).to eq(
        {'name' => 'John Jones', 'email' => 'jjones@example.com', 'active' => 'true'}
      )
      expect(redis.smembers('redis_example:8:assocs:templates_ids')).to eq(['2'])

      expect(redis.smembers('template::attrs:ids')).to eq(['2'])
      expect(redis.hgetall('template:2:attrs')).to eq({'name' => 'Template 1'})
      expect(redis.smembers('template:2:assocs:examples_ids')).to eq(['8'])
    end

    it "loads a record from a has_and_belongs_to_many relationship" do
      example = Sandstorm::RedisExample.find_by_id('8')
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
      example = Sandstorm::RedisExample.find_by_id('8')
      template = Sandstorm::Template.find_by_id('2')

      template.examples << example

      expect(redis.smembers('template::attrs:ids')).to eq(['2'])
      expect(redis.smembers('redis_example:8:assocs:templates_ids')).to eq(['2'])

      example.templates.delete(template)

      expect(redis.smembers('template::attrs:ids')).to eq(['2'])        # template not deleted
      expect(redis.smembers('redis_example:8:assocs:templates_ids')).to eq([]) # but association is
    end

    it "filters has_and_belongs_to_many records by indexed attribute values" do
      create_example(:id => '9', :name => 'James Smith',
                     :email => 'jsmith@example.com', :active => false)
      create_example(:id => '10', :name => 'Alpha Beta',
                     :email => 'abc@example.com', :active => true)

      example = Sandstorm::RedisExample.find_by_id('8')
      example_2 = Sandstorm::RedisExample.find_by_id('9')
      example_3 = Sandstorm::RedisExample.find_by_id('10')
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

      example = Sandstorm::RedisExample.find_by_id('8')
      example_2 = Sandstorm::RedisExample.find_by_id('9')
      template = Sandstorm::Template.find_by_id('2')

      example.templates << template
      example_2.templates << template

      expect(template.examples.intersect(:active => false).exists?('9')).to be_truthy
      expect(template.examples.intersect(:active => false).exists?('8')).to be_falsey
    end

    it "finds a record through a has_and_belongs_to_many filter" do
      create_example(:id => '9', :name => 'James Smith',
                     :email => 'jsmith@example.com', :active => false)

      example = Sandstorm::RedisExample.find_by_id('8')
      example_2 = Sandstorm::RedisExample.find_by_id('9')
      template = Sandstorm::Template.find_by_id('2')

      example.templates << template
      example_2.templates << template

      james = template.examples.intersect(:active => false).find_by_id('9')
      expect(james).not_to be_nil
      expect(james).to be_a(Sandstorm::RedisExample)
      expect(james.id).to eq(example_2.id)
    end

    it 'clears a has_and_belongs_to_many association when a record is deleted'

    it 'returns associated ids for multiple parent ids' do
      create_example(:id => '9', :name => 'Jane Johnson',
                     :email => 'jjohnson@example.com', :active => 'true')
      example_9 = Sandstorm::RedisExample.find_by_id('9')

      create_example(:id => '10', :name => 'Jim Smith',
                     :email => 'jsmith@example.com', :active => 'true')
      example_10 = Sandstorm::RedisExample.find_by_id('10')

      create_template(:id => '3', :name => 'Template 3')
      create_template(:id => '4', :name => 'Template 4')

      template_2 = Sandstorm::Template.find_by_id('2')
      template_3 = Sandstorm::Template.find_by_id('3')
      template_4 = Sandstorm::Template.find_by_id('4')

      example_9.templates.add(template_2)
      example_10.templates.add(template_3, template_4)

      assoc_ids = Sandstorm::RedisExample.intersect(:id => ['8', '9', '10']).
        associated_ids_for(:templates)
      expect(assoc_ids).to eq('8'  => Set.new([]),
                              '9'  => Set.new(['2']),
                              '10' => Set.new(['3', '4']))
    end

  end

  context 'bad parameters' do

    let(:example) { Sandstorm::RedisExample.find_by_id('8') }

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
        Sandstorm::RedisExample.intersect(:email => 'jjones@example.com').all
      }.to raise_error
    end
  end

end
