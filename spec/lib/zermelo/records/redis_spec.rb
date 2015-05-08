require 'spec_helper'
require 'zermelo/records/redis'

# NB: also covers associations.rb, which is mixed in to Zermelo::Record

describe Zermelo::Records::Redis, :redis => true do

  module Zermelo
    class RedisExample
      include Zermelo::Records::Redis

      define_attributes :name   => :string,
                        :email  => :string,
                        :active => :boolean

      validates :name, :presence => true

      has_many :children, :class_name => 'Zermelo::RedisExampleChild',
        :inverse_of => :example, :before_add => :fail_if_roger,
        :before_read => :pre_read, :after_read => :post_read

      # has_sorted_set :data, :class_name => 'Zermelo::RedisExampleDatum',
      #   :key => :timestamp, :inverse_of => :example

      has_and_belongs_to_many :templates, :class_name => 'Zermelo::Template',
        :inverse_of => :examples

      index_by :active
      unique_index_by :name

      def fail_if_roger(*childs)
        raise "Not adding child" if childs.any? {|c| 'Roger'.eql?(c.name) }
      end

      attr_accessor :read

      def pre_read
        @read ||= []
        @read << :pre
      end

      def post_read
        @read ||= []
        @read << :post
      end
    end

    class RedisExampleChild
      include Zermelo::Records::Redis

      define_attributes :name => :string,
                        :important => :boolean

      index_by :important

      belongs_to :example, :class_name => 'Zermelo::RedisExample', :inverse_of => :children

      validates :name, :presence => true
    end

    class RedisExampleDatum
      include Zermelo::Records::Redis

      define_attributes :timestamp => :timestamp,
                        :summary => :string,
                        :emotion => :string

      belongs_to :example, :class_name => 'Zermelo::RedisExample', :inverse_of => :data

      index_by :emotion

      validates :timestamp, :presence => true
    end

    class Template
      include Zermelo::Records::Redis

      define_attributes :name => :string

      has_and_belongs_to_many :examples, :class_name => 'Zermelo::RedisExample',
        :inverse_of => :templates

      validates :name, :presence => true
    end
  end

  let(:redis) { Zermelo.redis }

  def create_example(attrs = {})
    redis.hmset("redis_example:#{attrs[:id]}:attrs",
      {'name' => attrs[:name], 'email' => attrs[:email], 'active' => attrs[:active]}.to_a.flatten)
    redis.sadd("redis_example::indices:by_active:boolean:#{!!attrs[:active]}", attrs[:id])
    name = attrs[:name].gsub(/%/, '%%').gsub(/ /, '%20').gsub(/:/, '%3A')
    redis.hset('redis_example::indices:by_name', "string:#{name}", attrs[:id])
    redis.sadd('redis_example::attrs:ids', attrs[:id])
  end

  it "finds a record by id in redis" do
    create_example(:id => '8', :name => 'John Jones',
                   :email => 'jjones@example.com', :active => 'true')

    example = Zermelo::RedisExample.find_by_id('8')
    expect(example).not_to be_nil
    expect(example.id).to eq('8')
    expect(example.name).to eq('John Jones')
    expect(example.email).to eq('jjones@example.com')
  end

  it "finds records by a uniquely indexed value in redis" do
    create_example(:id => '8', :name => 'John Jones',
                   :email => 'jjones@example.com', :active => 'true')

    examples = Zermelo::RedisExample.intersect(:name => 'John Jones').all
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

    examples = Zermelo::RedisExample.intersect(:name => /hn Jones/).all
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
      Zermelo::RedisExample.intersect(:active => /alse/).all
    }.to raise_error
  end

  it "updates a record's attributes in redis" do
    create_example(:id => '8', :name => 'John Jones',
                   :email => 'jjones@example.com', :active => 'true')

    example = Zermelo::RedisExample.find_by_id('8')
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

    example = Zermelo::RedisExample.find_by_id('8')
    example.destroy

    expect(redis.keys('*')).to eq([])
  end

  it "resets changed state on refresh" do
    create_example(:id => '8', :name => 'John Jones',
                   :email => 'jjones@example.com', :active => 'true')
    example = Zermelo::RedisExample.find_by_id('8')

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
      expect(Zermelo::RedisExample.sort(:id).page(1, :per_page => 3).map(&:id)).to eq(['1','2', '3'])
      expect(Zermelo::RedisExample.sort(:id).page(2, :per_page => 2).map(&:id)).to eq(['3','4'])
      expect(Zermelo::RedisExample.sort(:id).page(3, :per_page => 2).map(&:id)).to eq(['5'])
      expect(Zermelo::RedisExample.sort(:id).page(3, :per_page => 3).map(&:id)).to eq([])

      expect(Zermelo::RedisExample.sort(:name).page(1, :per_page => 3).map(&:id)).to eq(['2','5', '4'])
      expect(Zermelo::RedisExample.sort(:name).page(2, :per_page => 2).map(&:id)).to eq(['4','3'])
      expect(Zermelo::RedisExample.sort(:name).page(3, :per_page => 2).map(&:id)).to eq(['1'])
      expect(Zermelo::RedisExample.sort(:name).page(3, :per_page => 3).map(&:id)).to eq([])
    end

  end

  context 'sorting by multiple keys' do

    def create_template(attrs = {})
      redis.hmset("template:#{attrs[:id]}:attrs", {'name' => attrs[:name]}.to_a.flatten)
      redis.sadd('template::attrs:ids', attrs[:id])
    end

    before do
      create_template(:id => '1', :name => 'abc')
      create_template(:id => '2', :name => 'def')
      create_template(:id => '3', :name => 'abc')
      create_template(:id => '4', :name => 'def')
    end

    it 'sorts by multiple fields' do
      expect(Zermelo::Template.sort(:name => :asc, :id => :desc).map(&:id)).to eq(['3', '1', '4', '2'])
    end

  end

  context 'bad parameters' do

    let(:example) { Zermelo::RedisExample.find_by_id('8') }

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
      skip "broken"

      expect {
        example.data.add
      }.to raise_error
    end

    it 'raises an error when calling delete on has_sorted_set without an argument' do
      skip "broken"

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
        Zermelo::RedisExample.intersect(:email => 'jjones@example.com').all
      }.to raise_error
    end
  end

end
