require 'spec_helper'
require 'zermelo/records/redis'
require 'zermelo/associations/has_many'

describe Zermelo::Associations::HasMany do

  context 'redis', :redis => true do

    before do
      skip "broken"
    end

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

      child = Zermelo::RedisExampleChild.new(:id => '3', :name => 'Abel Tasman')
      expect(child.save).to be_truthy

      example = Zermelo::RedisExample.find_by_id('8')
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
      example = Zermelo::RedisExample.find_by_id('8')
      create_child(example, :id => '3', :name => 'Abel Tasman')

      children = example.children.all

      expect(children).to be_an(Array)
      expect(children.size).to eq(1)
      child = children.first
      expect(child).to be_a(Zermelo::RedisExampleChild)
      expect(child.name).to eq('Abel Tasman')
    end

    it "loads a parent from a child's belongs_to relationship" do
      create_example(:id => '8', :name => 'John Jones',
                     :email => 'jjones@example.com', :active => 'true')
      example = Zermelo::RedisExample.find_by_id('8')
      create_child(example, :id => '3', :name => 'Abel Tasman')
      child = Zermelo::RedisExampleChild.find_by_id('3')

      other_example = child.example
      expect(other_example).not_to be_nil
      expect(other_example).to be_a(Zermelo::RedisExample)
      expect(other_example.name).to eq('John Jones')
    end

    it "removes a parent/child has_many relationship between two records in redis" do
      create_example(:id => '8', :name => 'John Jones',
                     :email => 'jjones@example.com', :active => 'true')
      example = Zermelo::RedisExample.find_by_id('8')

      create_child(example, :id => '3', :name => 'Abel Tasman')
      child = Zermelo::RedisExampleChild.find_by_id('3')

      expect(redis.smembers('redis_example_child::attrs:ids')).to eq(['3'])
      expect(redis.smembers('redis_example:8:assocs:children_ids')).to eq(['3'])

      example.children.delete(child)

      expect(redis.smembers('redis_example_child::attrs:ids')).to eq(['3'])   # child not deleted
      expect(redis.smembers('redis_example:8:assocs:children_ids')).to eq([]) # but association is
    end

    it "filters has_many records by indexed attribute values" do
      create_example(:id => '8', :name => 'John Jones',
                     :email => 'jjones@example.com', :active => 'true')
      example = Zermelo::RedisExample.find_by_id('8')

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
      example = Zermelo::RedisExample.find_by_id('8')

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
      example = Zermelo::RedisExample.find_by_id('8')

      create_child(example, :id => '3', :name => 'Martin Luther King', :important => true)
      create_child(example, :id => '4', :name => 'Julius Caesar', :important => true)
      create_child(example, :id => '5', :name => 'John Smith', :important => false)

      expect(example.children.intersect(:important => true).exists?('3')).to be_truthy
      expect(example.children.intersect(:important => true).exists?('5')).to be_falsey
    end

    it "finds a record through a has_many filter" do
      create_example(:id => '8', :name => 'John Jones',
                     :email => 'jjones@example.com', :active => 'true')
      example = Zermelo::RedisExample.find_by_id('8')

      create_child(example, :id => '3', :name => 'Martin Luther King', :important => true)
      create_child(example, :id => '4', :name => 'Julius Caesar', :important => true)
      create_child(example, :id => '5', :name => 'John Smith', :important => false)

      martin = example.children.intersect(:important => true).find_by_id('3')
      expect(martin).not_to be_nil
      expect(martin).to be_a(Zermelo::RedisExampleChild)
      expect(martin.id).to eq('3')
    end

    it "does not add a child if the before_add callback raises an exception" do
      create_example(:id => '8', :name => 'John Jones',
                     :email => 'jjones@example.com', :active => 'true')
      example = Zermelo::RedisExample.find_by_id('8')

      create_child(nil, :id => '6', :name => 'Roger', :important => true)
      child = Zermelo::RedisExampleChild.find_by_id('6')

      expect(example.children).to be_empty
      expect {
        example.children << child
      }.to raise_error
      expect(example.children).to be_empty
    end

    it 'calls the before/after_read callbacks as part of query execution' do
      create_example(:id => '8', :name => 'John Jones',
                     :email => 'jjones@example.com', :active => 'true')
      example = Zermelo::RedisExample.find_by_id('8')

      expect(example.read).to be_nil
      expect(example.children).to be_empty
      expect(example.read).to eq([:pre, :post])
    end

    it 'clears the belongs_to association when the child record is deleted' do
      create_example(:id => '8', :name => 'John Jones',
                     :email => 'jjones@example.com', :active => 'true')
      example = Zermelo::RedisExample.find_by_id('8')

      time = Time.now

      create_child(example, :id => '6', :name => 'Martin Luther King', :important => true)
      child = Zermelo::RedisExampleChild.find_by_id('6')

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
      example = Zermelo::RedisExample.find_by_id('8')

      time = Time.now

      create_child(example, :id => '6', :name => 'Martin Luther King', :important => true)
      child = Zermelo::RedisExampleChild.find_by_id('6')

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
      example_8 = Zermelo::RedisExample.find_by_id('8')

      create_example(:id => '9', :name => 'Jane Johnson',
                     :email => 'jjohnson@example.com', :active => 'true')
      example_9 = Zermelo::RedisExample.find_by_id('9')

      create_example(:id => '10', :name => 'Jim Smith',
                     :email => 'jsmith@example.com', :active => 'true')

      create_child(example_8, :id => '3', :name => 'abc', :important => false)
      create_child(example_9, :id => '4', :name => 'abc', :important => false)
      create_child(example_9, :id => '5', :name => 'abc', :important => false)

      assoc_ids = Zermelo::RedisExample.intersect(:id => [ '8', '9', '10']).
        associated_ids_for(:children)
      expect(assoc_ids).to eq('8'  => Set.new(['3']),
                              '9'  => Set.new(['4', '5']),
                              '10' => Set.new())

      assoc_parent_ids = Zermelo::RedisExampleChild.intersect(:id => ['3', '4', '5']).
        associated_ids_for(:example)
      expect(assoc_parent_ids).to eq('3' => '8',
                                     '4' => '9',
                                     '5' => '9')
    end

  end

end