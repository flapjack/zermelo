require 'spec_helper'
require 'zermelo/associations/has_sorted_set'

describe Zermelo::Associations::HasSortedSet do

  context 'redis', :redis => true do

    before do
      skip "broken"
    end

    let(:redis) { Zermelo.redis }

    module ZermeloExamples
      class RedisHasSortedSetParent
        include Zermelo::Records::RedisRecord
      end

      class RedisHasSortedSetChild
        include Zermelo::Records::RedisRecord
      end
    end

    def create_parent(attrs = {})

    end

    def create_child(parent, attrs = {})

    end

    def create_example(attrs = {})
      redis.hmset("redis_example:#{attrs[:id]}:attrs",
        {'name' => attrs[:name], 'email' => attrs[:email], 'active' => attrs[:active]}.to_a.flatten)
      redis.sadd("redis_example::indices:by_active:boolean:#{!!attrs[:active]}", attrs[:id])
      name = attrs[:name].gsub(/%/, '%%').gsub(/ /, '%20').gsub(/:/, '%3A')
      redis.hset('redis_example::indices:by_name', "string:#{name}", attrs[:id])
      redis.sadd('redis_example::attrs:ids', attrs[:id])
    end

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

      data = Zermelo::RedisExampleDatum.new(:id => '4', :timestamp => time,
        :summary => "hello!")
      expect(data.save).to be_truthy

      example = Zermelo::RedisExample.find_by_id('8')
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
      example = Zermelo::RedisExample.find_by_id('8')

      time = Time.now

      create_datum(example, :id => '4', :summary => 'well then', :timestamp => time)
      datum = Zermelo::RedisExampleDatum.find_by_id('4')

      data = example.data.all

      expect(data).to be_an(Array)
      expect(data.size).to eq(1)
      datum = data.first
      expect(datum).to be_a(Zermelo::RedisExampleDatum)
      expect(datum.summary).to eq('well then')
      expect(datum.timestamp).to be_within(1).of(time) # ignore fractional differences
    end

    it "removes a parent/child has_sorted_set relationship between two records in redis" do
      create_example(:id => '8', :name => 'John Jones',
                     :email => 'jjones@example.com', :active => 'true')
      example = Zermelo::RedisExample.find_by_id('8')

      time = Time.now

      create_datum(example, :id => '4', :summary => 'well then', :timestamp => time)
      datum = Zermelo::RedisExampleDatum.find_by_id('4')

      expect(redis.smembers('redis_example_datum::attrs:ids')).to eq(['4'])
      expect(redis.zrange('redis_example:8:assocs:data_ids', 0, -1)).to eq(['4'])

      example.data.delete(datum)

      expect(redis.smembers('redis_example_datum::attrs:ids')).to eq(['4'])    # child not deleted
      expect(redis.zrange('redis_example:8:assocs.data_ids', 0, -1)).to eq([]) # but association is
    end

    it "filters has_sorted_set records by indexed attribute values" do
      create_example(:id => '8', :name => 'John Jones',
                     :email => 'jjones@example.com', :active => 'true')
      example = Zermelo::RedisExample.find_by_id('8')

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
      example = Zermelo::RedisExample.find_by_id('8')

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
      skip "broken"

      create_example(:id => '8', :name => 'John Jones',
                     :email => 'jjones@example.com', :active => 'true')
      example = Zermelo::RedisExample.find_by_id('8')

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
      skip "broken"

      create_example(:id => '8', :name => 'John Jones',
                     :email => 'jjones@example.com', :active => 'true')
      example = Zermelo::RedisExample.find_by_id('8')

      time = Time.now

      create_datum(example, :id => '4', :summary => 'well then', :timestamp => time.to_i,
        :emotion => 'upset')
      create_datum(example, :id => '5', :summary => 'ok', :timestamp => time.to_i + 10,
        :emotion => 'happy')
      create_datum(example, :id => '6', :summary => 'aaargh', :timestamp => time.to_i + 20,
        :emotion => 'upset')

      data = example.data.intersect_range(1, 2).sort(:id, :desc => true).all

      expect(data).not_to be_nil
      expect(data).to be_an(Array)
      expect(data.size).to eq(2)
      expect(data.map(&:id)).to eq(['6', '5'])
    end

    it "retrieves a subset of a sorted set by score" do
      skip "broken"

      create_example(:id => '8', :name => 'John Jones',
                     :email => 'jjones@example.com', :active => 'true')
      example = Zermelo::RedisExample.find_by_id('8')

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
      skip "broken"

      create_example(:id => '8', :name => 'John Jones',
                     :email => 'jjones@example.com', :active => 'true')
      example = Zermelo::RedisExample.find_by_id('8')

      time = Time.now

      create_datum(example, :id => '4', :summary => 'well then', :timestamp => time,
        :emotion => 'upset')
      create_datum(example, :id => '5', :summary => 'ok', :timestamp => time.to_i + 10,
        :emotion => 'happy')
      create_datum(example, :id => '6', :summary => 'aaargh', :timestamp => time.to_i + 20,
        :emotion => 'upset')

      data = example.data.intersect_range(time.to_i - 1, time.to_i + 15,
              :by_score => true).sort(:timestamp, :desc => true).all
      expect(data).not_to be_nil
      expect(data).to be_an(Array)
      expect(data.size).to eq(2)
      expect(data.map(&:id)).to eq(['5', '4'])
    end

    it "checks whether a record exists through a has_sorted_set filter" do
      create_example(:id => '8', :name => 'John Jones',
                     :email => 'jjones@example.com', :active => 'true')
      example = Zermelo::RedisExample.find_by_id('8')

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
      skip "broken"

      create_example(:id => '8', :name => 'John Jones',
                     :email => 'jjones@example.com', :active => 'true')
      example = Zermelo::RedisExample.find_by_id('8')

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
      skip "broken"

      create_example(:id => '8', :name => 'John Jones',
                     :email => 'jjones@example.com', :active => 'true')
      example = Zermelo::RedisExample.find_by_id('8')

      time = Time.now

      create_datum(example, :id => '4', :summary => 'well then', :timestamp => time,
        :emotion => 'upset')
      create_datum(example, :id => '5', :summary => 'ok', :timestamp => time.to_i + 10,
        :emotion => 'happy')
      create_datum(example, :id => '6', :summary => 'aaargh', :timestamp => time.to_i + 20,
        :emotion => 'upset')

      data = example.data.diff_range(2, 2).sort(:id, :desc => true).all
      expect(data).not_to be_nil
      expect(data).to be_an(Array)
      expect(data.size).to eq(2)
      expect(data.map(&:id)).to eq(['5', '4'])
    end

    it "retrieves the exclusion of a sorted set by score" do
      skip "broken"

      create_example(:id => '8', :name => 'John Jones',
                     :email => 'jjones@example.com', :active => 'true')
      example = Zermelo::RedisExample.find_by_id('8')

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
      skip "broken"

      create_example(:id => '8', :name => 'John Jones',
                     :email => 'jjones@example.com', :active => 'true')
      example = Zermelo::RedisExample.find_by_id('8')

      time = Time.now

      create_datum(example, :id => '4', :summary => 'well then', :timestamp => time,
        :emotion => 'upset')
      create_datum(example, :id => '5', :summary => 'ok', :timestamp => time.to_i + 10,
        :emotion => 'happy')
      create_datum(example, :id => '6', :summary => 'aaargh', :timestamp => time.to_i + 20,
        :emotion => 'upset')

      data = example.data.diff_range(time.to_i - 1, time.to_i + 8, :by_score => true).
        sort(:timestamp, :desc => true).all
      expect(data).not_to be_nil
      expect(data).to be_an(Array)
      expect(data.size).to eq(2)
      expect(data.map(&:id)).to eq(['6', '5'])
    end

    it "finds a record through a has_sorted_set filter" do
      create_example(:id => '8', :name => 'John Jones',
                     :email => 'jjones@example.com', :active => 'true')
      example = Zermelo::RedisExample.find_by_id('8')

      time = Time.now

      create_datum(example, :id => '4', :summary => 'well then', :timestamp => time,
        :emotion => 'upset')
      create_datum(example, :id => '5', :summary => 'ok', :timestamp => time.to_i + 10,
        :emotion => 'happy')
      create_datum(example, :id => '6', :summary => 'aaargh', :timestamp => time.to_i + 20,
        :emotion => 'upset')

      wellthen = upset_data = example.data.intersect(:emotion => 'upset').find_by_id('4')
      expect(wellthen).not_to be_nil
      expect(wellthen).to be_a(Zermelo::RedisExampleDatum)
      expect(wellthen.id).to eq('4')
    end

    it 'clears the belongs_to association when the child record is deleted' do
      create_example(:id => '8', :name => 'John Jones',
                     :email => 'jjones@example.com', :active => 'true')
      example = Zermelo::RedisExample.find_by_id('8')

      time = Time.now

      create_datum(example, :id => '6', :summary => 'aaargh', :timestamp => time.to_i + 20,
        :emotion => 'upset')
      datum = Zermelo::RedisExampleDatum.find_by_id('6')

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
      example = Zermelo::RedisExample.find_by_id('8')

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
      example_8 = Zermelo::RedisExample.find_by_id('8')

      create_example(:id => '9', :name => 'Jane Johnson',
                     :email => 'jjohnson@example.com', :active => 'true')

      create_example(:id => '10', :name => 'Jim Smith',
                     :email => 'jsmith@example.com', :active => 'true')
      example_10 = Zermelo::RedisExample.find_by_id('10')

      time = Time.now.to_i

      create_datum(example_8, :id => '3', :summary => 'aaargh', :timestamp => time.to_i + 20,
        :emotion => 'ok')
      create_datum(example_8, :id => '4', :summary => 'aaargh', :timestamp => time.to_i + 30,
        :emotion => 'ok')
      create_datum(example_10, :id => '5', :summary => 'aaargh', :timestamp => time.to_i + 40,
        :emotion => 'not_ok')

      assoc_ids = Zermelo::RedisExample.intersect(:id => ['8', '9', '10']).
        associated_ids_for(:data)
      expect(assoc_ids).to eq('8'  => ['3', '4'],
                              '9'  => [],
                              '10' => ['5'])
    end

  end

end