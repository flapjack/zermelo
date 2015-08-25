require 'spec_helper'
require 'zermelo/records/redis'
require 'zermelo/associations/range_index'

describe Zermelo::Associations::RangeIndex do

  context 'redis', :redis => true do

    let(:redis) { Zermelo.redis }

    let(:time)  { Time.now }

    module ZermeloExamples
      class RedisRangeIndex
        include Zermelo::Records::RedisSet
        define_attributes :created_at => :timestamp
        range_index_by :created_at
        validates :created_at, :presence => true
      end
    end

    it 'adds an entry to a sorted set indexing an attribute' do
      example = ZermeloExamples::RedisRangeIndex.new(:id => '1',
        :created_at => time)
      expect(example).to be_valid
      expect(example.save).to be true

      expect(redis.exists('redis_range_index::indices:by_created_at')).to be true
      expect(redis.zrange('redis_range_index::indices:by_created_at', 0, -1, :withscores => true)).to eq([
        ['1', time.to_f]
      ])
    end

    it 'removes an entry from a sorted set indexing an attribute' do
      time_2 = time + 100

      example = ZermeloExamples::RedisRangeIndex.new(:id => '1',
        :created_at => time)
      example.save

      example_2 = ZermeloExamples::RedisRangeIndex.new(:id => '2',
        :created_at => time_2)
      example_2.save

      expect(redis.zrange('redis_range_index::indices:by_created_at', 0, -1, :withscores => true)).to eq([
        ['1', time.to_f],
        ['2', time_2.to_f]
      ])

      example.destroy

      expect(redis.zrange('redis_range_index::indices:by_created_at', 0, -1, :withscores => true)).to eq([
        ['2', time_2.to_f]
      ])
    end

    it 'changes an entry in a sorted set indexing an attribute' do
      example = ZermeloExamples::RedisRangeIndex.new(:id => '1',
        :created_at => time)
      example.save

      expect(redis.zrange('redis_range_index::indices:by_created_at', 0, -1, :withscores => true)).to eq([
        ['1', time.to_f]
      ])

      time_2 = time + 100
      example.created_at = time_2
      example.save

      expect(redis.zrange('redis_range_index::indices:by_created_at', 0, -1, :withscores => true)).to eq([
        ['1', time_2.to_f]
      ])
    end

  end

end