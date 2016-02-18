require 'spec_helper'
require 'zermelo/records/redis'
require 'zermelo/associations/index'

describe Zermelo::Associations::Index do

  context 'redis', :redis => true do

    let(:redis) { Zermelo.redis }

    let(:time)  { Time.now }

    module ZermeloExamples
      class RedisIndex
        include Zermelo::Records::RedisSet
        define_attributes :emotion => :string
        validates :emotion, :presence => true, :inclusion => {:in => %w(happy sad indifferent)}
        index_by :emotion
      end
    end

    it 'adds an entry to a set indexing an attribute' do
      example = ZermeloExamples::RedisIndex.new(:id => '1',
        :emotion => 'happy')
      expect(example).to be_valid
      expect(example.save).to be true

      expect(redis.exists('redis_index::indices:by_emotion:string:happy')).to be true
      expect(redis.smembers('redis_index::indices:by_emotion:string:happy')).to eq([
        '1'
      ])
    end

    it 'removes an entry from a sorted set indexing an attribute' do
      example = ZermeloExamples::RedisIndex.new(:id => '1',
        :emotion => 'happy')
      example.save

      example_2 = ZermeloExamples::RedisIndex.new(:id => '2',
        :emotion => 'happy')
      example_2.save

      expect(redis.smembers('redis_index::indices:by_emotion:string:happy')).to eq([
        '1', '2'
      ])

      example.destroy

      expect(redis.smembers('redis_index::indices:by_emotion:string:happy')).to eq([
        '2'
      ])
    end

    it 'changes an entry in a sorted set indexing an attribute' do
      example = ZermeloExamples::RedisIndex.new(:id => '1',
        :emotion => 'happy')
      example.save

      expect(redis.smembers('redis_index::indices:by_emotion:string:happy')).to eq([
        '1'
      ])

      example.emotion = 'sad'
      example.save

      expect(redis.exists('redis_index::indices:by_emotion:string:happy')).to be false
      expect(redis.exists('redis_index::indices:by_emotion:string:sad')).to be true
      expect(redis.smembers('redis_index::indices:by_emotion:string:sad')).to eq([
        '1'
      ])
    end

  end

end