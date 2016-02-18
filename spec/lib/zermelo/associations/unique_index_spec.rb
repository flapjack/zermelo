require 'spec_helper'
require 'zermelo/records/redis'
require 'zermelo/associations/unique_index'

describe Zermelo::Associations::UniqueIndex do

  context 'redis', :redis => true do

    let(:redis) { Zermelo.redis }

    module ZermeloExamples
      class RedisUniqueIndex
        include Zermelo::Records::RedisSet
        define_attributes :name => :string
        validates :name, :presence => true
        unique_index_by :name
      end
    end

    it 'adds an entry to a hash indexing an attribute' do
      example = ZermeloExamples::RedisUniqueIndex.new(:id => '1',
        :name => 'John Smith')
      expect(example).to be_valid
      expect(example.save).to be true

      expect(redis.exists('redis_unique_index::indices:by_name')).to be true
      expect(redis.hgetall('redis_unique_index::indices:by_name')).to eq('string:John%20Smith' => '1')
    end

    it 'removes an entry from a hash indexing an attribute' do
      example = ZermeloExamples::RedisUniqueIndex.new(:id => '1',
        :name => 'John Smith')
      example.save

      example_2 = ZermeloExamples::RedisUniqueIndex.new(:id => '2',
        :name => 'Roger Wilco')
      example_2.save

      expect(redis.hgetall('redis_unique_index::indices:by_name')).to eq('string:John%20Smith' => '1',
        'string:Roger%20Wilco' => '2')

      example.destroy

      expect(redis.hgetall('redis_unique_index::indices:by_name')).to eq({'string:Roger%20Wilco' => '2'})
    end

    it 'changes an entry in a hash indexing an attribute' do
      example = ZermeloExamples::RedisUniqueIndex.new(:id => '1',
        :name => 'John Smith')
      example.save

      expect(redis.hgetall('redis_unique_index::indices:by_name')).to eq('string:John%20Smith' => '1')

      example.name = 'Jane Jones'
      example.save

      expect(redis.hgetall('redis_unique_index::indices:by_name')).to eq('string:Jane%20Jones' => '1')
    end

  end

end
