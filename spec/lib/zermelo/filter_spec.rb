# require 'spec_helper'
# require 'zermelo/records/redis'
# require 'zermelo/associations/range_index'

# describe Zermelo::Filters::Redis, :redis => true do

#   # TODO shared examples for the filters, with different record class per DB

#   let(:redis) { Zermelo.redis }

#   module ZermeloExamples
#     class RedisFilter
#       include Zermelo::Records::Redis
#       define_attributes :name       => :string,
#                         :active     => :boolean,
#                         :created_at => :timestamp
#       validates :name, :presence => true
#       validates :active, :inclusion => {:in => [true, false]}
#       index_by :active
#       range_index_by :created_at
#       unique_index_by :name
#     end
#   end

#   def create_example(attrs = {})
#     redis.hmset("redis_filter:#{attrs[:id]}:attrs",
#       {'name' => attrs[:name], 'active' => attrs[:active]}.to_a.flatten)
#     redis.sadd("redis_filter::indices:by_active:boolean:#{!!attrs[:active]}", attrs[:id])
#     name = attrs[:name].gsub(/%/, '%%').gsub(/ /, '%20').gsub(/:/, '%3A')
#     redis.hset('redis_filter::indices:by_name', "string:#{name}", attrs[:id])
#     redis.zadd('redis_filter::indices:by_created_at', attrs[:created_at].to_f, attrs[:id])
#     redis.sadd('redis_filter::attrs:ids', attrs[:id])
#   end

#   let(:time) { Time.now }

#   let(:active) {
#     create_example(:id => '8', :name => 'John Jones', :active => true,
#       :created_at => (time - 100).to_f)
#   }

#   let(:inactive) {
#     create_example(:id => '9', :name => 'James Brown', :active => false,
#       :created_at => (time + 100).to_f)
#   }

#   before do
#     active; inactive
#   end

#   it 'can append to a filter chain fragment more than once' do
#     inter = ZermeloExamples::RedisFilter.intersect(:active => true)
#     expect(inter.ids).to eq(['8'])

#     union = inter.union(:name => 'James Brown')
#     expect(union.ids).to eq(['8', '9'])

#     diff = inter.diff(:id => ['8'])
#     expect(diff.ids).to eq([])
#   end

#   it "filters all class records by indexed attribute values" do
#     example = ZermeloExamples::RedisFilter.intersect(:active => true).all
#     expect(example).not_to be_nil
#     expect(example).to be_an(Array)
#     expect(example.size).to eq(1)
#     expect(example.map(&:id)).to eq(['8'])
#   end

#   it 'filters by id attribute values' do
#     example = ZermeloExamples::RedisFilter.intersect(:id => '9').all
#     expect(example).not_to be_nil
#     expect(example).to be_an(Array)
#     expect(example.size).to eq(1)
#     expect(example.map(&:id)).to eq(['9'])
#   end

#   it 'supports sequential intersection and union operations' do
#     examples = ZermeloExamples::RedisFilter.intersect(:active => true).
#                  union(:active => false).all
#     expect(examples).not_to be_nil
#     expect(examples).to be_an(Array)
#     expect(examples.size).to eq(2)
#     expect(examples.map(&:id)).to match_array(['8', '9'])
#   end

#   it "ANDs multiple union arguments, not ORs them" do
#     create_example(:id => '10', :name => 'Jay Johns', :active => true)
#     examples = ZermeloExamples::RedisFilter.intersect(:id => ['8']).
#                  union(:id => ['9', '10'], :active => true).all
#     expect(examples).not_to be_nil
#     expect(examples).to be_an(Array)
#     expect(examples.size).to eq(2)
#     expect(examples.map(&:id)).to match_array(['8', '10'])
#   end

#   it 'supports a regex as argument in union after intersect' do
#     create_example(:id => '10', :name => 'Jay Johns', :active => true)
#     examples = ZermeloExamples::RedisFilter.intersect(:id => ['8']).
#                  union(:id => ['9', '10'], :name => [nil, /^Jam/]).all
#     expect(examples).not_to be_nil
#     expect(examples).to be_an(Array)
#     expect(examples.size).to eq(2)
#     expect(examples.map(&:id)).to match_array(['8', '9'])
#   end

#   it 'allows intersection operations across multiple values for an attribute' do
#     create_example(:id => '10', :name => 'Jay Johns', :active => true)

#     examples = ZermeloExamples::RedisFilter.intersect(:name => ['Jay Johns', 'James Brown']).all
#     expect(examples).not_to be_nil
#     expect(examples).to be_an(Array)
#     expect(examples.size).to eq(2)
#     expect(examples.map(&:id)).to match_array(['9', '10'])
#   end

#   it 'allows union operations across multiple values for an attribute' do
#     create_example(:id => '10', :name => 'Jay Johns', :active => true)

#     examples = ZermeloExamples::RedisFilter.intersect(:active => false).
#                  union(:name => ['Jay Johns', 'James Brown']).all
#     expect(examples).not_to be_nil
#     expect(examples).to be_an(Array)
#     expect(examples.size).to eq(2)
#     expect(examples.map(&:id)).to match_array(['9', '10'])
#   end

#   it 'filters by multiple id attribute values' do
#     create_example(:id => '10', :name => 'Jay Johns', :active => true)

#     example = ZermeloExamples::RedisFilter.intersect(:id => ['8', '10']).all
#     expect(example).not_to be_nil
#     expect(example).to be_an(Array)
#     expect(example.size).to eq(2)
#     expect(example.map(&:id)).to eq(['8', '10'])
#   end

#   it 'excludes particular records' do
#     example = ZermeloExamples::RedisFilter.diff(:active => true).all
#     expect(example).not_to be_nil
#     expect(example).to be_an(Array)
#     expect(example.size).to eq(1)
#     expect(example.map(&:id)).to eq(['9'])
#   end

#   it 'sorts records by an attribute' do
#     example = ZermeloExamples::RedisFilter.sort(:name, :order => 'alpha').all
#     expect(example).not_to be_nil
#     expect(example).to be_an(Array)
#     expect(example.size).to eq(2)
#     expect(example.map(&:id)).to eq(['9', '8'])
#   end

#   it "does not return a spurious record count when records don't exist" do
#     scope = ZermeloExamples::RedisFilter.intersect(:id => ['3000', '5000'])
#     expect(scope.all).to be_empty
#     expect(scope.count).to eq 0
#   end

#   it 'filters by records created before a certain time' do
#     examples = ZermeloExamples::RedisFilter.intersect(:created_at => Zermelo::Filters::IndexRange.new(nil, time))
#     expect(examples.count).to eq(1)
#     expect(examples.map(&:id)).to eq(['8'])
#   end

#   it 'filters by records created after a certain time' do
#     examples = ZermeloExamples::RedisFilter.intersect(:created_at => Zermelo::Filters::IndexRange.new(time, nil))
#     expect(examples.count).to eq(1)
#     expect(examples.map(&:id)).to eq(['9'])
#   end

# end