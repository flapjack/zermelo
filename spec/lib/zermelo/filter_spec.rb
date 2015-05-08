require 'spec_helper'
require 'zermelo/filter'
require 'zermelo/records/redis'
require 'zermelo/associations/range_index'

describe Zermelo::Filter do

  shared_examples 'filter functions work', :filter => true do

    let(:time) { Time.now }

    let(:active) {
      create_example(:id => '8', :name => 'John Jones', :active => true,
        :created_at => (time - 100).to_f)
    }

    let(:inactive) {
      create_example(:id => '9', :name => 'James Brown', :active => false,
        :created_at => (time + 100).to_f)
    }

    before do
      active; inactive
    end

    it "finds a record by id" do
      example = example_class.find_by_id('8')
      expect(example).not_to be_nil
      expect(example.id).to eq('8')
      expect(example.name).to eq('John Jones')
    end

    it "finds records by a uniquely indexed value" do
      examples = example_class.intersect(:name => 'John Jones').all
      expect(examples).not_to be_nil
      expect(examples).to be_an(Array)
      expect(examples.size).to eq(1)
      example = examples.first
      expect(example.id).to eq('8')
      expect(example.name).to eq('John Jones')
    end

    it 'finds records by regex match against an indexed value'

    it 'finds records by regex match against a uniquely indexed value' do
      examples = example_class.intersect(:name => /hn Jones/).all
      expect(examples).not_to be_nil
      expect(examples).to be_an(Array)
      expect(examples.size).to eq(1)
      example = examples.first
      expect(example.id).to eq('8')
      expect(example.name).to eq('John Jones')
    end

    it 'cannot find records by regex match against non-string values' do
      expect {
        example_class.intersect(:active => /alse/).all
      }.to raise_error
    end

    it 'can append to a filter chain fragment more than once' do
      inter = example_class.intersect(:active => true)
      expect(inter.ids).to eq(['8'])

      union = inter.union(:name => 'James Brown')
      expect(union.ids).to eq(['8', '9'])

      diff = inter.diff(:id => ['8'])
      expect(diff.ids).to eq([])
    end

    it "filters all class records by indexed attribute values" do
      example = example_class.intersect(:active => true).all
      expect(example).not_to be_nil
      expect(example).to be_an(Array)
      expect(example.size).to eq(1)
      expect(example.map(&:id)).to eq(['8'])
    end

    it 'filters by id attribute values' do
      example = example_class.intersect(:id => '9').all
      expect(example).not_to be_nil
      expect(example).to be_an(Array)
      expect(example.size).to eq(1)
      expect(example.map(&:id)).to eq(['9'])
    end

    it 'supports sequential intersection and union operations' do
      examples = example_class.intersect(:active => true).
                   union(:active => false).all
      expect(examples).not_to be_nil
      expect(examples).to be_an(Array)
      expect(examples.size).to eq(2)
      expect(examples.map(&:id)).to match_array(['8', '9'])
    end

    it "ANDs multiple union arguments, not ORs them" do
      create_example(:id => '10', :name => 'Jay Johns', :active => true)
      examples = example_class.intersect(:id => ['8']).
                   union(:id => ['9', '10'], :active => true).all
      expect(examples).not_to be_nil
      expect(examples).to be_an(Array)
      expect(examples.size).to eq(2)
      expect(examples.map(&:id)).to match_array(['8', '10'])
    end

    it 'supports a regex as argument in union after intersect' do
      create_example(:id => '10', :name => 'Jay Johns', :active => true)
      examples = example_class.intersect(:id => ['8']).
                   union(:id => ['9', '10'], :name => [nil, /^Jam/]).all
      expect(examples).not_to be_nil
      expect(examples).to be_an(Array)
      expect(examples.size).to eq(2)
      expect(examples.map(&:id)).to match_array(['8', '9'])
    end

    it 'allows intersection operations across multiple values for an attribute' do
      create_example(:id => '10', :name => 'Jay Johns', :active => true)

      examples = example_class.intersect(:name => ['Jay Johns', 'James Brown']).all
      expect(examples).not_to be_nil
      expect(examples).to be_an(Array)
      expect(examples.size).to eq(2)
      expect(examples.map(&:id)).to match_array(['9', '10'])
    end

    it 'allows union operations across multiple values for an attribute' do
      create_example(:id => '10', :name => 'Jay Johns', :active => true)

      examples = example_class.intersect(:active => false).
                   union(:name => ['Jay Johns', 'James Brown']).all
      expect(examples).not_to be_nil
      expect(examples).to be_an(Array)
      expect(examples.size).to eq(2)
      expect(examples.map(&:id)).to match_array(['9', '10'])
    end

    it 'filters by multiple id attribute values' do
      create_example(:id => '10', :name => 'Jay Johns', :active => true)

      example = example_class.intersect(:id => ['8', '10']).all
      expect(example).not_to be_nil
      expect(example).to be_an(Array)
      expect(example.size).to eq(2)
      expect(example.map(&:id)).to eq(['8', '10'])
    end

    it 'excludes particular records' do
      example = example_class.diff(:active => true).all
      expect(example).not_to be_nil
      expect(example).to be_an(Array)
      expect(example.size).to eq(1)
      expect(example.map(&:id)).to eq(['9'])
    end

    it 'sorts records by an attribute' do
      example = example_class.sort(:name, :order => 'alpha').all
      expect(example).not_to be_nil
      expect(example).to be_an(Array)
      expect(example.size).to eq(2)
      expect(example.map(&:id)).to eq(['9', '8'])
    end

    it "does not return a spurious record count when records don't exist" do
      scope = example_class.intersect(:id => ['3000', '5000'])
      expect(scope.all).to be_empty
      expect(scope.count).to eq 0
    end

    it 'filters by records created before a certain time' do
      examples = example_class.intersect(:created_at => Zermelo::Filters::IndexRange.new(nil, time))
      expect(examples.count).to eq(1)
      expect(examples.map(&:id)).to eq(['8'])
    end

    it 'filters by records created after a certain time' do
      examples = example_class.intersect(:created_at => Zermelo::Filters::IndexRange.new(time, nil))
      expect(examples.count).to eq(1)
      expect(examples.map(&:id)).to eq(['9'])
    end

  end

  shared_examples 'pagination functions work', :pagination => true do
    it "returns paginated query responses" do
      create_example(:id => '1', :name => 'mno')
      create_example(:id => '2', :name => 'abc')
      create_example(:id => '3', :name => 'jkl')
      create_example(:id => '4', :name => 'ghi')
      create_example(:id => '5', :name => 'def')

      expect(example_class.sort(:id).page(1, :per_page => 3).map(&:id)).to eq(['1', '2', '3'])
      expect(example_class.sort(:id).page(2, :per_page => 2).map(&:id)).to eq(['3', '4'])
      expect(example_class.sort(:id).page(3, :per_page => 2).map(&:id)).to eq(['5', '8'])
      expect(example_class.sort(:id).page(3, :per_page => 3).map(&:id)).to eq(['9'])

      # sort is case-sensitive, may want a non-case sensitive version
      expect(example_class.sort(:name).page(1, :per_page => 3).map(&:id)).to eq(['9', '8', '2'])
      expect(example_class.sort(:name).page(2, :per_page => 3).map(&:id)).to eq(['5', '4', '3'])
      expect(example_class.sort(:name).page(3, :per_page => 3).map(&:id)).to eq(['1'])
      expect(example_class.sort(:name).page(4, :per_page => 3).map(&:id)).to eq([])
    end
  end

  context 'redis', :redis => true, :filter => true, :pagination => true do

    let(:redis) { Zermelo.redis }

    module ZermeloExamples
      class FilterRedis
        include Zermelo::Records::Redis
        define_attributes :name       => :string,
                          :active     => :boolean,
                          :created_at => :timestamp
        validates :name, :presence => true
        validates :active, :inclusion => {:in => [true, false]}
        index_by :active
        range_index_by :created_at
        unique_index_by :name
      end
    end

    let(:example_class) { ZermeloExamples::FilterRedis }

    # parent and child keys
    let(:ek) { 'filter_redis' }

    def create_example(attrs = {})
      redis.hmset("#{ek}:#{attrs[:id]}:attrs",
        {'name' => attrs[:name], 'active' => attrs[:active]}.to_a.flatten)
      redis.sadd("#{ek}::indices:by_active:boolean:#{!!attrs[:active]}", attrs[:id])
      name = attrs[:name].gsub(/%/, '%%').gsub(/ /, '%20').gsub(/:/, '%3A')
      redis.hset("#{ek}::indices:by_name", "string:#{name}", attrs[:id])
      redis.zadd("#{ek}::indices:by_created_at", attrs[:created_at].to_f, attrs[:id])
      redis.sadd("#{ek}::attrs:ids", attrs[:id])
    end
  end
end