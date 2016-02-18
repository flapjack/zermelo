require 'spec_helper'
require 'zermelo/filter'
require 'zermelo/records/redis'
require 'zermelo/records/influxdb'
require 'zermelo/associations/range_index'

describe Zermelo::Filter do

  shared_examples 'filter functions work', :filter => true do

    let(:time) { Time.now }

    let(:active) {
      create_example(:id => '8', :name => 'John Jones', :active => true,
        :created_at => (time - 200).to_f)
    }

    let(:inactive) {
      create_example(:id => '9', :name => 'James Brown', :active => false,
        :created_at => (time - 100).to_f)
    }

    before do
      active; inactive
    end

    it "returns all record ids" do
      examples = example_class.ids
      expect(examples).not_to be_nil
      expect(examples).to be_a(Set)
      expect(examples.size).to eq(2)
      expect(examples).to contain_exactly('8', '9')
    end

    it "returns a count of records" do
      example_count = example_class.count
      expect(example_count).not_to be_nil
      expect(example_count).to be_an(Integer)
      expect(example_count).to eq(2)
    end

    it "returns all records" do
      examples = example_class.all
      expect(examples).not_to be_nil
      expect(examples).to be_a(Set)
      expect(examples.size).to eq(2)
      expect(examples.map(&:id)).to contain_exactly('9', '8')
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
      expect(examples).to be_an(Set)
      expect(examples.size).to eq(1)
      example = examples.first
      expect(example.id).to eq('8')
      expect(example.name).to eq('John Jones')
    end

    it "filters all class records by indexed attribute values" do
      example = example_class.intersect(:active => true).all
      expect(example).not_to be_nil
      expect(example).to be_a(Set)
      expect(example.size).to eq(1)
      expect(example.map(&:id)).to eq(['8'])
    end

    it 'filters by id attribute values' do
      example = example_class.intersect(:id => '9').all
      expect(example).not_to be_nil
      expect(example).to be_a(Set)
      expect(example.size).to eq(1)
      expect(example.map(&:id)).to eq(['9'])
    end

    it 'filters by multiple id attribute values' do
      create_example(:id => '10', :name => 'Jay Johns', :active => true,
        :created_at => (time - 50).to_f)

      examples = example_class.intersect(:id => ['8', '10']).all
      expect(examples).not_to be_nil
      expect(examples).to be_a(Set)
      expect(examples.size).to eq(2)
      expect(examples.map(&:id)).to match_array(['8', '10'])
    end

    it 'supports sequential intersection and union operations' do
      examples = example_class.intersect(:active => true).
                   union(:active => false).all
      expect(examples).not_to be_nil
      expect(examples).to be_a(Set)
      expect(examples.size).to eq(2)
      expect(examples.map(&:id)).to match_array(['8', '9'])
    end

    it "chains two intersect filters together" do
      example = example_class.intersect(:active => true).
        intersect(:name => 'John Jones').all
      expect(example).not_to be_nil
      expect(example).to be_a(Set)
      expect(example.size).to eq(1)
      expect(example.map(&:id)).to eq(['8'])
    end

    it "allows multiple attributes in an intersect filter" do
      example = example_class.intersect(:active => true,
        :name => 'John Jones').all
      expect(example).not_to be_nil
      expect(example).to be_a(Set)
      expect(example.size).to eq(1)
      expect(example.map(&:id)).to eq(['8'])
    end

    it "chains an intersect and a diff filter together" do
      create_example(:id => '3', :name => 'Fred Bloggs',
        :active => 'true')

      example = example_class.intersect(:active => true).diff(:name => 'Fred Bloggs').all
      expect(example).not_to be_nil
      expect(example).to be_a(Set)
      expect(example.size).to eq(1)
      expect(example.map(&:id)).to eq(['8'])
    end

    it "does not return a spurious record count when records don't exist" do
      scope = example_class.intersect(:id => ['3000', '5000'])
      expect(scope.all).to be_empty
      expect(scope.count).to eq 0
    end

    it 'finds records by regex match against a uniquely indexed value' do
      examples = example_class.intersect(:name => /hn Jones/).all
      expect(examples).not_to be_nil
      expect(examples).to be_a(Set)
      expect(examples.size).to eq(1)
      example = examples.first
      expect(example.id).to eq('8')
      expect(example.name).to eq('John Jones')
    end

    it 'cannot find records by regex match against non-string values' do
      expect {
        example_class.intersect(:active => /alse/).all
      }.to raise_error("Can't query non-string values via regexp")
    end

    it 'can append to a filter chain fragment more than once' do
      inter = example_class.intersect(:active => true)
      expect(inter.ids).to eq(Set.new(['8']))

      union = inter.union(:name => 'James Brown')
      expect(union.ids).to eq(Set.new(['8', '9']))

      diff = inter.diff(:id => ['8'])
      expect(diff.ids).to eq(Set.new)
    end

    it "ANDs multiple union arguments, not ORs them" do
      create_example(:id => '10', :name => 'Jay Johns', :active => true)
      examples = example_class.intersect(:id => ['8']).
                   union(:id => ['9', '10'], :active => true).all
      expect(examples).not_to be_nil
      expect(examples).to be_a(Set)
      expect(examples.size).to eq(2)
      expect(examples.map(&:id)).to match_array(['8', '10'])
    end

    it "ANDs multiple diff arguments, not ORs them" do
      create_example(:id => '10', :name => 'Jay Johns', :active => true)
      examples = example_class.intersect(:id => ['8', '9', '10']).
                   diff(:id => ['9', '10'], :active => false).all
      expect(examples).not_to be_nil
      expect(examples).to be_a(Set)
      expect(examples.size).to eq(2)
      expect(examples.map(&:id)).to match_array(['8', '10'])
    end

    it 'supports a regex as argument in union after intersect' do
      create_example(:id => '10', :name => 'Jay Johns', :active => true)
      examples = example_class.intersect(:id => ['8']).
                   union(:id => ['9', '10'], :name => [nil, /^Jam/]).all
      expect(examples).not_to be_nil
      expect(examples).to be_a(Set)
      expect(examples.size).to eq(2)
      expect(examples.map(&:id)).to match_array(['8', '9'])
    end

    it 'allows intersection operations across multiple values for an attribute' do
      create_example(:id => '10', :name => 'Jay Johns', :active => true)

      examples = example_class.intersect(:name => ['Jay Johns', 'James Brown']).all
      expect(examples).not_to be_nil
      expect(examples).to be_a(Set)
      expect(examples.size).to eq(2)
      expect(examples.map(&:id)).to match_array(['9', '10'])
    end

    it 'allows union operations across multiple values for an attribute' do
      create_example(:id => '10', :name => 'Jay Johns', :active => true)

      examples = example_class.intersect(:active => false).
                   union(:name => ['Jay Johns', 'James Brown']).all
      expect(examples).not_to be_nil
      expect(examples).to be_a(Set)
      expect(examples.size).to eq(2)
      expect(examples.map(&:id)).to match_array(['9', '10'])
    end

    it 'excludes particular records' do
      example = example_class.diff(:active => true).all
      expect(example).not_to be_nil
      expect(example).to be_a(Set)
      expect(example.size).to eq(1)
      expect(example.map(&:id)).to eq(['9'])
    end

  end

  context 'redis', :redis => true, :filter => true do

    let(:redis) { Zermelo.redis }

    module ZermeloExamples
      class FilterRedis
        include Zermelo::Records::RedisSet
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

    # following only working in Redis for now
    it 'sorts records by an attribute' do
      example = example_class.sort(:name, :order => 'alpha').all
      expect(example).not_to be_nil
      expect(example).to be_a(Set)
      expect(example.size).to eq(2)
      expect(example.map(&:id)).to eq(['9', '8'])
    end

    it 'sorts by multiple fields' do
      data = {:active => true, :created_at => Time.now}
      create_example(data.merge(:id => '1', :name => 'abc'))
      create_example(data.merge(:id => '2', :name => 'def'))
      create_example(data.merge(:id => '3', :name => 'abc'))
      create_example(data.merge(:id => '4', :name => 'def'))

      expect(example_class.sort(:name => :asc, :id => :desc).map(&:id)).to eq(
        ['9', '8', '3', '1', '4', '2']
      )
    end

    # NB sort is case-sensitive, may want a non-case sensitive version
    it "returns paginated query responses" do
      data = {:active => true, :created_at => Time.now}
      create_example(data.merge(:id => '1', :name => 'mno'))
      create_example(data.merge(:id => '2', :name => 'abc'))
      create_example(data.merge(:id => '3', :name => 'jkl'))
      create_example(data.merge(:id => '4', :name => 'ghi'))
      create_example(data.merge(:id => '5', :name => 'def'))

      expect(example_class.sort(:id).page(1, :per_page => 3).map(&:id)).to eq(['1', '2', '3'])
      expect(example_class.sort(:id).page(2, :per_page => 2).map(&:id)).to eq(['3', '4'])
      expect(example_class.sort(:id).page(3, :per_page => 2).map(&:id)).to eq(['5', '8'])
      expect(example_class.sort(:id).page(3, :per_page => 3).map(&:id)).to eq(['9'])

      expect(example_class.sort(:name).page(1, :per_page => 3).map(&:id)).to eq(['9', '8', '2'])
      expect(example_class.sort(:name).page(2, :per_page => 3).map(&:id)).to eq(['5', '4', '3'])
      expect(example_class.sort(:name).page(3, :per_page => 3).map(&:id)).to eq(['1'])
      expect(example_class.sort(:name).page(4, :per_page => 3).map(&:id)).to eq([])
    end

    it 'filters by records created before a certain time' do
      examples = example_class.intersect(:created_at => Zermelo::Filters::IndexRange.new(nil, time - 150, :by_score => true))
      expect(examples.count).to eq(1)
      expect(examples.map(&:id)).to eq(['8'])
    end

    it 'filters by records created after a certain time' do
      examples = example_class.intersect(:created_at => Zermelo::Filters::IndexRange.new(time - 150, nil, :by_score => true))
      expect(examples.count).to eq(1)
      expect(examples.map(&:id)).to eq(['9'])
    end

    it 'raises an error when trying to filter on a non-indexed value' do
      expect {
        example_class.intersect(:email => 'jjones@example.com').all
      }.to raise_error("'email' property is not indexed")
    end

  end

  context 'influxdb', :influxdb => true, :filter => true do

    let(:influxdb) { Zermelo.influxdb }

    module ZermeloExamples
      class FilterInfluxDB
        include Zermelo::Records::InfluxDB
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

    let(:example_class) { ZermeloExamples::FilterInfluxDB }

    # parent and child keys
    let(:ek) { 'filter_influx_db' }

    def create_example(attrs = {})
      attrs[:active] = attrs[:active].to_s unless attrs[:active].nil?
      attrs[:time]   = attrs[:created_at].to_i
      Zermelo.influxdb.write_point("#{ek}/#{attrs[:id]}", attrs)
    end

    # need to fix the influxdb driver to work with these (see Redis examples above)
    it 'sorts records by an attribute'
    it 'sorts by multiple fields'
    it "returns paginated query responses"
    it 'filters by records created before a certain time'
    it 'filters by records created after a certain time'

    it 'raises an error when trying to filter on a non-indexed value' do
      expect {
        example_class.intersect(:email => 'jjones@example.com').all
      }.to raise_error("Field email doesn't exist in series filter_influx_db/10")
    end

  end
end