require 'spec_helper'
require 'zermelo/records/redis'
require 'zermelo/associations/has_sorted_set'

describe Zermelo::Associations::HasSortedSet do

  context 'redis', :redis => true do

    let(:redis) { Zermelo.redis }

    let(:time) { Time.now }

    module ZermeloExamples
      class AssociationsHasSortedSetParentRedis
        include Zermelo::Records::Redis
        has_sorted_set :children, :class_name => 'ZermeloExamples::AssociationsHasSortedSetChildRedis',
          :inverse_of => :parent, :sort_key => :timestamp
      end

      class AssociationsHasSortedSetChildRedis
        include Zermelo::Records::Redis
        define_attributes :emotion => :string,
                          :timestamp => :timestamp
        index_by :emotion
        range_index_by :timestamp
        belongs_to :parent, :class_name => 'ZermeloExamples::AssociationsHasSortedSetParentRedis',
          :inverse_of => :children
      end
    end

    let(:parent_class) { ZermeloExamples::AssociationsHasSortedSetParentRedis }
    let(:child_class) { ZermeloExamples::AssociationsHasSortedSetChildRedis }

    # parent and child keys
    let(:pk) { 'associations_has_sorted_set_parent_redis' }
    let(:ck) { 'associations_has_sorted_set_child_redis' }

    def create_parent(attrs = {})
      redis.sadd("#{pk}::attrs:ids", attrs[:id])
    end

    def create_child(parent, attrs = {})
      redis.zadd("#{pk}:#{parent.id}:assocs:children_ids",  attrs[:timestamp].to_f, attrs[:id]) unless parent.nil?

      redis.hmset("#{ck}:#{attrs[:id]}:attrs", {:emotion => attrs[:emotion],
        'timestamp' => attrs[:timestamp].to_f}.to_a.flatten)

      redis.sadd("#{ck}::indices:by_emotion:string:#{attrs[:emotion]}", attrs[:id])
      redis.zadd("#{ck}::indices:by_timestamp", attrs[:timestamp].to_f, attrs[:id])
      redis.hmset("#{ck}:#{attrs[:id]}:assocs:belongs_to",
                  {'parent_id' => parent.id}.to_a.flatten) unless parent.nil?
      redis.sadd("#{ck}::attrs:ids", attrs[:id])
    end

    let(:parent) {
      create_parent(:id => '8')
      parent_class.find_by_id('8')
    }

    it "sets a parent/child has_sorted_set relationship between two records in redis" do
      child = child_class.new(:id => '4', :emotion => 'indifferent',
                              :timestamp => time)
      expect(child.save).to be_truthy

      parent.children << child

      expect(redis.keys('*')).to match_array(["#{pk}::attrs:ids",
                                 "#{pk}:8:assocs:children_ids",
                                 "#{ck}::attrs:ids",
                                 "#{ck}::indices:by_emotion:string:indifferent",
                                 "#{ck}::indices:by_timestamp",
                                 "#{ck}:4:attrs",
                                 "#{ck}:4:assocs:belongs_to"])

      expect(redis.smembers("#{ck}::attrs:ids")).to eq(['4'])
      expect(redis.hgetall("#{ck}:4:attrs")).to eq(
        {'emotion' => 'indifferent', 'timestamp' => time.to_f.to_s}
      )
      expect(redis.hgetall("#{ck}:4:assocs:belongs_to")).to eq(
        {'parent_id' => '8'}
      )

      result = redis.zrange("#{pk}:8:assocs:children_ids", 0, -1,
        :with_scores => true) # .should == [['4', time.to_f]]
      expect(result.size).to eq(1)
      expect(result.first.first).to eq('4')
      expect(result.first.last).to be_within(0.001).of(time.to_f)
    end

    it "loads a child from a parent's has_sorted_set relationship" do
      create_child(parent, :id => '4', :emotion => 'indifferent', :timestamp => time)
      child = child_class.find_by_id('4')

      children = parent.children.all

      expect(children).to be_an(Array)
      expect(children.size).to eq(1)
      child = children.first
      expect(child).to be_a(child_class)
      expect(child.timestamp).to be_within(1).of(time) # ignore fractional differences
    end

    it "removes a parent/child has_sorted_set relationship between two records" do
      create_child(parent, :id => '4', :emotion => 'indifferent', :timestamp => time)
      child = child_class.find_by_id('4')

      expect(redis.smembers("#{ck}::attrs:ids")).to eq(['4'])
      expect(redis.zrange("#{pk}:8:assocs:children_ids", 0, -1)).to eq(['4'])

      parent.children.delete(child)

      expect(redis.smembers("#{ck}::attrs:ids")).to eq(['4'])    # child not deleted
      expect(redis.zrange("#{pk}:8:assocs.children_ids", 0, -1)).to eq([]) # but association is
    end

  #   it "clears the belongs_to association when the parent record is deleted" do
  #     create_parent(:id => '8', :name => 'John Jones',
  #                    :email => 'jjones@example.com', :active => 'true')
  #     parent = parent_class.find_by_id('8')

  #     time = Time.now

  #     create_child(example, :id => '6', :summary => 'aaargh', :timestamp => time.to_i + 20,
  #       :emotion => 'upset')

  #     expect(redis.keys).to match_array(["#{pk}::attrs:ids",
  #                           "#{pk}::indices:by_name",
  #                           "#{pk}::indices:by_active:boolean:true",
  #                           "#{pk}:8:attrs",
  #                           "#{pk}:8:assocs:data_ids",
  #                           "#{ck}::attrs:ids",
  #                           "#{ck}::indices:by_emotion:string:upset",
  #                           "#{ck}:6:attrs",
  #                           "#{ck}:6:assocs:belongs_to"])

  #     example.destroy

  #     expect(redis.keys).to match_array(["#{ck}::attrs:ids",
  #                           "#{ck}::indices:by_emotion:string:upset",
  #                           "#{ck}:6:attrs"])
  #   end

  #   it 'returns associated ids for multiple parent ids' do
  #     create_parent(:id => '8', :name => 'John Jones',
  #                    :email => 'jjones@example.com', :active => 'true')
  #     example_8 = parent_class.find_by_id('8')

  #     create_parent(:id => '9', :name => 'Jane Johnson',
  #                    :email => 'jjohnson@example.com', :active => 'true')

  #     create_parent(:id => '10', :name => 'Jim Smith',
  #                    :email => 'jsmith@example.com', :active => 'true')
  #     example_10 = parent_class.find_by_id('10')

  #     time = Time.now.to_i

  #     create_child(example_8, :id => '3', :summary => 'aaargh', :timestamp => time.to_i + 20,
  #       :emotion => 'ok')
  #     create_child(example_8, :id => '4', :summary => 'aaargh', :timestamp => time.to_i + 30,
  #       :emotion => 'ok')
  #     create_child(example_10, :id => '5', :summary => 'aaargh', :timestamp => time.to_i + 40,
  #       :emotion => 'not_ok')

  #     assoc_ids = parent_class.intersect(:id => ['8', '9', '10']).
  #       associated_ids_for(:data)
  #     expect(assoc_ids).to eq('8'  => ['3', '4'],
  #                             '9'  => [],
  #                             '10' => ['5'])
  #   end

    context 'filters' do
      before do
        create_child(parent, :id => '4', :timestamp => time - 20,
          :emotion => 'upset')
        create_child(parent, :id => '5', :timestamp => time - 10,
          :emotion => 'happy')
        create_child(parent, :id => '6', :timestamp => time,
          :emotion => 'upset')
      end

      it "by indexed attribute values" do
        upset_children = parent.children.intersect(:emotion => 'upset').all
        expect(upset_children).not_to be_nil
        expect(upset_children).to be_an(Array)
        expect(upset_children.size).to eq(2)
        expect(upset_children.map(&:id)).to eq(['4', '6'])
      end

      it "by indexed attribute values with a regex search" do
        upset_children = parent.children.intersect(:emotion => /^ups/).all
        expect(upset_children).not_to be_nil
        expect(upset_children).to be_an(Array)
        expect(upset_children.size).to eq(2)
        expect(upset_children.map(&:id)).to eq(['4', '6'])
      end

      it "a subset of a sorted set by index" do
        range = Zermelo::Filters::IndexRange.new(0, 1)
        children = parent.children.intersect(:timestamp => range).all
        expect(children).not_to be_nil
        expect(children).to be_an(Array)
        expect(children.size).to eq(2)
        expect(children.map(&:id)).to eq(['4', '5'])
      end

      it "a reversed subset of a sorted set by index" do
        range = Zermelo::Filters::IndexRange.new(1, 2)
        children = parent.children.intersect(:timestamp => range).sort(:id, :desc => true).all
        expect(children).not_to be_nil
        expect(children).to be_an(Array)
        expect(children.size).to eq(2)
        expect(children.map(&:id)).to eq(['6', '5'])
      end

      it "a subset of a sorted set by score" do
        range = Zermelo::Filters::IndexRange.new((time - 25).to_f, (time - 5).to_f, :by_score => true)
        children = parent.children.intersect(:timestamp => range).all
        expect(children).not_to be_nil
        expect(children).to be_an(Array)
        expect(children.size).to eq(2)
        expect(children.map(&:id)).to eq(['4', '5'])
      end

      it "a reversed subset of a sorted set by score" do
        range = Zermelo::Filters::IndexRange.new((time - 25).to_f, (time - 5).to_f, :by_score => true)
        children = parent.children.intersect(:timestamp => range).
                     sort(:timestamp, :desc => true).all
        expect(children).not_to be_nil
        expect(children).to be_an(Array)
        expect(children.size).to eq(2)
        expect(children.map(&:id)).to eq(['5', '4'])
      end

      it "checks whether a record exists" do
        expect(parent.children.intersect(:emotion => 'upset').exists?('4')).to be true
        expect(parent.children.intersect(:emotion => 'upset').exists?('5')).to be false
      end

      it "finds a record" do
        child = parent.children.intersect(:emotion => 'upset').find_by_id('4')
        expect(child).not_to be_nil
        expect(child).to be_a(child_class)
        expect(child.id).to eq('4')
      end

      it "the union of a sorted set by index"
      it "a reversed union of a sorted set by index"

      it "the union of a sorted set by score"
      it "a reversed union of a sorted set by score"

      it "the exclusion of a sorted set by index" do
        range = Zermelo::Filters::IndexRange.new(0, 1)
        children = parent.children.diff(:timestamp => range).all
        expect(children).not_to be_nil
        expect(children).to be_an(Array)
        expect(children.size).to eq(1)
        expect(children.map(&:id)).to eq(['6'])
      end

    #   it "a reversed exclusion of a sorted set by index" do
    #     data = example.data.diff_range(2, 2).sort(:id, :desc => true).all
    #     expect(data).not_to be_nil
    #     expect(data).to be_an(Array)
    #     expect(data.size).to eq(2)
    #     expect(data.map(&:id)).to eq(['5', '4'])
    #   end

    #   it "the exclusion of a sorted set by score" do
    #     data = example.data.diff_range(time.to_i - 1, time.to_i + 15, :by_score => true).all
    #     expect(data).not_to be_nil
    #     expect(data).to be_an(Array)
    #     expect(data.size).to eq(1)
    #     expect(data.map(&:id)).to eq(['6'])
    #   end

    #   it "a reversed exclusion of a sorted set by score" do
    #     data = example.data.diff_range(time.to_i - 1, time.to_i + 8, :by_score => true).
    #       sort(:timestamp, :desc => true).all
    #     expect(data).not_to be_nil
    #     expect(data).to be_an(Array)
    #     expect(data.size).to eq(2)
    #     expect(data.map(&:id)).to eq(['6', '5'])
    #   end

    end

  #   it 'clears the belongs_to association when the child record is deleted' do
  #     create_parent(:id => '8', :name => 'John Jones',
  #                    :email => 'jjones@example.com', :active => 'true')
  #     parent = parent_class.find_by_id('8')

  #     time = Time.now

  #     create_child(example, :id => '6', :summary => 'aaargh', :timestamp => time.to_i + 20,
  #       :emotion => 'upset')
  #     child = child_class.find_by_id('6')

  #     expect(redis.keys).to match_array(["#{pk}::attrs:ids",
  #                           "#{pk}::indices:by_name",
  #                           "#{pk}::indices:by_active:boolean:true",
  #                           "#{pk}:8:attrs",
  #                           "#{pk}:8:assocs:data_ids",
  #                           "#{ck}::attrs:ids",
  #                           "#{ck}::indices:by_emotion:string:upset",
  #                           "#{ck}:6:attrs",
  #                           "#{ck}:6:assocs:belongs_to"])

  #     datum.destroy

  #     expect(redis.keys).to match_array(["#{pk}::attrs:ids",
  #                           "#{pk}::indices:by_name",
  #                           "#{pk}::indices:by_active:boolean:true",
  #                           "#{pk}:8:attrs"])
  #   end

  end

end