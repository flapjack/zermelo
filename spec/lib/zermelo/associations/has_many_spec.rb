require 'spec_helper'
require 'zermelo/records/redis'
require 'zermelo/records/influxdb'
require 'zermelo/associations/has_many'

describe Zermelo::Associations::HasMany do

  shared_examples "has_many functions work", :has_many => true do

    let(:parent) { parent_class.find_by_id('8') }

    it "loads a child from a parent's has_many relationship" do
      create_parent(:id => '8')
      parent = parent_class.find_by_id('8')
      create_child(parent, :id => '3')

      children = parent.children.all

      expect(children).to be_an(Array)
      expect(children.size).to eq(1)
      child = children.first
      expect(child).to be_a(child_class)
      expect(child.id).to eq('3')
    end

    it "loads a parent from a child's belongs_to relationship" do
      create_parent(:id => '8')
      parent = parent_class.find_by_id('8')
      create_child(parent, :id => '3')
      child = child_class.find_by_id('3')

      other_parent = child.parent
      expect(other_parent).not_to be_nil
      expect(other_parent).to be_a(parent_class)
      expect(other_parent.id).to eq('8')
    end

    it "does not add a child if the before_add callback raises an exception" # do
    #   create_parent(:id => '8')
    #   parent = parent_class.find_by_id('8')

    #   create_child(nil, :id => '6', :important => true)
    #   child = child_class.find_by_id('6')

    #   expect(parent.children).to be_empty
    #   expect {
    #     parent.children << child
    #   }.to raise_error
    #   expect(parent.children).to be_empty
    # end

    it 'calls the before/after_read callbacks as part of query execution' # do
    #   create_parent(:id => '8', :name => 'John Jones',
    #                  :email => 'jjones@parent.com', :active => 'true')
    #   parent = parent_class.find_by_id('8')

    #   expect(parent.read).to be_nil
    #   expect(parent.children).to be_empty
    #   expect(parent.read).to eq([:pre, :post])
    # end

    it 'raises an error when calling add on has_many without an argument' do
      expect {
        parent.children.add
      }.to raise_error
    end

    it 'raises an error when calling delete on has_many without an argument' do
      expect {
        parent.children.delete
      }.to raise_error
    end

    context 'filters' do

      before do
        create_parent(:id => '8')

        create_child(parent, :id => '3', :important => true)
        create_child(parent, :id => '4', :important => true)
        create_child(parent, :id => '5', :important => false)
      end

      it "filters has_many records by indexed attribute values" do
        important_kids = parent.children.intersect(:important => true).all
        expect(important_kids).not_to be_nil
        expect(important_kids).to be_an(Array)
        expect(important_kids.size).to eq(2)
        expect(important_kids.map(&:id)).to match_array(['3', '4'])
      end

      it "filters has_many records by intersecting ids" do
        important_kids = parent.children.intersect(:important => true, :id => ['4', '5']).all
        expect(important_kids).not_to be_nil
        expect(important_kids).to be_an(Array)
        expect(important_kids.size).to eq(1)
        expect(important_kids.map(&:id)).to match_array(['4'])
      end

      it "checks whether a record id exists through a has_many filter" do
        expect(parent.children.intersect(:important => true).exists?('3')).to be true
        expect(parent.children.intersect(:important => true).exists?('5')).to be false
      end

      it "finds a record through a has_many filter" do
        child = parent.children.intersect(:important => true).find_by_id('3')
        expect(child).not_to be_nil
        expect(child).to be_a(child_class)
        expect(child.id).to eq('3')
      end

      it 'returns associated ids for multiple parent ids' do
        create_parent(:id => '9')
        parent_9 = parent_class.find_by_id('9')

        create_child(parent_9, :id => '6', :important => false)

        create_parent(:id => '10')

        assoc_ids = parent_class.intersect(:id => [ '8', '9', '10']).
          associated_ids_for(:children)
        expect(assoc_ids).to eq('8'  => Set.new(['3', '4', '5']),
                                '9'  => Set.new(['6']),
                                '10' => Set.new())

        assoc_parent_ids = child_class.intersect(:id => ['3', '4', '5', '6']).
          associated_ids_for(:parent)
        expect(assoc_parent_ids).to eq('3' => '8',
                                       '4' => '8',
                                       '5' => '8',
                                       '6' => '9')
      end
    end
  end

  context 'redis', :redis => true, :has_many => true do

    let(:redis) { Zermelo.redis }

    module ZermeloExamples
      class AssociationsHasManyParentRedis
        include Zermelo::Records::Redis
        has_many :children, :class_name => 'ZermeloExamples::AssociationsHasManyChildRedis',
          :inverse_of => :parent
      end

      class AssociationsHasManyChildRedis
        include Zermelo::Records::Redis
        define_attributes :important => :boolean
        index_by :important
        belongs_to :parent, :class_name => 'ZermeloExamples::AssociationsHasManyParentRedis',
          :inverse_of => :children
      end
    end

    let(:parent_class) { ZermeloExamples::AssociationsHasManyParentRedis }
    let(:child_class) { ZermeloExamples::AssociationsHasManyChildRedis }

    # parent and child keys
    let(:pk) { 'associations_has_many_parent_redis' }
    let(:ck) { 'associations_has_many_child_redis' }

    def create_parent(attrs = {})
      redis.sadd("#{pk}::attrs:ids", attrs[:id])
    end

    def create_child(parent, attrs = {})
      redis.sadd("#{pk}:#{parent.id}:assocs:children_ids", attrs[:id]) unless parent.nil?

      redis.hmset("#{ck}:#{attrs[:id]}:attrs",
                  {'important' => attrs[:important]}.to_a.flatten)

      redis.sadd("#{ck}::indices:by_important:boolean:#{!!attrs[:important]}", attrs[:id])
      redis.hmset("#{ck}:#{attrs[:id]}:assocs:belongs_to",
                  {'parent_id' => parent.id}.to_a.flatten) unless parent.nil?
      redis.sadd("#{ck}::attrs:ids", attrs[:id])
    end

    it "sets a parent/child has_many relationship between two records" do
      create_parent(:id => '8')

      child = child_class.new(:id => '3')
      expect(child.save).to be true

      parent = parent_class.find_by_id('8')
      parent.children << child

      expect(redis.keys('*')).to match_array(["#{pk}::attrs:ids",
                                 "#{pk}:8:assocs:children_ids",
                                 "#{ck}::attrs:ids",
                                 "#{ck}::indices:by_important:null:null",
                                 "#{ck}:3:assocs:belongs_to"])

      expect(redis.smembers("#{pk}::attrs:ids")).to eq(['8'])
      expect(redis.smembers("#{pk}:8:assocs:children_ids")).to eq(['3'])

      expect(redis.smembers("#{ck}::attrs:ids")).to eq(['3'])
    end

    it "removes a parent/child has_many relationship between two records" do
      create_parent(:id => '8')
      parent = parent_class.find_by_id('8')

      create_child(parent, :id => '3', :important => true)
      child = child_class.find_by_id('3')

      expect(redis.smembers("#{ck}::attrs:ids")).to eq(['3'])
      expect(redis.smembers("#{pk}:8:assocs:children_ids")).to eq(['3'])

      parent.children.delete(child)

      expect(redis.smembers("#{ck}::attrs:ids")).to eq(['3'])   # child not deleted
      expect(redis.smembers("#{pk}:8:assocs:children_ids")).to eq([]) # but association is
    end

    it 'clears the belongs_to association when the child record is deleted' do
      create_parent(:id => '8')
      parent = parent_class.find_by_id('8')

      time = Time.now

      create_child(parent, :id => '6', :important => true)
      child = child_class.find_by_id('6')

      expect(redis.keys).to match_array(["#{pk}::attrs:ids",
                            "#{pk}:8:assocs:children_ids",
                            "#{ck}::attrs:ids",
                            "#{ck}::indices:by_important:boolean:true",
                            "#{ck}:6:attrs",
                            "#{ck}:6:assocs:belongs_to"])

      child.destroy

      expect(redis.keys).to match_array(["#{pk}::attrs:ids"])
    end

    it "clears the belongs_to association when the parent record is deleted" do
      create_parent(:id => '8', :name => 'John Jones',
                     :email => 'jjones@parent.com', :active => 'true')
      parent = parent_class.find_by_id('8')

      time = Time.now

      create_child(parent, :id => '6', :name => 'Martin Luther King', :important => true)
      child = child_class.find_by_id('6')

      expect(redis.keys).to match_array(["#{pk}::attrs:ids",
                            "#{pk}:8:assocs:children_ids",
                            "#{ck}::attrs:ids",
                            "#{ck}::indices:by_important:boolean:true",
                            "#{ck}:6:attrs",
                            "#{ck}:6:assocs:belongs_to"])

      parent.destroy

      expect(redis.keys).to match_array(["#{ck}::attrs:ids",
                            "#{ck}::indices:by_important:boolean:true",
                            "#{ck}:6:attrs"])
    end

  end

  context 'influxdb', :influxdb => true, :has_many => true do

    before do
      skip "FIXME"
    end

    let(:influxdb) { Zermelo.influxdb }

    module Zermelo
      class InfluxDBExample
        include Zermelo::Records::InfluxDB

        define_attributes :name   => :string,
                          :email  => :string,
                          :active => :boolean

        validates :name, :presence => true

        has_many :children, :class_name => 'Zermelo::InfluxDBChild'
        # has_sorted_set :sorted, :class_name => 'Zermelo::InfluxDBSorted'
      end

      class InfluxDBChild
        include Zermelo::Records::InfluxDB

        define_attributes :name => :string,
                          :important => :boolean

        belongs_to :example, :class_name => 'Zermelo::InfluxDBExample', :inverse_of => :children

        validates :name, :presence => true
      end

      class InfluxDBSorted
        include Zermelo::Records::InfluxDB

        define_attributes :name => :string,
                          :important => :boolean

        belongs_to :example, :class_name => 'Zermelo::InfluxDBExample', :inverse_of => :sorted

        validates :name, :presence => true
      end
    end

    def create_example(attrs = {})
      Zermelo.influxdb.write_point("influx_db_example/#{attrs[:id]}", attrs)
    end

    it "sets a parent/child has_many relationship between two records in influxdb" do
      create_example(:id => '8', :name => 'John Jones',
                     :email => 'jjones@example.com', :active => 'true')
      example = Zermelo::InfluxDBExample.find_by_id('8')

      child = Zermelo::InfluxDBChild.new(:id => '3', :name => 'Abel Tasman')
      expect(child.save).to be_truthy

      children = example.children.all

      expect(children).to be_an(Array)
      expect(children).to be_empty

      example.children << child

      children = example.children.all

      expect(children).to be_an(Array)
      expect(children.size).to eq(1)
    end

    it "applies an intersect filter to a has_many association" do
      create_example(:id => '8', :name => 'John Jones',
                     :email => 'jjones@example.com', :active => 'true')
      example = Zermelo::InfluxDBExample.find_by_id('8')

      child_1 = Zermelo::InfluxDBChild.new(:id => '3', :name => 'John Smith')
      expect(child_1.save).to be_truthy

      child_2 = Zermelo::InfluxDBChild.new(:id => '4', :name => 'Jane Doe')
      expect(child_2.save).to be_truthy

      example.children.add(child_1, child_2)
      expect(example.children.count).to eq(2)

      result = example.children.intersect(:name => 'John Smith').all
      expect(result).to be_an(Array)
      expect(result.size).to eq(1)
      expect(result.map(&:id)).to eq(['3'])
    end

    it "applies chained intersect and union filters to a has_many association" do
      create_example(:id => '8', :name => 'John Jones',
                     :email => 'jjones@example.com', :active => 'true')
      example = Zermelo::InfluxDBExample.find_by_id('8')

      child_1 = Zermelo::InfluxDBChild.new(:id => '3', :name => 'John Smith')
      expect(child_1.save).to be_truthy

      child_2 = Zermelo::InfluxDBChild.new(:id => '4', :name => 'Jane Doe')
      expect(child_2.save).to be_truthy

      example.children.add(child_1, child_2)
      expect(example.children.count).to eq(2)

      result = example.children.intersect(:name => 'John Smith').union(:id => '4').all
      expect(result).to be_an(Array)
      expect(result.size).to eq(2)
      expect(result.map(&:id)).to eq(['3', '4'])
    end

  end

end