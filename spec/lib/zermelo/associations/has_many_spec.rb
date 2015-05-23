require 'spec_helper'
require 'zermelo/records/redis'
require 'zermelo/records/influxdb'
require 'zermelo/associations/has_many'

describe Zermelo::Associations::HasMany do

  shared_examples "has_many functions work", :has_many => true do

    let(:parent) {
      create_parent(:id => '8')
      parent_class.find_by_id('8')
    }

    it "sets a parent/child has_many relationship between two records" do
      child = child_class.new(:id => '3', :important => true)
      expect(child.save).to be_truthy

      children = parent.children.all

      expect(children).to be_an(Array)
      expect(children).to be_empty

      parent.children << child

      children = parent.children.all

      expect(children).to be_an(Array)
      expect(children.size).to eq(1)
    end

    it "loads a child from a parent's has_many relationship" do
      create_child(parent, :id => '3')

      children = parent.children.all

      expect(children).to be_an(Array)
      expect(children.size).to eq(1)
      child = children.first
      expect(child).to be_a(child_class)
      expect(child.id).to eq('3')
    end

    it "loads a parent from a child's belongs_to relationship" do
      create_child(parent, :id => '3')
      child = child_class.find_by_id('3')

      other_parent = child.parent
      expect(other_parent).not_to be_nil
      expect(other_parent).to be_a(parent_class)
      expect(other_parent.id).to eq('8')
    end

    it "deletes a record from the set" do
      create_child(parent, :id => '3')
      create_child(parent, :id => '4')

      expect(parent.children.count).to eq(2)
      child = child_class.find_by_id('3')
      parent.children.remove(child)
      expect(parent.children.count).to eq(1)
      expect(parent.children.ids).to eq(['4'])
    end

    it "deletes a record from the set by id" do
      create_child(parent, :id => '3')
      create_child(parent, :id => '4')

      expect(parent.children.count).to eq(2)
      parent.children.remove_ids('3')
      expect(parent.children.count).to eq(1)
      expect(parent.children.ids).to eq(['4'])
    end

    it "clears all records from the set" do
      create_child(parent, :id => '3')
      create_child(parent, :id => '4')

      expect(parent.children.count).to eq(2)
      child = child_class.find_by_id('3')
      parent.children.clear
      expect(parent.children.count).to eq(0)
      expect(parent.children.ids).to eq([])
    end

    it "does not add a child if the before_add callback raises an exception" # do
    #   create_child(nil, :id => '6', :important => true)
    #   child = child_class.find_by_id('6')

    #   expect(parent.children).to be_empty
    #   expect {
    #     parent.children << child
    #   }.to raise_error
    #   expect(parent.children).to be_empty
    # end

    it 'calls the before/after_read callbacks as part of query execution' # do
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
        parent.children.remove
      }.to raise_error
    end

    context 'filters' do

      before do
        create_child(parent, :id => '3', :important => true)
        create_child(parent, :id => '4', :important => true)
        create_child(parent, :id => '5', :important => false)
      end

      it "by indexed attribute values" do
        important_kids = parent.children.intersect(:important => true).all
        expect(important_kids).not_to be_nil
        expect(important_kids).to be_an(Array)
        expect(important_kids.size).to eq(2)
        expect(important_kids.map(&:id)).to match_array(['3', '4'])
      end

      it "by intersecting ids" do
        important_kids = parent.children.intersect(:important => true, :id => ['4', '5']).all
        expect(important_kids).not_to be_nil
        expect(important_kids).to be_an(Array)
        expect(important_kids.size).to eq(1)
        expect(important_kids.map(&:id)).to match_array(['4'])
      end

      it "applies chained intersect and union filters to a has_many association" do
        create_child(parent, :id => '3', :important => true)
        create_child(parent, :id => '4', :important => false)

        result = parent.children.intersect(:important => true).union(:id => '4').all
        expect(result).to be_an(Array)
        expect(result.size).to eq(2)
        expect(result.map(&:id)).to eq(['3', '4'])
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

      parent.children.remove(child)

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

    let(:influxdb) { Zermelo.influxdb }

    module ZermeloExamples
      class AssociationsHasManyParentInfluxDB
        include Zermelo::Records::InfluxDB
        has_many :children, :class_name => 'ZermeloExamples::AssociationsHasManyChildInfluxDB',
          :inverse_of => :parent
      end

      class AssociationsHasManyChildInfluxDB
        include Zermelo::Records::InfluxDB
        define_attributes :important => :boolean
        index_by :important
        belongs_to :parent, :class_name => 'ZermeloExamples::AssociationsHasManyParentInfluxDB',
          :inverse_of => :children
      end
    end

    let(:parent_class) { ZermeloExamples::AssociationsHasManyParentInfluxDB }
    let(:child_class) { ZermeloExamples::AssociationsHasManyChildInfluxDB }

    # parent and child keys
    let(:pk) { 'associations_has_many_parent_influx_db' }
    let(:ck) { 'associations_has_many_child_influx_db' }

    def create_parent(attrs = {})
      Zermelo.influxdb.write_point("#{pk}/#{attrs[:id]}", attrs)
    end

    def create_child(par, attrs = {})
      attrs[:important] = attrs[:important].to_s unless attrs[:important].nil?
      Zermelo.influxdb.write_point("#{ck}/#{attrs[:id]}", attrs)
      par.children.add(child_class.find_by_id!(attrs[:id]))
    end

  end

end