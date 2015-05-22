require 'spec_helper'
require 'zermelo/records/redis'
require 'zermelo/associations/has_and_belongs_to_many'

describe Zermelo::Associations::HasAndBelongsToMany do

  shared_examples "has_many functions work", :has_and_belongs_to_many => true do

    before do
      create_primary(:id => '8', :active => true)
      create_secondary(:id => '2')
    end

    it "loads a record from a has_and_belongs_to_many relationship" do
      primary = primary_class.find_by_id('8')
      secondary = secondary_class.find_by_id('2')

      secondary.primaries << primary

      secondaries = primary.secondaries.all

      expect(secondaries).to be_an(Array)
      expect(secondaries.size).to eq(1)
      other_secondary = secondaries.first
      expect(other_secondary).to be_a(secondary_class)
      expect(other_secondary.id).to eq(secondary.id)
    end

    it 'raises an error when calling add on has_and_belongs_to_many without an argument' do
      expect {
        primary.secondaries.add
      }.to raise_error
    end

    it 'raises an error when calling delete on has_and_belongs_to_many without an argument' do
      expect {
        primary.secondaries.delete
      }.to raise_error
    end

    it "deletes a record from the set" do
      create_primary(:id => '9', :active => false)
      primary = primary_class.find_by_id('8')
      primary_2 = primary_class.find_by_id('9')
      secondary = secondary_class.find_by_id('2')

      secondary.primaries.add(primary, primary_2)

      expect(secondary.primaries.count).to eq(2)
      expect(primary.secondaries.count).to eq(1)
      expect(primary_2.secondaries.count).to eq(1)

      secondary.primaries.delete(primary)

      expect(secondary.primaries.count).to eq(1)
      expect(primary.secondaries.count).to eq(0)
      expect(primary_2.secondaries.count).to eq(1)
      expect(secondary.primaries.ids).to eq(['9'])
      expect(primary.secondaries.ids).to eq([])
      expect(primary_2.secondaries.ids).to eq(['2'])
    end

    it "clears all records from the set" do
      create_primary(:id => '9', :active => false)
      primary = primary_class.find_by_id('8')
      primary_2 = primary_class.find_by_id('9')
      secondary = secondary_class.find_by_id('2')

      secondary.primaries.add(primary, primary_2)

      expect(secondary.primaries.count).to eq(2)
      expect(primary.secondaries.count).to eq(1)
      expect(primary_2.secondaries.count).to eq(1)

      secondary.primaries.clear

      expect(secondary.primaries.count).to eq(0)
      expect(primary.secondaries.count).to eq(0)
      expect(primary_2.secondaries.count).to eq(0)
      expect(secondary.primaries.ids).to eq([])
      expect(primary.secondaries.ids).to eq([])
      expect(primary_2.secondaries.ids).to eq([])
    end

    context 'filters' do

      it "filters has_and_belongs_to_many records by indexed attribute values" do
        create_primary(:id => '9', :active => false)
        create_primary(:id => '10', :active => true)

        primary = primary_class.find_by_id('8')
        primary_2 = primary_class.find_by_id('9')
        primary_3 = primary_class.find_by_id('10')
        secondary = secondary_class.find_by_id('2')

        primary.secondaries << secondary
        primary_2.secondaries << secondary
        primary_3.secondaries << secondary

        primaries = secondary.primaries.intersect(:active => true).all
        expect(primaries).not_to be_nil
        expect(primaries).to be_an(Array)
        expect(primaries.size).to eq(2)
        expect(primaries.map(&:id)).to match_array(['8', '10'])
      end

      it "checks whether a record id exists through a has_and_belongs_to_many filter"  do
        create_primary(:id => '9', :active => false)

        primary = primary_class.find_by_id('8')
        primary_2 = primary_class.find_by_id('9')
        secondary = secondary_class.find_by_id('2')

        primary.secondaries << secondary
        primary_2.secondaries << secondary

        expect(secondary.primaries.intersect(:active => false).exists?('9')).to be true
        expect(secondary.primaries.intersect(:active => false).exists?('8')).to be false
      end

      it "finds a record through a has_and_belongs_to_many filter" do
        create_primary(:id => '9', :active => false)

        primary = primary_class.find_by_id('8')
        primary_2 = primary_class.find_by_id('9')
        secondary = secondary_class.find_by_id('2')

        primary.secondaries << secondary
        primary_2.secondaries << secondary

        james = secondary.primaries.intersect(:active => false).find_by_id('9')
        expect(james).not_to be_nil
        expect(james).to be_a(primary_class)
        expect(james.id).to eq(primary_2.id)
      end

      it 'clears a has_and_belongs_to_many association when a record is deleted'

      it 'returns associated ids for multiple parent ids' do
        create_primary(:id => '9', :active => false)
        primary_9 = primary_class.find_by_id('9')

        create_primary(:id => '10', :active => true)
        primary_10 = primary_class.find_by_id('10')

        create_secondary(:id => '3')
        create_secondary(:id => '4')

        secondary_2 = secondary_class.find_by_id('2')
        secondary_3 = secondary_class.find_by_id('3')
        secondary_4 = secondary_class.find_by_id('4')

        primary_9.secondaries.add(secondary_2)
        primary_10.secondaries.add(secondary_3, secondary_4)

        assoc_ids = primary_class.intersect(:id => ['8', '9', '10']).
          associated_ids_for(:secondaries)
        expect(assoc_ids).to eq('8'  => Set.new([]),
                                '9'  => Set.new(['2']),
                                '10' => Set.new(['3', '4']))
      end
    end
  end

  context 'redis', :redis => true, :has_and_belongs_to_many => true do

    let(:redis) { Zermelo.redis }

    module ZermeloExamples
      class AssociationsHasAndBelongsToManyPrimaryRedis
        include Zermelo::Records::Redis
        define_attributes :active => :boolean
        index_by :active
        has_and_belongs_to_many :secondaries,
          :class_name => 'ZermeloExamples::AssociationsHasAndBelongsToManySecondaryRedis',
          :inverse_of => :primaries
      end

      class AssociationsHasAndBelongsToManySecondaryRedis
        include Zermelo::Records::Redis
        # define_attributes :important => :boolean
        # index_by :important
        has_and_belongs_to_many :primaries,
          :class_name => 'ZermeloExamples::AssociationsHasAndBelongsToManyPrimaryRedis',
          :inverse_of => :secondaries
      end
    end

    let(:primary_class) { ZermeloExamples::AssociationsHasAndBelongsToManyPrimaryRedis }
    let(:secondary_class) { ZermeloExamples::AssociationsHasAndBelongsToManySecondaryRedis }

    # primary and secondary keys
    let(:pk) { 'associations_has_and_belongs_to_many_primary_redis' }
    let(:sk) { 'associations_has_and_belongs_to_many_secondary_redis' }

    def create_primary(attrs = {})
      redis.sadd("#{pk}::attrs:ids", attrs[:id])
      redis.hmset("#{pk}:#{attrs[:id]}:attrs",
                  {'active' => attrs[:active]}.to_a.flatten)
      redis.sadd("#{pk}::indices:by_active:boolean:#{!!attrs[:active]}", attrs[:id])
    end

    def create_secondary(attrs = {})
      redis.sadd("#{sk}::attrs:ids", attrs[:id])
    end

    it "sets a has_and_belongs_to_many relationship between two records" do
      create_primary(:id => '8', :active => true)
      create_secondary(:id => '2')

      primary = primary_class.find_by_id('8')
      secondary = secondary_class.find_by_id('2')

      primary.secondaries << secondary

      expect(redis.keys('*')).to match_array([
        "#{pk}::attrs:ids",
        "#{pk}::indices:by_active:boolean:true",
        "#{pk}:8:attrs",
        "#{pk}:8:assocs:secondaries_ids",
        "#{sk}::attrs:ids",
        "#{sk}:2:assocs:primaries_ids"
      ])

      expect(redis.smembers("#{pk}::attrs:ids")).to eq(['8'])
      expect(redis.smembers("#{pk}::indices:by_active:boolean:true")).to eq(['8'])
      expect(redis.hgetall("#{pk}:8:attrs")).to eq('active' => 'true')
      expect(redis.smembers("#{pk}:8:assocs:secondaries_ids")).to eq(['2'])

      expect(redis.smembers("#{sk}::attrs:ids")).to eq(['2'])
      expect(redis.smembers("#{sk}:2:assocs:primaries_ids")).to eq(['8'])
    end

    it "removes a has_and_belongs_to_many relationship between two records" do
      create_primary(:id => '8', :active => true)
      create_secondary(:id => '2')

      primary = primary_class.find_by_id('8')
      secondary = secondary_class.find_by_id('2')

      secondary.primaries << primary

      expect(redis.smembers("#{sk}::attrs:ids")).to eq(['2'])
      expect(redis.smembers("#{pk}:8:assocs:secondaries_ids")).to eq(['2'])

      primary.secondaries.delete(secondary)

      expect(redis.smembers("#{sk}::attrs:ids")).to eq(['2'])       # secondary not deleted
      expect(redis.smembers("#{pk}:8:assocs:secondaries_ids")).to eq([]) # but association is
    end

    it 'clears a has_and_belongs_to_many association when a record is deleted'

  end

end