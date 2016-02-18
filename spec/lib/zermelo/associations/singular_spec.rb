require 'spec_helper'
require 'zermelo/records/redis'
require 'zermelo/associations/singular'

describe Zermelo::Associations::Singular do

  context 'has_one' do

    shared_examples "has_one functions work", :has_one => true do

      it 'returns associated ids for multiple parent ids' do
        create_parent(:id => '8')
        create_parent(:id => '9')
        create_parent(:id => '10')

        time = Time.now.to_i

        create_child(parent_class.find_by_id('9'),  :id => '3')
        create_child(parent_class.find_by_id('10'), :id => '4')

        assoc_ids = parent_class.intersect(:id => ['8', '9', '10']).
          associated_ids_for(:child)
        expect(assoc_ids).to eq('8'  => nil,
                                '9'  => '3',
                                '10' => '4')
      end

      it 'calls before/after_read callbacks when the value is read' # do
      #   create_parent(:id => '8')
      #   parent = parent_class.find_by_id('8')

      #   expect(parent.read).to be_nil
      #   expect(parent.child).to be_nil
      #   expect(parent.read).to eq([:pre, :post])
      # end

    end

    context 'redis', :redis => true, :has_one => true do

      let(:redis) { Zermelo.redis }

      module ZermeloExamples
        class AssociationsHasOneParentRedis
          include Zermelo::Records::RedisSet
          has_one :child, :class_name => 'ZermeloExamples::AssociationsHasOneChildRedis',
            :inverse_of => :parent
        end

        class AssociationsHasOneChildRedis
          include Zermelo::Records::RedisSet
          belongs_to :parent, :class_name => 'ZermeloExamples::AssociationsHasOneParentRedis',
            :inverse_of => :child
        end
      end

      let(:parent_class) { ZermeloExamples::AssociationsHasOneParentRedis }
      let(:child_class) { ZermeloExamples::AssociationsHasOneChildRedis }

      # parent and child keys
      let(:pk) { 'associations_has_one_parent_redis' }
      let(:ck) { 'associations_has_one_child_redis' }

      def create_parent(attrs = {})
        redis.sadd("#{pk}::attrs:ids", attrs[:id])
      end

      def create_child(parent, attrs = {})
        redis.hset("#{ck}:#{attrs[:id]}:assocs:belongs_to", 'parent_id', parent.id)
        redis.hset("#{pk}:#{parent.id}:assocs:has_one", 'child_id', attrs[:id])
        redis.sadd("#{ck}::attrs:ids", attrs[:id])
      end

      it "sets and retrieves a record via a has_one association" do
        create_parent(:id => '8')

        child = child_class.new(:id => '22')
        expect(child.save).to be true

        parent = parent_class.find_by_id('8')
        parent.child = child

        expect(redis.keys('*')).to match_array([
          "#{pk}::attrs:ids",
          "#{pk}:8:assocs:has_one",
          "#{ck}::attrs:ids",
          "#{ck}:22:assocs:belongs_to"
        ])

        expect(redis.hgetall("#{pk}:8:assocs:has_one")).to eq("child_id" => "22")

        expect(redis.smembers("#{ck}::attrs:ids")).to eq(["22"])

        expect(redis.hgetall("#{ck}:22:assocs:belongs_to")).to eq(
          'parent_id' => '8'
        )

        parent2 = parent_class.find_by_id('8')
        child2 = parent2.child
        expect(child2).not_to be_nil

        expect(child2.id).to eq('22')
        expect(child2.parent.id).to eq('8')
      end

      it 'clears the belongs_to association when the child record is deleted' do
        create_parent(:id => '8')
        parent = parent_class.find_by_id('8')
        create_child(parent, :id => '3')
        child = child_class.find_by_id('3')

        expect(redis.keys).to match_array([
          "#{pk}::attrs:ids",
          "#{pk}:8:assocs:has_one",
          "#{ck}::attrs:ids",
          "#{ck}:3:assocs:belongs_to"
        ])

        child.destroy

        expect(redis.keys).to match_array([
          "#{pk}::attrs:ids",
        ])
      end

      it "clears the belongs_to association when the parent record is deleted" do
        create_parent(:id => '8')
        parent = parent_class.find_by_id('8')
        create_child(parent, :id => '3')

        expect(redis.keys).to match_array([
          "#{pk}::attrs:ids",
          "#{pk}:8:assocs:has_one",
          "#{ck}::attrs:ids",
          "#{ck}:3:assocs:belongs_to"
        ])

        parent.destroy

        expect(redis.keys).to match_array([
          "#{ck}::attrs:ids"
        ])
      end

    end

  end

end