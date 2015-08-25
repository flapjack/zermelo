require 'spec_helper'
require 'zermelo/records/redis'
require 'zermelo/records/influxdb'
require 'zermelo/associations/multiple'

describe Zermelo::Associations::Multiple do

  context 'has_many' do

    shared_examples "has_many functions work", :has_many => true do

      let(:parent) {
        create_parent(:id => '8')
        parent_class.find_by_id('8')
      }

      it "sets a parent/child has_many relationship between two records" do
        child = child_class.new(:id => '3', :important => true)
        expect(child.save).to be_truthy

        children = parent.children.all

        expect(children).to be_a(Set)
        expect(children).to be_empty

        parent.children << child

        children = parent.children.all

        expect(children).to be_a(Set)
        expect(children.size).to eq(1)
      end

      it "loads a child from a parent's has_many relationship" do
        create_child(parent, :id => '3')

        children = parent.children.all

        expect(children).to be_a(Set)
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
        expect(parent.children.ids).to eq(Set.new(['4']))
      end

      it "deletes a record from the set by id" do
        create_child(parent, :id => '3')
        create_child(parent, :id => '4')

        expect(parent.children.count).to eq(2)
        parent.children.remove_ids('3')
        expect(parent.children.count).to eq(1)
        expect(parent.children.ids).to eq(Set.new(['4']))
      end

      it "clears all records from the set" do
        create_child(parent, :id => '3')
        create_child(parent, :id => '4')

        expect(parent.children.count).to eq(2)
        child = child_class.find_by_id('3')
        parent.children.clear
        expect(parent.children.count).to eq(0)
        expect(parent.children.ids).to eq(Set.new)
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
        }.to raise_error("No records to add")
      end

      it 'raises an error when calling delete on has_many without an argument' do
        expect {
          parent.children.remove
        }.to raise_error("No records to remove")
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
          expect(important_kids).to be_a(Set)
          expect(important_kids.size).to eq(2)
          expect(important_kids.map(&:id)).to eq(['3', '4'])
        end

        it "by intersecting ids" do
          important_kids = parent.children.intersect(:important => true, :id => ['4', '5']).all
          expect(important_kids).not_to be_nil
          expect(important_kids).to be_a(Set)
          expect(important_kids.size).to eq(1)
          expect(important_kids.map(&:id)).to match_array(['4'])
        end

        it "applies chained intersect and union filters to a has_many association" do
          create_child(parent, :id => '3', :important => true)
          create_child(parent, :id => '4', :important => false)

          result = parent.children.intersect(:important => true).union(:id => '4').all
          expect(result).to be_a(Set)
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
                                  '10' => Set.new)

          assoc_parent_ids = child_class.intersect(:id => ['3', '4', '5', '6']).
            associated_ids_for(:parent)
          expect(assoc_parent_ids).to eq('3' => '8',
                                         '4' => '8',
                                         '5' => '8',
                                         '6' => '9')
        end

        it 'returns associations for multiple parent ids' do
          create_parent(:id => '9')
          parent_9 = parent_class.find_by_id('9')

          create_child(parent_9, :id => '6', :important => false)

          create_parent(:id => '10')

          assocs = parent_class.intersect(:id => [ '8', '9', '10']).
            associations_for(:children)
          expect(assocs).to be_a(Hash)
          expect(assocs.keys).to match_array(['8', '9', '10'])
          expect(assocs.values.all? {|r| r.is_a?(Zermelo::Associations::Multiple)}).to be true

          expect(assocs['8'].count).to eq(3)
          expect(assocs['8'].ids).to eq(Set.new(['3', '4', '5']))
          expect(assocs['9'].count).to eq(1)
          expect(assocs['9'].ids).to eq(Set.new(['6']))
          expect(assocs['10'].count).to eq(0)
          expect(assocs['10'].ids).to eq(Set.new)
        end
      end
    end

    context 'redis', :redis => true, :has_many => true do

      let(:redis) { Zermelo.redis }

      module ZermeloExamples
        class AssociationsHasManyParentRedis
          include Zermelo::Records::RedisSet
          has_many :children, :class_name => 'ZermeloExamples::AssociationsHasManyChildRedis',
            :inverse_of => :parent
        end

        class AssociationsHasManyChildRedis
          include Zermelo::Records::RedisSet
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

      it 'queries using association objects' do
        create_parent(:id => '8')
        parent_8 = parent_class.find_by_id('8')
        create_child(parent_8, :id => '5')
        create_child(parent_8, :id => '6')

        create_parent(:id => '9')
        parent_9 = parent_class.find_by_id('9')
        create_child(parent_9, :id => '7')

        create_parent(:id => '10')

        assocs = parent_class.intersect(:id => ['8', '10']).
          associations_for(:children).values

        children = child_class.intersect(:id => assocs)
        expect(children.count).to eq(2)
        expect(children.ids).to eq(Set.new(['5', '6']))
      end

      it 'queries using multiple association objects' do
        create_parent(:id => '8')
        parent_8 = parent_class.find_by_id('8')
        create_child(parent_8, :id => '5')
        create_child(parent_8, :id => '6')

        create_parent(:id => '9')
        parent_9 = parent_class.find_by_id('9')
        create_child(parent_9, :id => '7')

        create_parent(:id => '10')
        parent_10 = parent_class.find_by_id('10')
        create_child(parent_10, :id => '4')

        children = child_class.intersect(:id => [parent_8.children, parent_9.children])
        expect(children.count).to eq(3)
        expect(children.ids).to eq(Set.new(['5', '6', '7']))
      end

      it 'queries using a single filter object' do
        create_parent(:id => '8')
        parent_8 = parent_class.find_by_id('8')
        create_child(parent_8, :id => '5')
        create_child(parent_8, :id => '6')

        create_parent(:id => '9')
        parent_9 = parent_class.find_by_id('9')
        create_child(parent_9, :id => '7')

        create_parent(:id => '10')

        par = parent_class.intersect(:id => ['8', '10'])

        parent_ids = parent_class.intersect(:id => par).ids
        expect(parent_ids).to eq(Set.new(['8', '10']))
      end

      it 'queries using multiple filter objects' do
        create_parent(:id => '8')
        parent_8 = parent_class.find_by_id('8')
        create_child(parent_8, :id => '5')
        create_child(parent_8, :id => '6')

        create_parent(:id => '9')
        parent_9 = parent_class.find_by_id('9')
        create_child(parent_9, :id => '7')

        create_parent(:id => '10')

        par_1 = parent_class.intersect(:id => ['8'])
        par_2 = parent_class.intersect(:id => ['10'])

        parent_ids = parent_class.intersect(:id => [par_1, par_2]).ids
        expect(parent_ids).to eq(Set.new(['8', '10']))
      end

      it 'queries using a combination of bare value, association and filter object' do
        create_parent(:id => '8')
        parent_8 = parent_class.find_by_id('8')
        create_child(parent_8, :id => '5')
        create_child(parent_8, :id => '6')

        create_parent(:id => '9')
        parent_9 = parent_class.find_by_id('9')
        create_child(parent_9, :id => '7')

        create_parent(:id => '10')
        parent_10 = parent_class.find_by_id('10')
        create_child(parent_10, :id => '4')

        assocs = parent_class.intersect(:id => ['8']).
          associations_for(:children).values

        children = child_class.intersect(:id => assocs + [
          parent_9.children.intersect(:id => '7'), '4'
        ])
        expect(children.count).to eq(4)
        expect(children.ids).to eq(Set.new(['4', '5', '6', '7']))
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

      # FIXME not implemented yet for InfluxDB, see SetStep
      it 'queries associated filters transparently'

    end

  end



  context 'has_and_belongs_to_many' do

    shared_examples "has_and_belongs_to_many functions work", :has_and_belongs_to_many => true do

      before do
        create_primary(:id => '8', :active => true)
        create_secondary(:id => '2')
      end

      it "loads a record from a has_and_belongs_to_many relationship" do
        primary = primary_class.find_by_id('8')
        secondary = secondary_class.find_by_id('2')

        secondary.primaries << primary

        secondaries = primary.secondaries.all

        expect(secondaries).to be_a(Set)
        expect(secondaries.size).to eq(1)
        other_secondary = secondaries.first
        expect(other_secondary).to be_a(secondary_class)
        expect(other_secondary.id).to eq(secondary.id)
      end

      it 'raises an error when calling add on has_and_belongs_to_many without an argument' do
        primary = primary_class.find_by_id('8')

        expect {
          primary.secondaries.add
        }.to raise_error("No records to add")
      end

      it 'raises an error when calling delete on has_and_belongs_to_many without an argument' do
        primary = primary_class.find_by_id('8')

        expect {
          primary.secondaries.remove
        }.to raise_error("No records to remove")
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

        secondary.primaries.remove(primary)

        expect(secondary.primaries.count).to eq(1)
        expect(primary.secondaries.count).to eq(0)
        expect(primary_2.secondaries.count).to eq(1)
        expect(secondary.primaries.ids).to eq(Set.new(['9']))
        expect(primary.secondaries.ids).to eq(Set.new)
        expect(primary_2.secondaries.ids).to eq(Set.new(['2']))
      end

      it "deletes a record from the set by id" do
        create_primary(:id => '9', :active => false)
        primary = primary_class.find_by_id('8')
        primary_2 = primary_class.find_by_id('9')
        secondary = secondary_class.find_by_id('2')

        secondary.primaries.add(primary, primary_2)

        expect(secondary.primaries.count).to eq(2)
        expect(primary.secondaries.count).to eq(1)
        expect(primary_2.secondaries.count).to eq(1)

        secondary.primaries.remove_ids('8')

        expect(secondary.primaries.count).to eq(1)
        expect(primary.secondaries.count).to eq(0)
        expect(primary_2.secondaries.count).to eq(1)
        expect(secondary.primaries.ids).to eq(Set.new(['9']))
        expect(primary.secondaries.ids).to eq(Set.new)
        expect(primary_2.secondaries.ids).to eq(Set.new(['2']))
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
        expect(secondary.primaries.ids).to eq(Set.new)
        expect(primary.secondaries.ids).to eq(Set.new)
        expect(primary_2.secondaries.ids).to eq(Set.new)
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
          expect(primaries).to be_a(Set)
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
          include Zermelo::Records::RedisSet
          define_attributes :active => :boolean
          index_by :active
          has_and_belongs_to_many :secondaries,
            :class_name => 'ZermeloExamples::AssociationsHasAndBelongsToManySecondaryRedis',
            :inverse_of => :primaries
        end

        class AssociationsHasAndBelongsToManySecondaryRedis
          include Zermelo::Records::RedisSet
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

        primary.secondaries.remove(secondary)

        expect(redis.smembers("#{sk}::attrs:ids")).to eq(['2'])       # secondary not deleted
        expect(redis.smembers("#{pk}:8:assocs:secondaries_ids")).to eq([]) # but association is
      end

      it 'clears a has_and_belongs_to_many association when a record is deleted'

    end

  end

  context 'has_sorted_set' do

    shared_examples "has_sorted_set functions work", :has_sorted_set => true do

      let(:time) { Time.now }

      # TODO
    end

    context 'redis', :redis => true, :has_sorted_set => true do

      let(:redis) { Zermelo.redis }

      module ZermeloExamples
        class AssociationsHasSortedSetParentRedis
          include Zermelo::Records::RedisSet
          has_sorted_set :children, :class_name => 'ZermeloExamples::AssociationsHasSortedSetChildRedis',
            :inverse_of => :parent, :key => :timestamp
          has_sorted_set :reversed_children, :class_name => 'ZermeloExamples::AssociationsHasSortedSetChildRedis',
            :inverse_of => :reversed_parent, :key => :timestamp, :order => :desc
        end

        class AssociationsHasSortedSetChildRedis
          include Zermelo::Records::RedisSortedSet
          define_attributes :emotion => :string,
                            :timestamp => :timestamp
          define_sort_attribute :timestamp
          index_by :emotion
          belongs_to :parent, :class_name => 'ZermeloExamples::AssociationsHasSortedSetParentRedis',
            :inverse_of => :children
          belongs_to :reversed_parent, :class_name => 'ZermeloExamples::AssociationsHasSortedSetParentRedis',
            :inverse_of => :reversed_children
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
        redis.hmset("#{ck}:#{attrs[:id]}:assocs:belongs_to",
                    {'parent_id' => parent.id}.to_a.flatten) unless parent.nil?
        redis.zadd("#{ck}::attrs:ids", attrs[:timestamp].to_f, attrs[:id])
      end

      def create_reversed_child(parent, attrs = {})
        redis.zadd("#{pk}:#{parent.id}:assocs:reversed_children_ids",  attrs[:timestamp].to_f, attrs[:id]) unless parent.nil?

        redis.hmset("#{ck}:#{attrs[:id]}:attrs", {:emotion => attrs[:emotion],
          'timestamp' => attrs[:timestamp].to_f}.to_a.flatten)

        redis.sadd("#{ck}::indices:by_emotion:string:#{attrs[:emotion]}", attrs[:id])
        redis.hmset("#{ck}:#{attrs[:id]}:assocs:belongs_to",
                    {'reversed_parent_id' => parent.id}.to_a.flatten) unless parent.nil?
        redis.zadd("#{ck}::attrs:ids", attrs[:timestamp].to_f, attrs[:id])
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
                                   "#{ck}:4:attrs",
                                   "#{ck}:4:assocs:belongs_to"])

        expect(redis.zrange("#{ck}::attrs:ids", 0, -1)).to eq(['4'])
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

        expect(children).to be_a(Zermelo::OrderedSet)
        expect(children.size).to eq(1)
        child = children.first
        expect(child).to be_a(child_class)
        expect(child.timestamp).to be_within(1).of(time) # ignore fractional differences
      end

      it "removes a parent/child has_sorted_set relationship between two records" do
        create_child(parent, :id => '4', :emotion => 'indifferent', :timestamp => time)
        child = child_class.find_by_id('4')

        expect(redis.zrange("#{ck}::attrs:ids", 0, -1)).to eq(['4'])
        expect(redis.zrange("#{pk}:8:assocs:children_ids", 0, -1)).to eq(['4'])

        parent.children.remove(child)

        expect(redis.zrange("#{ck}::attrs:ids", 0, -1)).to eq(['4'])         # child not deleted
        expect(redis.zrange("#{pk}:8:assocs:children_ids", 0, -1)).to eq([]) # but association is
      end

      it "clears the belongs_to association when the parent record is deleted" do
        create_child(parent, :id => '6', :timestamp => time,
          :emotion => 'upset')

        expect(redis.keys).to match_array(["#{pk}::attrs:ids",
                              "#{pk}:8:assocs:children_ids",
                              "#{ck}::attrs:ids",
                              "#{ck}::indices:by_emotion:string:upset",
                              "#{ck}:6:attrs",
                              "#{ck}:6:assocs:belongs_to"])

        parent.destroy

        expect(redis.keys).to match_array(["#{ck}::attrs:ids",
                              "#{ck}::indices:by_emotion:string:upset",
                              "#{ck}:6:attrs"])
      end

      it 'sets the score in a sorted set appropriately when assigned from the belongs_to' do
        create_child(nil, :id => '4', :timestamp => time - 20,
          :emotion => 'upset')

        child = child_class.find_by_id!('4')
        child.parent = parent

        expect(redis.zrange("#{pk}:8:assocs:children_ids", 0, -1, :with_scores => true)).to eq([['4', (time - 20).to_f]])
      end

      it 'returns the first record' do
        create_child(parent, :id => '4', :timestamp => time - 20,
          :emotion => 'upset')
        create_child(parent, :id => '5', :timestamp => time - 10,
          :emotion => 'happy')
        create_child(parent, :id => '6', :timestamp => time,
          :emotion => 'upset')

        child = parent.children.first
        expect(child).not_to be_nil
        expect(child.id).to eq('4')
      end

      it 'returns the last record for first if reversed' do
        create_reversed_child(parent, :id => '4', :timestamp => time - 20,
          :emotion => 'upset')
        create_reversed_child(parent, :id => '5', :timestamp => time - 10,
          :emotion => 'happy')
        create_reversed_child(parent, :id => '6', :timestamp => time,
          :emotion => 'upset')

        child = parent.reversed_children.first
        expect(child).not_to be_nil
        expect(child.id).to eq('6')
      end

      it 'returns the last record' do
        create_child(parent, :id => '4', :timestamp => time - 20,
          :emotion => 'upset')
        create_child(parent, :id => '5', :timestamp => time - 10,
          :emotion => 'happy')
        create_child(parent, :id => '6', :timestamp => time,
          :emotion => 'upset')

        child = parent.children.last
        expect(child).not_to be_nil
        expect(child.id).to eq('6')
      end

      it 'returns the first record for last if reversed' do
        create_reversed_child(parent, :id => '4', :timestamp => time - 20,
          :emotion => 'upset')
        create_reversed_child(parent, :id => '5', :timestamp => time - 10,
          :emotion => 'happy')
        create_reversed_child(parent, :id => '6', :timestamp => time,
          :emotion => 'upset')

        child = parent.reversed_children.last
        expect(child).not_to be_nil
        expect(child.id).to eq('4')
      end

      it 'returns associated ids for multiple parent ids' do
        create_parent(:id => '9')

        create_parent(:id => '10')
        parent_10 = parent_class.find_by_id('10')

        time = Time.now.to_i

        create_child(parent, :id => '3', :timestamp => time - 20,
          :emotion => 'ok')
        create_child(parent, :id => '4', :timestamp => time - 10,
          :emotion => 'ok')
        create_child(parent_10, :id => '5', :timestamp => time,
          :emotion => 'not_ok')

        assoc_ids = parent_class.intersect(:id => ['8', '9', '10']).
          associated_ids_for(:children)
        expect(assoc_ids).to eq('8'  => Zermelo::OrderedSet.new(['3', '4']),
                                '9'  => Zermelo::OrderedSet.new,
                                '10' => Zermelo::OrderedSet.new(['5']))
      end

      it "deletes a record from the set" do
        create_child(parent, :id => '3', :timestamp => time - 20,
          :emotion => 'ok')
        create_child(parent, :id => '4', :timestamp => time - 10,
          :emotion => 'ok')

        expect(parent.children.count).to eq(2)
        child = child_class.find_by_id('3')
        parent.children.remove(child)
        expect(parent.children.count).to eq(1)
        expect(parent.children.ids).to eq(Set.new(['4']))
      end

      it "deletes a record from the set by id" do
        create_child(parent, :id => '3', :timestamp => time - 20,
          :emotion => 'ok')
        create_child(parent, :id => '4', :timestamp => time - 10,
          :emotion => 'ok')

        expect(parent.children.count).to eq(2)
        parent.children.remove_ids('3')
        expect(parent.children.count).to eq(1)
        expect(parent.children.ids).to eq(Zermelo::OrderedSet.new(['4']))
      end

      it "clears all records from the set" do
        create_child(parent, :id => '3', :timestamp => time - 20,
          :emotion => 'ok')
        create_child(parent, :id => '4', :timestamp => time - 10,
          :emotion => 'ok')

        expect(parent.children.count).to eq(2)
        child = child_class.find_by_id('3')
        parent.children.clear
        expect(parent.children.count).to eq(0)
        expect(parent.children.ids).to eq(Zermelo::OrderedSet.new)
      end

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
          expect(upset_children).to be_a(Zermelo::OrderedSet)
          expect(upset_children.size).to eq(2)
          expect(upset_children.map(&:id)).to eq(['4', '6'])
        end

        it "by indexed attribute values with a regex search" do
          upset_children = parent.children.intersect(:emotion => /^ups/).all
          expect(upset_children).not_to be_nil
          expect(upset_children).to be_a(Zermelo::OrderedSet)
          expect(upset_children.size).to eq(2)
          expect(upset_children.map(&:id)).to eq(['4', '6'])
        end

        it "a subset of a sorted set by index" do
          range = Zermelo::Filters::IndexRange.new(0, 1)
          children = parent.children.intersect(:timestamp => range).all
          expect(children).not_to be_nil
          expect(children).to be_a(Zermelo::OrderedSet)
          expect(children.size).to eq(2)
          expect(children.map(&:id)).to eq(['4', '5'])
        end

        it "a reversed subset of a sorted set by index" do
          range = Zermelo::Filters::IndexRange.new(1, 2)
          children = parent.children.intersect(:timestamp => range).sort(:id, :desc => true).all
          expect(children).not_to be_nil
          expect(children).to be_a(Zermelo::OrderedSet)
          expect(children.size).to eq(2)
          expect(children.map(&:id)).to eq(['6', '5'])
        end

        it "a subset of a sorted set by score" do
          range = Zermelo::Filters::IndexRange.new(time - 25, time - 5, :by_score => true)
          children = parent.children.intersect(:timestamp => range).all
          expect(children).not_to be_nil
          expect(children).to be_a(Zermelo::OrderedSet)
          expect(children.size).to eq(2)
          expect(children.map(&:id)).to eq(['4', '5'])
        end

        it "a reversed subset of a sorted set by score" do
          range = Zermelo::Filters::IndexRange.new(time - 25, time - 5, :by_score => true)
          children = parent.children.intersect(:timestamp => range).
                       sort(:timestamp, :desc => true).all
          expect(children).not_to be_nil
          expect(children).to be_a(Zermelo::OrderedSet)
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

        it "ANDs multiple union arguments, not ORs them" do
          children = parent.children.intersect(:id => ['4']).
                       union(:emotion => 'upset', :id => ['4', '6']).all
          expect(children).not_to be_nil
          expect(children).to be_a(Zermelo::OrderedSet)
          expect(children.size).to eq(2)
          expect(children.map(&:id)).to eq(['4', '6'])
        end

        it "ANDs multiple diff arguments, not ORs them" do
          children = parent.children.diff(:emotion => 'upset', :id => ['4', '5']).all
          expect(children).not_to be_nil
          expect(children).to be_a(Zermelo::OrderedSet)
          expect(children.size).to eq(2)
          expect(children.map(&:id)).to eq(['5', '6'])
        end

        it "the exclusion of a sorted set by index" do
          range = Zermelo::Filters::IndexRange.new(0, 1)
          children = parent.children.diff(:timestamp => range).all
          expect(children).not_to be_nil
          expect(children).to be_a(Zermelo::OrderedSet)
          expect(children.size).to eq(1)
          expect(children.map(&:id)).to eq(['6'])
        end

        it "a reversed exclusion of a sorted set by index" do
          range = Zermelo::Filters::IndexRange.new(2, 2)
          children = parent.children.diff(:timestamp => range).sort(:id, :desc => true).all
          expect(children).not_to be_nil
          expect(children).to be_a(Zermelo::OrderedSet)
          expect(children.size).to eq(2)
          expect(children.map(&:id)).to eq(['5', '4'])
        end

        it "the exclusion of a sorted set by score" do
          range = Zermelo::Filters::IndexRange.new(time - 25, time - 5, :by_score => true)
          children = parent.children.diff(:timestamp => range).all
          expect(children).not_to be_nil
          expect(children).to be_a(Zermelo::OrderedSet)
          expect(children.size).to eq(1)
          expect(children.map(&:id)).to eq(['6'])
        end

        it "a reversed exclusion of a sorted set by score" do
          range = Zermelo::Filters::IndexRange.new(time - 5, time, :by_score => true)
          children = parent.children.diff(:timestamp => range).sort(:timestamp, :desc => true).all
          expect(children).not_to be_nil
          expect(children).to be_a(Zermelo::OrderedSet)
          expect(children.size).to eq(2)
          expect(children.map(&:id)).to eq(['5', '4'])
        end

      end

    end

    context 'influxdb', :influxdb => true, :has_sorted_set => true do

      let(:influxdb) { Zermelo.influxdb }

      module ZermeloExamples
        class AssociationsHasSortedSetParentInfluxDB
          include Zermelo::Records::InfluxDB
          has_sorted_set :children, :class_name => 'ZermeloExamples::AssociationsHasSortedSetChildInfluxDB',
            :inverse_of => :parent, :key => :timestamp
        end

        class AssociationsHasSortedSetChildInfluxDB
          include Zermelo::Records::InfluxDB
          define_attributes :emotion => :string,
                            :timestamp => :timestamp
          index_by :emotion
          range_index_by :timestamp
          belongs_to :parent, :class_name => 'ZermeloExamples::AssociationsHasSortedSetParentInfluxDB',
            :inverse_of => :children
        end
      end

      let(:parent_class) { ZermeloExamples::AssociationsHasSortedSetParentInfluxDB }
      let(:child_class) { ZermeloExamples::AssociationsHasSortedSetChildInfluxDB }

      # parent and child keys
      let(:pk) { 'associations_has_sorted_set_parent_influx_db' }
      let(:ck) { 'associations_has_sorted_set_child_influx_db' }

      # TODO
    end

  end

end