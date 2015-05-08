require 'spec_helper'
require 'zermelo/records/redis'
require 'zermelo/associations/has_one'

describe Zermelo::Associations::HasOne do

  context 'redis', :redis => true do

    before do
      skip "broken"
    end

    def create_example
    end

    def create_template(attrs = {})
      redis.hmset("template:#{attrs[:id]}:attrs", {'name' => attrs[:name]}.to_a.flatten)
      redis.sadd('template::attrs:ids', attrs[:id])
    end

    before(:each) do
      create_example(:id => '8', :name => 'John Jones',
                     :email => 'jjones@example.com', :active => true)
      create_template(:id => '2', :name => 'Template 1')
    end

    it "sets a has_and_belongs_to_many relationship between two records in redis" do
      example = Zermelo::RedisExample.find_by_id('8')
      template = Zermelo::Template.find_by_id('2')

      example.templates << template

      expect(redis.keys('*')).to match_array(['redis_example::attrs:ids',
                                 'redis_example::indices:by_name',
                                 'redis_example::indices:by_active:boolean:true',
                                 'redis_example:8:attrs',
                                 'redis_example:8:assocs:templates_ids',
                                 'template::attrs:ids',
                                 'template:2:attrs',
                                 'template:2:assocs:examples_ids'])

      expect(redis.smembers('redis_example::attrs:ids')).to eq(['8'])
      expect(redis.smembers('redis_example::indices:by_active:boolean:true')).to eq(['8'])
      expect(redis.hgetall('redis_example:8:attrs')).to eq(
        {'name' => 'John Jones', 'email' => 'jjones@example.com', 'active' => 'true'}
      )
      expect(redis.smembers('redis_example:8:assocs:templates_ids')).to eq(['2'])

      expect(redis.smembers('template::attrs:ids')).to eq(['2'])
      expect(redis.hgetall('template:2:attrs')).to eq({'name' => 'Template 1'})
      expect(redis.smembers('template:2:assocs:examples_ids')).to eq(['8'])
    end

    it "loads a record from a has_and_belongs_to_many relationship" do
      example = Zermelo::RedisExample.find_by_id('8')
      template = Zermelo::Template.find_by_id('2')

      template.examples << example

      templates = example.templates.all

      expect(templates).to be_an(Array)
      expect(templates.size).to eq(1)
      other_template = templates.first
      expect(other_template).to be_a(Zermelo::Template)
      expect(other_template.id).to eq(template.id)
    end

    it "removes a has_and_belongs_to_many relationship between two records in redis" do
      example = Zermelo::RedisExample.find_by_id('8')
      template = Zermelo::Template.find_by_id('2')

      template.examples << example

      expect(redis.smembers('template::attrs:ids')).to eq(['2'])
      expect(redis.smembers('redis_example:8:assocs:templates_ids')).to eq(['2'])

      example.templates.delete(template)

      expect(redis.smembers('template::attrs:ids')).to eq(['2'])        # template not deleted
      expect(redis.smembers('redis_example:8:assocs:templates_ids')).to eq([]) # but association is
    end

    it "filters has_and_belongs_to_many records by indexed attribute values" do
      create_example(:id => '9', :name => 'James Smith',
                     :email => 'jsmith@example.com', :active => false)
      create_example(:id => '10', :name => 'Alpha Beta',
                     :email => 'abc@example.com', :active => true)

      example = Zermelo::RedisExample.find_by_id('8')
      example_2 = Zermelo::RedisExample.find_by_id('9')
      example_3 = Zermelo::RedisExample.find_by_id('10')
      template = Zermelo::Template.find_by_id('2')

      example.templates << template
      example_2.templates << template
      example_3.templates << template

      examples = template.examples.intersect(:active => true).all
      expect(examples).not_to be_nil
      expect(examples).to be_an(Array)
      expect(examples.size).to eq(2)
      expect(examples.map(&:id)).to match_array(['8', '10'])
    end

    it "checks whether a record id exists through a has_and_belongs_to_many filter"  do
      create_example(:id => '9', :name => 'James Smith',
                     :email => 'jsmith@example.com', :active => false)

      example = Zermelo::RedisExample.find_by_id('8')
      example_2 = Zermelo::RedisExample.find_by_id('9')
      template = Zermelo::Template.find_by_id('2')

      example.templates << template
      example_2.templates << template

      expect(template.examples.intersect(:active => false).exists?('9')).to be_truthy
      expect(template.examples.intersect(:active => false).exists?('8')).to be_falsey
    end

    it "finds a record through a has_and_belongs_to_many filter" do
      create_example(:id => '9', :name => 'James Smith',
                     :email => 'jsmith@example.com', :active => false)

      example = Zermelo::RedisExample.find_by_id('8')
      example_2 = Zermelo::RedisExample.find_by_id('9')
      template = Zermelo::Template.find_by_id('2')

      example.templates << template
      example_2.templates << template

      james = template.examples.intersect(:active => false).find_by_id('9')
      expect(james).not_to be_nil
      expect(james).to be_a(Zermelo::RedisExample)
      expect(james.id).to eq(example_2.id)
    end

    it 'clears a has_and_belongs_to_many association when a record is deleted'

    it 'returns associated ids for multiple parent ids' do
      create_example(:id => '9', :name => 'Jane Johnson',
                     :email => 'jjohnson@example.com', :active => 'true')
      example_9 = Zermelo::RedisExample.find_by_id('9')

      create_example(:id => '10', :name => 'Jim Smith',
                     :email => 'jsmith@example.com', :active => 'true')
      example_10 = Zermelo::RedisExample.find_by_id('10')

      create_template(:id => '3', :name => 'Template 3')
      create_template(:id => '4', :name => 'Template 4')

      template_2 = Zermelo::Template.find_by_id('2')
      template_3 = Zermelo::Template.find_by_id('3')
      template_4 = Zermelo::Template.find_by_id('4')

      example_9.templates.add(template_2)
      example_10.templates.add(template_3, template_4)

      assoc_ids = Zermelo::RedisExample.intersect(:id => ['8', '9', '10']).
        associated_ids_for(:templates)
      expect(assoc_ids).to eq('8'  => Set.new([]),
                              '9'  => Set.new(['2']),
                              '10' => Set.new(['3', '4']))
    end

  end

end