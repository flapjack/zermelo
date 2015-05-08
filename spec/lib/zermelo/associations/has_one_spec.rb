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

    # class Zermelo::RedisExampleSpecial
    #   include Zermelo::Records::RedisRecord

    #   define_attributes :name => :string

    #   belongs_to :example, :class_name => 'Zermelo::RedisExample', :inverse_of => :special

    #   validates :name, :presence => true
    # end

    # class Zermelo::RedisExample
    #   has_one :special, :class_name => 'Zermelo::RedisExampleSpecial', :inverse_of => :example,
    #     :before_read => :pre_special_read, :after_read => :post_special_read

    #   attr_accessor :special_read

    #   def pre_special_read
    #     @read ||= []
    #     @read << :pre
    #   end

    #   def post_special_read(value)
    #     @read ||= []
    #     @read << :post
    #   end
    # end

    it "sets and retrieves a record via a has_one association" do
      create_example(:id => '8', :name => 'John Jones',
                     :email => 'jjones@example.com', :active => 'true')

      special = Zermelo::RedisExampleSpecial.new(:id => '22', :name => 'Bill Smith')
      expect(special.save).to be_truthy

      example = Zermelo::RedisExample.find_by_id('8')
      example.special = special

      expect(redis.keys('*')).to match_array(['redis_example::attrs:ids',
                                 'redis_example::indices:by_name',
                                 'redis_example::indices:by_active:boolean:true',
                                 'redis_example:8:attrs',
                                 'redis_example:8:assocs',
                                 'redis_example_special::attrs:ids',
                                 'redis_example_special:22:attrs',
                                 'redis_example_special:22:assocs:belongs_to'])

      expect(redis.hgetall('redis_example:8:assocs')).to eq("special_id" => "22")

      expect(redis.smembers('redis_example_special::attrs:ids')).to eq(['22'])
      expect(redis.hgetall('redis_example_special:22:attrs')).to eq(
        {'name' => 'Bill Smith'}
      )

      expect(redis.hgetall('redis_example_special:22:assocs:belongs_to')).to eq(
        {'example_id' => '8'}
      )

      example2 = Zermelo::RedisExample.find_by_id('8')
      special2 = example2.special
      expect(special2).not_to be_nil

      expect(special2.id).to eq('22')
      expect(special2.example.id).to eq('8')
    end

    it 'calls the before/after_read callbacks when the value is read' do
      create_example(:id => '8', :name => 'John Jones',
                     :email => 'jjones@example.com', :active => 'true')
      example = Zermelo::RedisExample.find_by_id('8')

      expect(example.read).to be_nil
      expect(example.special).to be_nil
      expect(example.read).to eq([:pre, :post])
    end

    def create_special(parent, attrs = {})
      redis.hmset("redis_example_special:#{attrs[:id]}:attrs", {'name' => attrs[:name]}.to_a.flatten)

      redis.hset("redis_example_special:#{attrs[:id]}:assocs:belongs_to", 'example_id', parent.id)
      redis.hset("redis_example:#{parent.id}:assocs", 'special_id', attrs[:id])

      redis.sadd('redis_example_special::attrs:ids', attrs[:id])
    end

    it 'clears the belongs_to association when the child record is deleted' do
      create_example(:id => '8', :name => 'John Jones',
                     :email => 'jjones@example.com', :active => 'true')
      example = Zermelo::RedisExample.find_by_id('8')
      create_special(example, :id => '3', :name => 'Another Jones')
      special = Zermelo::RedisExampleSpecial.find_by_id('3')

      expect(redis.keys).to match_array(['redis_example::attrs:ids',
                            'redis_example::indices:by_name',
                            'redis_example::indices:by_active:boolean:true',
                            'redis_example:8:attrs',
                            'redis_example:8:assocs',
                            'redis_example_special::attrs:ids',
                            'redis_example_special:3:attrs',
                            'redis_example_special:3:assocs:belongs_to'])

      special.destroy

      expect(redis.keys).to match_array(['redis_example::attrs:ids',
                            'redis_example::indices:by_name',
                            'redis_example::indices:by_active:boolean:true',
                            'redis_example:8:attrs'])
    end

    it "clears the belongs_to association when the parent record is deleted" do
      create_example(:id => '8', :name => 'John Jones',
                     :email => 'jjones@example.com', :active => 'true')
      example = Zermelo::RedisExample.find_by_id('8')
      create_special(example, :id => '3', :name => 'Another Jones')

      expect(redis.keys).to match_array(['redis_example::attrs:ids',
                            'redis_example::indices:by_name',
                            'redis_example::indices:by_active:boolean:true',
                            'redis_example:8:attrs',
                            'redis_example:8:assocs',
                            'redis_example_special::attrs:ids',
                            'redis_example_special:3:attrs',
                            'redis_example_special:3:assocs:belongs_to'])

      example.destroy

      expect(redis.keys).to match_array(['redis_example_special::attrs:ids',
                            'redis_example_special:3:attrs'])
    end

    it 'returns associated ids for multiple parent ids' do
      create_example(:id => '8', :name => 'John Jones',
                     :email => 'jjones@example.com', :active => 'true')

      create_example(:id => '9', :name => 'Jane Johnson',
                     :email => 'jjohnson@example.com', :active => 'true')
      example_9 = Zermelo::RedisExample.find_by_id('9')

      create_example(:id => '10', :name => 'Jim Smith',
                     :email => 'jsmith@example.com', :active => 'true')
      example_10 = Zermelo::RedisExample.find_by_id('10')

      time = Time.now.to_i

      create_special(example_9,  :id => '3', :name => 'jkl')
      create_special(example_10, :id => '4', :name => 'pqr')

      assoc_ids = Zermelo::RedisExample.intersect(:id => ['8', '9', '10']).
        associated_ids_for(:special)
      expect(assoc_ids).to eq('8'  => nil,
                              '9'  => '3',
                              '10' => '4')
    end

  end

end