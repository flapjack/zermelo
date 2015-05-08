require 'spec_helper'
require 'zermelo/records/redis'

# NB: also covers associations.rb, which is mixed in to Zermelo::Record

describe Zermelo::Records::Redis, :redis => true do

  module Zermelo
    class RedisExample
      include Zermelo::Records::Redis

      define_attributes :name   => :string,
                        :email  => :string,
                        :active => :boolean

      validates :name, :presence => true

      has_many :children, :class_name => 'Zermelo::RedisExampleChild',
        :inverse_of => :example, :before_add => :fail_if_roger,
        :before_read => :pre_read, :after_read => :post_read

      # has_sorted_set :data, :class_name => 'Zermelo::RedisExampleDatum',
      #   :key => :timestamp, :inverse_of => :example

      has_and_belongs_to_many :templates, :class_name => 'Zermelo::Template',
        :inverse_of => :examples

      index_by :active
      unique_index_by :name

      def fail_if_roger(*childs)
        raise "Not adding child" if childs.any? {|c| 'Roger'.eql?(c.name) }
      end

      attr_accessor :read

      def pre_read
        @read ||= []
        @read << :pre
      end

      def post_read
        @read ||= []
        @read << :post
      end
    end

    class RedisExampleChild
      include Zermelo::Records::Redis

      define_attributes :name => :string,
                        :important => :boolean

      index_by :important

      belongs_to :example, :class_name => 'Zermelo::RedisExample', :inverse_of => :children

      validates :name, :presence => true
    end

    class RedisExampleDatum
      include Zermelo::Records::Redis

      define_attributes :timestamp => :timestamp,
                        :summary => :string,
                        :emotion => :string

      belongs_to :example, :class_name => 'Zermelo::RedisExample', :inverse_of => :data

      index_by :emotion

      validates :timestamp, :presence => true
    end

    class Template
      include Zermelo::Records::Redis

      define_attributes :name => :string

      has_and_belongs_to_many :examples, :class_name => 'Zermelo::RedisExample',
        :inverse_of => :templates

      validates :name, :presence => true
    end
  end

  let(:redis) { Zermelo.redis }

  def create_example(attrs = {})
    redis.hmset("redis_example:#{attrs[:id]}:attrs",
      {'name' => attrs[:name], 'email' => attrs[:email], 'active' => attrs[:active]}.to_a.flatten)
    redis.sadd("redis_example::indices:by_active:boolean:#{!!attrs[:active]}", attrs[:id])
    name = attrs[:name].gsub(/%/, '%%').gsub(/ /, '%20').gsub(/:/, '%3A')
    redis.hset('redis_example::indices:by_name', "string:#{name}", attrs[:id])
    redis.sadd('redis_example::attrs:ids', attrs[:id])
  end

  context 'sorting by multiple keys' do

    def create_template(attrs = {})
      redis.hmset("template:#{attrs[:id]}:attrs", {'name' => attrs[:name]}.to_a.flatten)
      redis.sadd('template::attrs:ids', attrs[:id])
    end

    before do
      create_template(:id => '1', :name => 'abc')
      create_template(:id => '2', :name => 'def')
      create_template(:id => '3', :name => 'abc')
      create_template(:id => '4', :name => 'def')
    end

    it 'sorts by multiple fields' do
      expect(Zermelo::Template.sort(:name => :asc, :id => :desc).map(&:id)).to eq(['3', '1', '4', '2'])
    end

  end

  context 'bad parameters' do

    let(:example) { Zermelo::RedisExample.find_by_id('8') }

    before(:each) do
      create_example(:id => '8', :name => 'John Jones',
                     :email => 'jjones@example.com', :active => true)
    end

    it 'raises an error when calling add on has_many without an argument' do
      expect {
        example.children.add
      }.to raise_error
    end

    it 'raises an error when calling delete on has_many without an argument' do
      expect {
        example.children.delete
      }.to raise_error
    end

    it 'raises an error when calling add on has_sorted_set without an argument' do
      skip "broken"

      expect {
        example.data.add
      }.to raise_error
    end

    it 'raises an error when calling delete on has_sorted_set without an argument' do
      skip "broken"

      expect {
        example.data.delete
      }.to raise_error
    end

    it 'raises an error when calling add on has_and_belongs_to_many without an argument' do
      expect {
        example.templates.add
      }.to raise_error
    end

    it 'raises an error when calling delete on has_and_belongs_to_many without an argument' do
      expect {
        example.templates.delete
      }.to raise_error
    end

    it 'raises an error when trying to filter on a non-indexed value' do
      expect {
        Zermelo::RedisExample.intersect(:email => 'jjones@example.com').all
      }.to raise_error
    end
  end

end
