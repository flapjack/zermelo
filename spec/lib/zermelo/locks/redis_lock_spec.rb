require 'spec_helper'
require 'zermelo/locks/redis_lock'
require 'zermelo/records/redis'

describe Zermelo::Locks::RedisLock, :redis => true do

  let(:redis) { Zermelo.redis }

  module Zermelo
    class RedisLockExample
      include Zermelo::Records::RedisSet
      define_attributes :name => :string
    end

    class AnotherExample
      include Zermelo::Records::RedisSet
      define_attributes :age => :integer
    end
  end

  it "locks access to class-level data" do
    expect(redis.keys('*')).to be_empty

    slock = Zermelo::Locks::RedisLock.new
    locked = slock.lock(Zermelo::RedisLockExample)
    expect(locked).to be_truthy

    lock_keys = ["redis_lock_example::lock:owner", "redis_lock_example::lock:expiry"]
    expect(redis.keys('*')).to match_array(lock_keys)

    example = Zermelo::RedisLockExample.new(:name => 'temporary')
    example.save

    example_keys = ["redis_lock_example::attrs:ids", "redis_lock_example:#{example.id}:attrs"]
    expect(redis.keys('*')).to match_array(lock_keys + example_keys)

    unlocked = slock.unlock
    expect(unlocked).to be_truthy
    expect(redis.keys('*')).to match_array(example_keys)
  end

  it "locks access to class-level data from two classes" do
    expect(redis.keys('*')).to be_empty

    slock = Zermelo::Locks::RedisLock.new
    locked = slock.lock(Zermelo::RedisLockExample, Zermelo::AnotherExample)
    expect(locked).to be_truthy

    lock_keys = ["another_example::lock:owner", "another_example::lock:expiry",
      "redis_lock_example::lock:owner", "redis_lock_example::lock:expiry"]
    expect(redis.keys('*')).to match_array(lock_keys)

    redis_lock_example = Zermelo::RedisLockExample.new(:name => 'temporary')
    redis_lock_example.save

    another_example = Zermelo::AnotherExample.new(:age => 36)
    another_example.save

    example_keys = ["redis_lock_example::attrs:ids", "redis_lock_example:#{redis_lock_example.id}:attrs",
      "another_example::attrs:ids", "another_example:#{another_example.id}:attrs"]
    expect(redis.keys('*')).to match_array(lock_keys + example_keys)

    unlocked = slock.unlock
    expect(unlocked).to be_truthy
    expect(redis.keys('*')).to match_array(example_keys)
  end

  it "extends an existing lock", :time => true do
    slock = Zermelo::Locks::RedisLock.new
    slock.life = 60

    time = Time.local(2012, 1, 1, 12, 0, 0)
    Timecop.freeze(time)

    locked = slock.lock(Zermelo::RedisLockExample)

    expiry_time = redis.get("redis_lock_example::lock:expiry")
    expect(expiry_time.to_i).to eq(time.to_i + 60)

    Timecop.travel(time + 45)

    extended = slock.extend_life(30)
    expect(extended).to be_truthy

    expiry_time = redis.get("redis_lock_example::lock:expiry")
    expect(expiry_time.to_i).to eq(time.to_i + 75)

    unlocked = slock.unlock
    expect(unlocked).to be_truthy
  end

  it "expires a lock when its lifetime has expired"

  it "stops another thread from accessing a lock on a single class while held" do
    monitor = Monitor.new

    times = {}

    slock = Zermelo::Locks::RedisLock.new
    locked = slock.lock(Zermelo::RedisLockExample)
    expect(locked).to be_truthy

    # ensure Redis connection is instantiated
    redis

    t = Thread.new do
      Zermelo.redis = redis # thread-local

      slock.lock(Zermelo::RedisLockExample)
      expect(locked).to be_truthy

      monitor.synchronize do
        times['thread'] = Time.now
      end

      unlocked = slock.unlock
      expect(unlocked).to be_truthy
    end

    monitor.synchronize do
      times['main'] = Time.now
    end

    sleep 0.25
    slock.unlock

    t.join

    expect(times['thread'] - times['main']).to be >= 0.25
  end

  it "stops another thread from accessing a lock on multiple classes while held" do
    monitor = Monitor.new

    times = {}

    slock = Zermelo::Locks::RedisLock.new
    locked = slock.lock(Zermelo::RedisLockExample, Zermelo::AnotherExample)
    expect(locked).to be_truthy

    # ensure Redis connection is instantiated
    redis

    t = Thread.new do
      Zermelo.redis = redis # thread-local

      slock.lock(Zermelo::RedisLockExample)
      expect(locked).to be_truthy

      monitor.synchronize do
        times['thread'] = Time.now
      end

      unlocked = slock.unlock
      expect(unlocked).to be_truthy
    end

    monitor.synchronize do
      times['main'] = Time.now
    end

    sleep 0.25
    slock.unlock

    t.join

    expect(times['thread'] - times['main']).to be >= 0.25
  end

end
