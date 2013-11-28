require 'spec_helper'
require 'sandstorm/lock'
require 'sandstorm/record'

describe Sandstorm::Lock, :redis => true do

  module Sandstorm
    class LockExample
      include Sandstorm::Record
      define_attributes :name => :string
    end

    class AnotherExample
      include Sandstorm::Record
      define_attributes :age => :integer
    end
  end

  let(:redis) { Sandstorm.redis }

  it "locks access to class-level data" do
    redis.keys.should be_empty

    slock = Sandstorm::Lock.new(Sandstorm::LockExample)
    locked = slock.lock
    locked.should be_true

    lock_keys = ["lock_example::lock:owner", "lock_example::lock:expiry"]
    redis.keys.should =~ lock_keys

    example = Sandstorm::LockExample.new(:name => 'temporary')
    example.save

    example_keys = ["lock_example::ids", "lock_example:#{example.id}:attrs"]
    redis.keys.should =~ lock_keys + example_keys

    unlocked = slock.unlock
    unlocked.should be_true
    redis.keys.should =~ example_keys
  end

  it "locks access to class-level data from two classes" do
    redis.keys.should be_empty

    slock = Sandstorm::Lock.new(Sandstorm::LockExample, Sandstorm::AnotherExample)
    locked = slock.lock
    locked.should be_true

    lock_keys = ["another_example::lock:owner", "another_example::lock:expiry",
      "lock_example::lock:owner", "lock_example::lock:expiry"]
    redis.keys.should =~ lock_keys

    lock_example = Sandstorm::LockExample.new(:name => 'temporary')
    lock_example.save

    another_example = Sandstorm::AnotherExample.new(:age => 36)
    another_example.save

    example_keys = ["lock_example::ids", "lock_example:#{lock_example.id}:attrs",
      "another_example::ids", "another_example:#{another_example.id}:attrs"]
    redis.keys.should =~ lock_keys + example_keys

    unlocked = slock.unlock
    unlocked.should be_true
    redis.keys.should =~ example_keys
  end

  it "extends an existing lock", :time => true do
    slock = Sandstorm::Lock.new(Sandstorm::LockExample)
    slock.life = 60

    time = Time.local(2012, 1, 1, 12, 0, 0)
    Timecop.freeze(time)

    locked = slock.lock

    expiry_time = redis.get("lock_example::lock:expiry")
    expiry_time.to_i.should == time.to_i + 60

    Timecop.travel(time + 45)

    extended = slock.extend_life(30)
    extended.should be_true

    expiry_time = redis.get("lock_example::lock:expiry")
    expiry_time.to_i.should == time.to_i + 75

    unlocked = slock.unlock
    unlocked.should be_true
  end

  it "expires a lock when its lifetime has expired"

  it "stops another thread from accessing a lock on a single class while held" do
    monitor = Monitor.new

    times = {}

    slock = Sandstorm::Lock.new(Sandstorm::LockExample)
    locked = slock.lock
    locked.should be_true

    # ensure Redis connection is instantiated
    redis

    t = Thread.new do
      Sandstorm.redis = redis # thread-local

      slock.lock
      locked.should be_true

      monitor.synchronize do
        times['thread'] = Time.now
      end

      unlocked = slock.unlock
      unlocked.should be_true
    end

    monitor.synchronize do
      times['main'] = Time.now
    end

    sleep 0.25
    slock.unlock

    t.join

    (times['thread'] - times['main']).should be >= 0.25
  end

  it "stops another thread from accessing a lock on multiple classes while held" do
    monitor = Monitor.new

    times = {}

    slock = Sandstorm::Lock.new(Sandstorm::LockExample, Sandstorm::AnotherExample)
    locked = slock.lock
    locked.should be_true

    # ensure Redis connection is instantiated
    redis

    t = Thread.new do
      Sandstorm.redis = redis # thread-local

      slock.lock
      locked.should be_true

      monitor.synchronize do
        times['thread'] = Time.now
      end

      unlocked = slock.unlock
      unlocked.should be_true
    end

    monitor.synchronize do
      times['main'] = Time.now
    end

    sleep 0.25
    slock.unlock

    t.join

    (times['thread'] - times['main']).should be >= 0.25
  end

end
