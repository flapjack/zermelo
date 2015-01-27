
module Zermelo

  class LockNotAcquired < StandardError; end

  module Locks

    class RedisLock

      # Adapted from https://github.com/mlanett/redis-lock,
      # now covers locking multiple keys at once

      attr_accessor :expires_at, :life, :sleep_in_ms

      def initialize
        @owner_value = Thread.current.object_id
        @life        = 60
        @timeout     = 10
        @sleep_in_ms = 125
      end

      def lock(*record_klasses, &block)
        @keys = record_klasses.map{|k| k.send(:class_key) }.sort.map{|k| "#{k}::lock" }
        do_lock_with_timeout(@timeout) or raise Zermelo::LockNotAcquired.new(@keys.join(", "))
        result = true
        if block
          begin
            result = (block.arity == 1) ? block.call(self) : block.call
          # rescue Exception => e
          #   puts e.message
          #   puts e.backtrace.join("\n")
          #   raise e
          ensure
            release_lock
          end
        end
        result
      end

      def extend_life( new_life )
        do_extend( new_life ) or raise Zermelo::LockNotAcquired.new(@keys.join(", "))
      end

      def unlock
        release_lock
      end

      private

      def full_keys
        @full_keys ||= @keys.map {|k| ["#{k}:owner", "#{k}:expiry"] }.flatten
      end

      def owner_keys
        @owner_keys ||= @keys.map {|k| "#{k}:owner" }
      end

      def expiry_keys
        @expiry_keys ||= @keys.map {|k| "#{k}:expiry" }
      end

      def do_lock_with_timeout( timeout )
        locked = false
        with_timeout(timeout) { locked = do_lock }
        locked
      end

      # @returns true if locked, false otherwise
      def do_lock( tries = 3 )
        # We need to set both owner and expire at the same time
        # If the existing lock is stale, we delete it and try again once

        locked = nil

        loop do
          new_xval = Time.now.to_i + @life

          lock_keyvals = @keys.map {|k| ["#{k}:owner",  @owner_value,
                                         "#{k}:expiry", new_xval] }.flatten

          result = Zermelo.redis.msetnx(*lock_keyvals)

          if [1, true].include?(result)
            # log :debug, "do_lock() success"
            @expires_at = new_xval
            locked = true
            break
          else
            # log :debug, "do_lock() failed"
            # consider the possibility that this lock is stale
            tries -= 1
            next if tries > 0 && stale_key?
            locked = false
            break
          end
        end
        locked
      end

      def do_extend( new_life )
        # We use watch and a transaction to ensure we only change a lock we own
        # The transaction fails if the watched variable changed
        # Use my_owner = oval to make testing easier.
        new_xval = Time.now.to_i + new_life
        extended = false
        with_watch( *owner_keys  ) do
          owners = Zermelo.redis.mget( *owner_keys )
          if owners == ([@owner_value.to_s] * owner_keys.size)
            result = Zermelo.redis.multi do |multi|
              multi.mset( *(expiry_keys.zip( [new_xval] * expiry_keys.size)) )
            end
            if result == ['OK']
              # log :debug, "do_extend() success"
              @expires_at = new_xval
              extended = true
            end
          end
        end
        extended
      end

      # Only actually deletes it if we own it.
      # There may be strange cases where we fail to delete it, in which case expiration will solve the problem.
      def release_lock
        released = false
        with_watch( *full_keys ) do
          owners = Zermelo.redis.mget( *owner_keys )
          if owners == ([@owner_value.to_s] * owner_keys.size)
            result = Zermelo.redis.multi do |multi|
              multi.del(*full_keys)
            end
            if result && (result.size == 1) && (result.first == full_keys.size)
              released = true
            end
          end
        end
        released
      end

      def stale_key?
        # Check if expiration exists and is it stale?
        # If so, delete it.
        # watch() all keys so we can detect if they change while we do this
        # multi() will fail if keys have changed after watch()
        # Thus, we snapshot consistency at the time of watch()
        # Note: inside a watch() we get one and only one multi()
        now = Time.now.to_i
        stale = false
        with_watch( *full_keys ) do

          owners_expires = Zermelo.redis.mget(full_keys)

          if owners_expires.each_slice(2).all? {|owner, expire| is_deletable?( owner, expire, now)}
            result = Zermelo.redis.multi do |multi|
              multi.del(*full_keys)
            end
            # If anything changed then multi() fails and returns nil
            if result && (result.size == 1) && (result.first == owner_keys.size)
              # log :info, "Deleted stale key from #{owner}"
              stale = true
            end
          end
        end # watch
        stale
      end

      def locked?
        now = Time.now.to_i
        owners_expires = Zermelo.redis.mget(full_keys)
        owners_expires && (owners_expires.size == (@keys.size * 2)) &&
          owners_expires.each_slice(2).all? {|owner, expiration| is_locked?(owner, expiration, now)}
      end

      # returns true if the lock exists and is owned by the given owner
      def is_locked?(owner, expiration, now)
        (owner == @owner_value) && ! is_deletable?(owner, expiration, now)
      end

      # returns true if this is a broken or expired lock
      def is_deletable?( owner, expiration, now)
        expiration = expiration.to_i
        (owner || (expiration > 0)) && (!owner || (expiration < now))
      end

      def with_watch( *args, &block )
        Zermelo.redis.watch( *args )
        begin
          block.call
        ensure
          Zermelo.redis.unwatch
        end
      end

      # Calls block until it returns true or times out.
      # @param block should return true if successful, false otherwise
      # @returns true if successful, false otherwise
      def with_timeout( timeout, &block )
        expire = Time.now + timeout.to_f
        sleepy = @sleep_in_ms / 1000.to_f()
        # this looks inelegant compared to while Time.now < expire, but does not oversleep
        ret = nil
        loop do
          if block.call
            ret = true
            break
          end
          if (Time.now + sleepy) > expire
            ret = false
            break
          end
          sleep(sleepy)
          # might like a different strategy, but general goal is not use 100% cpu while contending for a lock.
        end
        ret
      end

    end

  end

end