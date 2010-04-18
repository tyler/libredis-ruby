require File.join(File.dirname(__FILE__), '..', 'ext', 'redis')

describe 'Redis' do
  it 'exists' do
    defined?(Redis).should be_true
  end

  it 'accepts a connection string on initialize' do
    Redis.new('127.0.0.1:6379')
  end

  describe 'instance method' do
    before :each do
      @redis = Redis.new('127.0.0.1:6379')
      @redis.flush_all
    end

    describe :connections do
      it 'returns an array of connection strings' do
        @redis.connections.should == ['127.0.0.1:6379']
      end
    end

    describe :quit do
      it 'always returns true'
      #it 'always returns true' do
      #  @redis.quit.should == true
      #end

      it 'causes subsequent commands to asplode'
      #it 'causes subsequent commands to asplode' do
      #  @redis.quit
      #  @redis.dbsize
      #end
    end

    describe :auth do
      # need a better test env before we can test this
    end

    describe :exists? do
      it 'returns true if the key exists' do
        @redis.incr('foo')
        @redis.exists?('foo').should == true
      end

      it 'returns false if the key does not exist' do
        @redis.exists?('bar').should == false
      end
    end

    describe :del do
      it 'causes the key to stop existing' do
        @redis.incr('foo')
        @redis.del('foo')
        @redis.exists?('foo').should == false
      end
    end

    describe :type do
      it 'returns "none" for nonexistent keys' do
        @redis.type('foo').should == 'none'
      end

      it 'returns "string" for string keys' do
        @redis.set('foo','bar')
        @redis.type('foo').should == 'string'
      end
    end

    describe :random_key do
      it 'returns an empty string if no keys exist' do
        @redis.random_key.should == ''
      end

      it 'returns a key if a key exists' do
        @redis.incr('foo')
        @redis.random_key.should == 'foo'
      end
    end

    describe :rename do
      it 'transfers the value of one key to another' do
        @redis.set('foo', 'abc')
        @redis.rename('foo','bar').should == true
        @redis.exists?('foo').should == false
        @redis.get('bar').should == 'abc'
      end

      it 'destroys the target key if necessary' do
        @redis.set('foo', 'abc')
        @redis.set('bar', 'xyz')
        @redis.rename('foo','bar').should == true
        @redis.exists?('foo').should == false
        @redis.get('bar').should == 'abc'
      end
    end

    describe :renamenx do
      it 'transfers the value of one key to another' do
        @redis.set('foo', 'abc')
        @redis.renamenx('foo','bar').should == true
        @redis.exists?('foo').should == false
        @redis.get('bar').should == 'abc'
      end

      it 'blows up if the target key exists' do
        @redis.set('foo', 'abc')
        @redis.set('bar', 'xyz')
        @redis.renamenx('foo','bar').should == false
      end
    end

    describe :dbsize do
      it 'returns the number of keys in the db' do
        @redis.incr('foo')
        @redis.dbsize.should == 1

        @redis.incr('bar')
        @redis.dbsize.should == 2
      end
    end

    describe :expire do
      it 'marks a key for expiration a number of seconds from now' do
        @redis.set('foo','bar')
        @redis.expire('foo', 10)
        @redis.ttl('foo').should <= 10
      end
    end

    describe :expire_at do
      it 'marks a key for expiration at a specific time' do
        @redis.set('foo','bar')
        @redis.expire_at('foo', Time.now.to_i + 10)
        @redis.ttl('foo').should <= 10
      end
    end

    describe :ttl do
      it 'returns the number of seconds left before a key expires' do
        @redis.set('foo','bar')
        @redis.expire('foo', 10)
        @redis.ttl('foo').should <= 10
      end

      it 'returns -1 if the key does not exist' do
        @redis.ttl('foo').should == -1
      end

      it 'returns -1 if the key is not set to expire' do
        @redis.set('foo','bar')
        @redis.ttl('foo').should <= 10
      end
    end

    describe :select do
      it 'selects a given db' do
        @redis.select(0)
        @redis.set('foo','bar')
        @redis.select(1)
        @redis.exists?('foo').should == false
      end
    end

    describe :move do
      it 'moves a key to a different db' do
        @redis.select(0)
        @redis.set('foo', 'bar')
        @redis.move('foo', 1)
        @redis.exists?('foo').should == false
        @redis.select(1)
        @redis.exists?('foo').should == true
      end
    end

    describe :flush_db do
      it 'removes all keys from current db' do
        @redis.select(0)
        @redis.set('foo', 'bar')
        @redis.select(1)
        @redis.set('foo', 'bar')

        @redis.flush_db

        @redis.exists?('foo').should == false
        @redis.dbsize.should == 0

        @redis.select(0)

        @redis.exists?('foo').should == true
        @redis.dbsize.should == 1
      end
    end

    describe :flush_all do
      it 'removes all keys' do
        @redis.select(0)
        @redis.set('foo', 'bar')
        @redis.select(1)
        @redis.set('foo', 'bar')

        @redis.flush_all

        @redis.exists?('foo').should == false
        @redis.dbsize.should == 0

        @redis.select(0)

        @redis.exists?('foo').should == false
        @redis.dbsize.should == 0
      end
    end

    describe :set do
      it 'sets a key to a value' do
        @redis.set('my_key', 'my_value').should be_true
      end
    end

    describe :get do
      it 'retrieves the value of a key' do
        @redis.set('my_key', 'my_value').should be_true
        @redis.get('my_key').should == 'my_value'
      end
    end

    describe :incr do
      it 'increments the value of a key' do
        @redis.set('incr_test', '0')
        @redis.incr('incr_test').should == 1
        @redis.get('incr_test').should == '1'
      end
    end

    describe :decr do
      it 'decrements the value of a key' do
        @redis.set('decr_test', '3')
        @redis.decr('decr_test').should == 2
        @redis.get('decr_test').should == '2'
      end
    end

    describe :keys do
      it 'returns a space separated list of keys' do
        @redis.set('keys_a', '1')
        @redis.set('keys_b', '1')
        @redis.set('keys_c', '1')
        @redis.keys('keys_?').should == ['keys_a', 'keys_b', 'keys_c']
      end
    end

    describe :rpush do
      it 'adds a value to the tail of a list' do
        @redis.rpush('foo','a')
        @redis.rpush('foo','b')
        @redis.lindex('foo', -1).should == 'b'
      end
    end

    describe :lpush do
      it 'adds a value to the head of a list' do
        @redis.lpush('foo','a')
        @redis.lpush('foo','b')
        @redis.lindex('foo', 0).should == 'b'
      end
    end

    describe :llen do
      it 'returns the length of the list' do
        @redis.lpush('foo','a')
        @redis.llen('foo').should == 1
        @redis.lpush('foo','b')
        @redis.llen('foo').should == 2
      end
    end

    describe :lrange do
      it 'returns a subset of the values of a list'
    end

    describe :ltrim do
      it 'removes all but the specified range of elements from a list' do
        %w(a b c d).each do |x|
          @redis.lpush('foo', x)
        end
        @redis.ltrim('foo', 1, 2)
        @redis.llen('foo').should == 2
      end
    end

    describe :lindex do
      it 'retrieves a particular element from a list' do
        @redis.lpush('foo', 'a')
        @redis.lpush('foo', 'b')
        @redis.lindex('foo', 0).should == 'b'
      end
    end

    describe :lset do
      it 'sets a particular element of a list' do
        %w(a b c).each do |x|
          @redis.lpush('foo', x)
        end
        @redis.lset('foo', 1, 'd')
        @redis.lindex('foo', 1).should == 'd'
      end
    end

    describe :lrem do
      it 'removes a specified number of elements matching a value from a list' do
        %w(a b a).each do |x|
          @redis.lpush('foo', x)
        end
        @redis.lrem('foo', 2, 'a')
        @redis.llen('foo').should == 1
      end
    end

    describe :lpop do
      it 'removes the first element of a list' do
        @redis.lpush('foo', 'a')
        @redis.lpush('foo', 'b')
        @redis.lpop('foo').should == 'b'
        @redis.llen('foo').should == 1
      end
    end

    describe :rpop do
      it 'removes the last element of a list' do
        @redis.lpush('foo', 'a')
        @redis.lpush('foo', 'b')
        @redis.rpop('foo').should == 'a'
        @redis.llen('foo').should == 1
      end
    end

    describe :rpoplpush do
      it 'removes the last element of one list and pushes it onto the front of another' do
        @redis.lpush('foo', 'a')
        @redis.rpoplpush('foo', 'bar')
        @redis.llen('foo').should == 0
        @redis.llen('bar').should == 1
      end
    end
  end
end
