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
      it 'always returns true' do
        @redis.quit.should == true
      end
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

    describe :flush_all do
      it 'removes all keys' do
        @redis.incr('foo')
        @redis.dbsize.should == 1

        @redis.flush_all
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
  end
end
