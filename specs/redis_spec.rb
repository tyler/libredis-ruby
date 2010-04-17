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
    end

    describe :connections do
      it 'returns an array of connection strings' do
        @redis.connections.should == ['127.0.0.1:6379']
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
