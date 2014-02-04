require 'redis'

class RedisConnection
  def initialize args
    @args = args
  end

  def run
    @redis ||= new_redis

    begin
      yield(@redis)
    rescue Redis::CannotConnectError, Redis::TimeoutError => e
      puts e.backtrace
      puts "Redis crashed, retrying"
      sleep 5
      @redis = new_redis
      retry
    end
  end

  def new_redis
    ::Redis.connect(@args)
  end
end