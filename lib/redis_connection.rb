require 'redis'

class RedisConnection
  def initialize url
    @url = url
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
    ::Redis.connect(url: @url)
  end
end