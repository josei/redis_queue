require_relative 'redis_connection'

class RedisQueue
  def initialize id=:messages, url='redis://localhost:6379/0'
    @id = id
    @redis_connection = RedisConnection.new(url)
  end

  def pop
    @redis_connection.run do |redis|
      redis.blpop(@id).last.tap { |msg| redis.sadd "#{@id}_in_use", msg }
    end
  end

  def push task
    @redis_connection.run { |redis| redis.rpush @id, task }
  end

  def fail task
    @redis_connection.run do |redis|
      redis.pipelined do
        redis.sadd "#{@id}_failed", task
        redis.srem "#{@id}_in_use", task
      end
    end
  end

  def done task
    @redis_connection.run do |redis|
      redis.pipelined do
        redis.sadd "#{@id}_done", task
        redis.srem "#{@id}_in_use", task
      end
    end
  end

  def unpop task
    @redis_connection.run do |redis|
      redis.pipelined do
        redis.lpush @id, task
        redis.srem "#{@id}_in_use", task
      end
    end
  end

  def reset
    init_from "#{@id}_in_use"
    @redis_connection.run { |redis| redis.del "#{@id}_in_use" }
  end

  def restart
    init_from "#{@id}_done"
    @redis_connection.run { |redis| redis.del "#{@id}_done" }
  end

  def init_from set
    @redis_connection.run do |redis|
      redis.eval "local vals = redis.call('smembers', '#{set}')
      for i = 1, table.getn(vals) do
        redis.call('rpush', '#{@id}', vals[i])
      end"
    end
  end

  def size
    @redis_connection.run { |redis| redis.llen @id }.to_i
  end

  def done_size
    @redis_connection.run { |redis| redis.scard "#{@id}_done" }.to_i
  end

  def failed_size
    @redis_connection.run { |redis| redis.scard "#{@id}_failed" }.to_i
  end

  def in_use_size
    @redis_connection.run { |redis| redis.scard "#{@id}_in_use" }.to_i
  end

  def list
    @redis_connection.run { |redis| redis.lrange @id, 0, -1 }
  end

  def done_list
    @redis_connection.run { |redis| redis.smembers "#{@id}_done" }
  end

  def failed_list
    @redis_connection.run { |redis| redis.smembers "#{@id}_failed" }
  end

  def in_use_list
    @redis_connection.run { |redis| redis.smembers "#{@id}_in_use" }
  end

  def print_stats
    puts "#{@id} enqueued: #{size}"
    puts "#{@id} in use:   #{in_use_size}"
    puts "#{@id} failed:   #{failed_size}"
    puts "#{@id} done:     #{done_size}"
  end

  def print_contents
    puts "#{@id} enqueued: #{list}"
    puts "#{@id} in use:   #{in_use_list}"
    puts "#{@id} failed:   #{failed_list}"
    puts "#{@id} done:     #{done_list}"
  end

  def clear
    @redis_connection.run do |redis|
      redis.del @id
      redis.del "#{@id}_in_use"
      redis.del "#{@id}_done"
      redis.del "#{@id}_failed"
    end
  end
end
