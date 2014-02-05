require_relative 'redis_connection'

class RedisQueue
  SCRIPTS       = {
    fail: """
      redis.call('sadd', ARGV[1]..'_failed', ARGV[2])
      redis.call('srem', ARGV[1]..'_in_use', ARGV[2])
    """,
    done: """
      redis.call('sadd', ARGV[1]..'_done', ARGV[2])
      redis.call('srem', ARGV[1]..'_in_use', ARGV[2])
    """,
    unpop: """
      redis.call('lpush', ARGV[1], ARGV[2])
      redis.call('srem', ARGV[1]..'_in_use', ARGV[2])
    """,
    init_from: """
      local vals = redis.call('smembers', ARGV[2])
      for i = 1, table.getn(vals) do
        redis.call('rpush', ARGV[1], vals[i])
      end"""
  }

  def initialize args={id: :messages, url: 'redis://localhost:6379/0'}
    @id             = args.delete(:id)
    @redis          = RedisConnection.new(args)
    @redis_blocking = RedisConnection.new(args)
    load_scripts
  end

  def pop
    task = @redis_blocking.run { |redis| redis.blpop(@id) }.last
    @redis.run { |redis| redis.sadd "#{@id}_in_use", task }
    task
  end

  def push task
    @redis.run { |redis| redis.rpush @id, task }
  end

  def fail task
    script :fail, @id, task
  end

  def done task
    script :done, @id, task
  end

  def unpop task
    script :unpop, @id, task
  end

  def reset
    init_from "#{@id}_in_use"
    @redis.run { |redis| redis.del "#{@id}_in_use" }
  end

  def restart
    init_from "#{@id}_done"
    @redis.run { |redis| redis.del "#{@id}_done" }
  end

  def init_from set
    script(:init_from, @id, set)
  end

  def size
    @redis.run { |redis| redis.llen @id }.to_i
  end

  def done_size
    @redis.run { |redis| redis.scard "#{@id}_done" }.to_i
  end

  def failed_size
    @redis.run { |redis| redis.scard "#{@id}_failed" }.to_i
  end

  def in_use_size
    @redis.run { |redis| redis.scard "#{@id}_in_use" }.to_i
  end

  def list
    @redis.run { |redis| redis.lrange @id, 0, -1 }
  end

  def done_list
    @redis.run { |redis| redis.smembers "#{@id}_done" }
  end

  def failed_list
    @redis.run { |redis| redis.smembers "#{@id}_failed" }
  end

  def in_use_list
    @redis.run { |redis| redis.smembers "#{@id}_in_use" }
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
    @redis.run do |redis|
      redis.del @id
      redis.del "#{@id}_in_use"
      redis.del "#{@id}_done"
      redis.del "#{@id}_failed"
    end
  end

  private

  def load_scripts
    @scripts = {}
    @redis.run do |redis|
      SCRIPTS.each do |name, code|
        @scripts[name] = redis.script(:load, code)
      end
    end
  end

  def script name, *args
    @redis.run { |redis| redis.evalsha @scripts[name], argv: args }
  end
end
