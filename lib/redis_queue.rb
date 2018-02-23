class RedisQueue
  PUSH_CODE = ''"
    if ARGV[3] == 'true' then
      local insert = redis.call('linsert', ARGV[1], 'before', '', ARGV[2])
      if insert == -1 or insert == 0 then
        redis.call('lpush', ARGV[1], '')
        redis.call('lpush', ARGV[1], ARGV[2])
      end
    else
      redis.call('rpush', ARGV[1], ARGV[2])
    end"''.freeze

  SCRIPTS   = {
    push: PUSH_CODE,
    nonblpop: ''"
      local message = ''
      while message == '' do
        message = redis.call('lpop', ARGV[1])
      end
      if message then
        redis.call('sadd', ARGV[1]..'_in_use', message)
      end
      return message
    "'',
    touch: ''"
      local message = ''
      while message == '' do
        message = redis.call('lpop', ARGV[1])
      end
      if message then
        redis.call('rpush', ARGV[1], message)
      end
      return message
    "'',
    repush: ''"
      #{PUSH_CODE}
      redis.call('srem', ARGV[1]..'_in_use', ARGV[2])
    "'',
    fail: ''"
      redis.call('sadd', ARGV[1]..'_failed', ARGV[2])
      redis.call('srem', ARGV[1]..'_in_use', ARGV[2])
    "'',
    done: ''"
      redis.call('sadd', ARGV[1]..'_done', ARGV[2])
      redis.call('srem', ARGV[1]..'_in_use', ARGV[2])
    "'',
    unpop: ''"
      redis.call('lpush', ARGV[1], ARGV[2])
      redis.call('srem', ARGV[1]..'_in_use', ARGV[2])
    "'',
    init_from: ''"
      local vals = redis.call('smembers', ARGV[2])
      for i = 1, table.getn(vals) do
        redis.call('lpush', ARGV[1], vals[i])
      end"''
  }.freeze

  def initialize(args = {})
    args = { id: :messages, url: 'redis://localhost:6379/0' }.merge(args)
    @id             = args.delete(:id)
    @redis          = RedisConnection.new(args)
    @redis_blocking = RedisConnection.new(args)
    load_scripts
  end

  def pop(block: true)
    return nonblpop unless block
    message = blpop
    @redis.run { |redis| redis.sadd "#{@id}_in_use", message } if message
    message
  end

  def push(message, priority = false)
    script :push, @id, message, priority
  end

  def fail(message)
    script :fail, @id, message
  end

  def done(message)
    script :done, @id, message
  end

  def unpop(message)
    script :unpop, @id, message
  end

  def repush(message, priority = false)
    script :repush, @id, message, priority
  end

  def forget(message)
    @redis.run { |redis| redis.srem "#{@id}_in_use", message }
  end

  def remove(message)
    @redis.run { |redis| redis.lrem @id, 0, message }
  end

  def touch(block: true)
    return nonbltouch unless block
    message = blpop
    push(message)
    message
  end

  def reset
    init_from "#{@id}_in_use"
    @redis.run { |redis| redis.del "#{@id}_in_use" }
  end

  def restart
    init_from "#{@id}_done"
    @redis.run { |redis| redis.del "#{@id}_done" }
  end

  def init_from(set)
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

  def blpop
    begin
      message = @redis_blocking.run { |redis| redis.blpop(@id) }.last
    end while message == ''
    message
  end

  def nonblpop
    script :nonblpop, @id
  end

  def nonbltouch
    script :touch, @id
  end

  def load_scripts
    @scripts = {}
    @redis.run do |redis|
      SCRIPTS.each do |name, code|
        @scripts[name] = redis.script(:load, code)
      end
    end
  end

  def script(name, *args)
    @redis.run { |redis| redis.evalsha @scripts[name], argv: args }
  end
end

require_relative 'redis_connection'
