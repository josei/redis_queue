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
        redis.call('hset', ARGV[1]..'_in_use', message, ARGV[2])
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
      redis.call('hdel', ARGV[1]..'_in_use', ARGV[2])
    "'',
    fail: ''"
      redis.call('hset', ARGV[1]..'_failed', ARGV[2], ARGV[3])
      redis.call('hdel', ARGV[1]..'_in_use', ARGV[2])
    "'',
    done: ''"
      redis.call('hset', ARGV[1]..'_done', ARGV[2], ARGV[3])
      redis.call('hdel', ARGV[1]..'_in_use', ARGV[2])
    "'',
    unpop: ''"
      redis.call('lpush', ARGV[1], ARGV[2])
      redis.call('hdel', ARGV[1]..'_in_use', ARGV[2])
    "'',
    init_from: ''"
      local vals = redis.call('hkeys', ARGV[2])
      for i = 1, table.getn(vals) do
        local timestamp = redis.call('hget', ARGV[2], vals[i])
        if timestamp < ARGV[3] then
          redis.call('lpush', ARGV[1], vals[i])
          redis.call('hdel', ARGV[2], vals[i])
        end
      end
      "''
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
    @redis.run { |redis| redis.hset "#{@id}_in_use", message, now } if message
    message
  end

  def push(message, priority = false)
    script :push, @id, message, priority
  end

  def fail(message)
    script :fail, @id, message, now
  end

  def done(message)
    script :done, @id, message, now
  end

  def unpop(message)
    script :unpop, @id, message
  end

  def repush(message, priority = false)
    script :repush, @id, message, priority
  end

  def forget(message)
    @redis.run { |redis| redis.hdel "#{@id}_in_use", message }
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

  def reset(older_than: nil)
    init_from "#{@id}_in_use", older_than
  end

  def restart
    init_from "#{@id}_done"
  end

  def size
    @redis.run { |redis| redis.llen @id }.to_i
  end

  def done_size
    @redis.run { |redis| redis.hlen "#{@id}_done" }.to_i
  end

  def failed_size
    @redis.run { |redis| redis.hlen "#{@id}_failed" }.to_i
  end

  def in_use_size
    @redis.run { |redis| redis.hlen "#{@id}_in_use" }.to_i
  end

  def list
    @redis.run { |redis| redis.lrange @id, 0, -1 }
  end

  def done_list
    Hash[@redis.run { |redis| redis.hgetall "#{@id}_done" }]
  end

  def failed_list
    Hash[@redis.run { |redis| redis.hgetall "#{@id}_failed" }]
  end

  def in_use_list
    Hash[@redis.run { |redis| redis.hgetall "#{@id}_in_use" }]
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
    loop do
      message = @redis_blocking.run { |redis| redis.blpop(@id) }.last
      return message unless message == ''
    end
  end

  def nonblpop
    script :nonblpop, @id, now
  end

  def nonbltouch
    script :touch, @id
  end

  def init_from(key, older_than = nil)
    script(:init_from, @id, key, older_than || now + 100_000)
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

  def now
    (Time.now.to_f * 1000).to_i
  end
end

require_relative 'redis_connection'
