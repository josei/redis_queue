# Redis queue

A lightweight Redis-based queue with message acknowledgement and support for multiple consumers and producers.

## Installation

`gem install redis_queue`

## Usage

A message lifecycle is as follows:

* Message is pushed to the queue
* Message is popped out of the queue and marked as in use so that no other consumer takes it
* Once the message is processed, the consumer acknowledges the message by marking it as finished or failed or by rolling it back to the queue.

Whenever a consumer dies, its messages won't be processed by another consumer. They are kept inside the queue (in a different Redis key). Calling `RedisQueue#reset` puts all messages in use back into the queue.

### Producer-consumer

Let's build a simple producer that enqueues some messages followed by a consumer that properly processes one message, marks one message as failed, forgets to mark another message as either finished or failed, returns a fourth, and repushes, unpushes and forgets other messages:

```ruby
queue = RedisQueue.new
queue.clear

p queue.pop(block: false) # Non-blocking pop, which will return nil when empty

queue.push "message 2"
queue.push "message 3"
queue.push "message 4"
queue.push "message 5"
queue.push "message 6"
queue.push "message 6.5"
queue.push "message 7"
queue.push "message 1", true # This gives priority to this message
queue.remove "message 6.5"

queue.pop.tap do |msg|
  queue.done  msg
  puts        msg
end

queue.pop.tap do |msg|
  queue.fail  msg
  puts        msg
end

queue.pop.tap do |msg|
  puts        msg
end

queue.pop.tap do |msg|
  queue.repush msg
  puts        msg
end

queue.pop.tap do |msg|
  queue.repush msg
  puts        msg
end

queue.pop.tap do |msg|
  queue.forget msg
  puts        msg
end

queue.pop.tap do |msg|
  queue.unpop msg
  puts        msg
end

queue.touch.tap do |msg|
  puts        msg
end

queue.print_stats
queue.print_contents
```

The output will be:
```
nil
message 1
message 2
message 3
message 4
message 5
message 6
message 7
message 7
messages enqueued: 3
messages in use:   1
messages failed:   1
messages done:     1
messages enqueued: ["message 4", "message 5", "message 7"]
messages in use:   ["message 3"]
messages failed:   ["message 2"]
messages done:     ["message 1"]
```

## Copyright

Copyright (c) 2014. MIT license, José Ignacio Fernández <`joseignacio.fernandez@gmail.com`>
