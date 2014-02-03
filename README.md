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

Let's build a simple producer that enqueues 4 messages followed by a consumer that properly processes one message, marks one message as failed, forgets to mark another message as either finished or failed, and returns a fourth message back to the queue:

```ruby
queue = RedisQueue.new :messages
queue.clear
queue.push "message 1"
queue.push "message 2"
queue.push "message 3"
queue.push "message 4"

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
  queue.unpop msg
  puts        msg
end

queue.print_stats
queue.print_contents
```

The output will be:
```
message 1
message 2
message 3
message 4
messages enqueued: 1
messages in use:   1
messages failed:   1
messages done:     1
messages enqueued: ["message 4"]
messages in use:   ["message 3"]
messages failed:   ["message 2"]
messages done:     ["message 1"]
```

## Copyright

Copyright (c) 2014. MIT license, José Ignacio Fernández <`joseignacio.fernandez@gmail.com`>