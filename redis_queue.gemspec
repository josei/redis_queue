Gem::Specification.new do |s|
  s.name        = 'redis_queue'
  s.version     = '0.3.1'
  s.date        = '2014-02-05'
  s.summary     = "Redis Queue"
  s.description = "A redis-based queue"
  s.authors     = ["Jose Ignacio Fernandez"]
  s.email       = 'joseignacio.fernandez@gmail.com'
  s.files       = ["lib/redis_queue.rb", "lib/redis_connection.rb"]
  s.homepage    = 'http://github.com/josei/redis_queue'
  s.license     = 'MIT'
  s.add_runtime_dependency 'redis'
end