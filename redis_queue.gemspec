Gem::Specification.new do |s|
  s.name        = 'redis_queue'
  s.version     = '0.6.0'
  s.date        = '2014-07-16'
  s.summary     = 'Redis Queue'
  s.description = 'A redis-based queue'
  s.authors     = ['Jose Ignacio Fernandez']
  s.email       = 'joseignacio.fernandez@gmail.com'
  s.files       = ['lib/redis_queue.rb', 'lib/redis_connection.rb']
  s.homepage    = 'http://github.com/josei/redis_queue'
  s.license     = 'MIT'
  s.add_runtime_dependency 'redis', '~> 4.0', '>= 4.0.1'
end
