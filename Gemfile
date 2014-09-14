source 'https://rubygems.org'

gemspec :name => 'sandstorm'

group :test do
  gem 'influxdb'
  gem 'redis'
  gem 'rspec', '>= 3.0.0'
  gem 'simplecov', :require => false

  # force Gemfile.lock updates for builds, switching between Ruby versions
  gem 'moneta', '> 0.7.2'
  gem 'timecop', '> 0.6.1'
end
