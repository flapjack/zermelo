source 'https://rubygems.org'

if RUBY_VERSION.split('.')[0] == '1' && RUBY_VERSION.split('.')[1] == '8'
  gemspec :name => 'sandstorm-ruby1.8'
else
  gemspec :name => 'sandstorm'
end

group :test do
  gem 'influxdb'
  gem 'redis'
  gem 'rspec', '>= 3.0.0'
  gem 'simplecov', :require => false
  gem 'timecop'
end
