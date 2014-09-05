# sandstorm

[![Build Status](https://travis-ci.org/flapjack/sandstorm.png)](https://travis-ci.org/flapjack/sandstorm)

Sandstorm is an [ActiveModel](http://yehudakatz.com/2010/01/10/activemodel-make-any-ruby-object-feel-like-activerecord/)-based [Object-Relational Mapper](http://en.wikipedia.org/wiki/Object-relational_mapping) for [Redis](http://redis.io/), written in [Ruby](http://www.ruby-lang.org/).

## Installation

Add this line to your application's Gemfile:

    gem 'sandstorm'

And then execute:

    $ bundle

Or install it yourself as:

    $ gem install sandstorm

## Requirements

  * Redis 2.4.0 or higher (as it uses the multiple arguments provided in 2.4 for some commands). This could probaby be lowered to 2.0 with some branching for backwards compatibility.

  * Ruby 1.8.7 or higher.

## Usage

### Initialisation

Firstly, you'll need to set up Sandstorm's Redis access, e.g.

```ruby
Sandstorm.redis = Redis.new(:host => '127.0.0.1', :db => 8)
```

### Class ids

Include Sandstorm's Record module in the class you want to persist data from:

```ruby
class Post
  include Sandstorm:Record
end
```

and then create and save an instance of that class:

```ruby
post = Post.new(:id => 'abcde')
post.save
```

Behind the scenes, this will run the following Redis command:

```
SADD post::ids 'abcde'
```

(along with a few others which we'll discuss shortly).

### Simple instance attributes
TODO

### Complex instance attributes
TODO

### Validations
TODO

### Callbacks
TODO

### Detecting changes
TODO

### Loading data
TODO

### Class methods
TODO methods from Filter

### Associations

#### has_many
TODO
#### has_sorted_set
TODO
#### has_one
TODO
#### belongs_to
TODO
#### has_and_belongs_to_many
TODO

### Class data indexing
TODO

### Queries against these indices
TODO

## License

Sandstorm is released under the MIT license:

    www.opensource.org/licenses/MIT

## Contributing

1. Fork it
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Add some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create new Pull Request
