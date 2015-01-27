# sandstorm

[![Build Status](https://travis-ci.org/flapjack/sandstorm.png)](https://travis-ci.org/flapjack/sandstorm)

Sandstorm is an [ActiveModel](http://yehudakatz.com/2010/01/10/activemodel-make-any-ruby-object-feel-like-activerecord/)-based [Object-Relational Mapper](http://en.wikipedia.org/wiki/Object-relational_mapping) for [Redis](http://redis.io/), written in [Ruby](http://www.ruby-lang.org/).

## Installation

Add this line to your application's Gemfile:

    gem 'sandstorm', :github => 'flapjack/sandstorm', :branch => 'master'

And then execute:

    $ bundle

<!--
Or install it yourself as:

    $ gem install sandstorm
-->

## Requirements

  * Redis 2.4.0 or higher (as it uses the multiple arguments provided in 2.4 for some commands). This could probaby be lowered to 2.0 with some branching for backwards compatibility.

  * Ruby 1.8.7 or higher.

## Usage

### Initialisation

Firstly, you'll need to set up **sandstorm**'s Redis access, e.g.

```ruby
Sandstorm.redis = Redis.new(:host => '127.0.0.1', :db => 8)
```

You can optionally set `Sandstorm.logger` to an instance of a Ruby `Logger` class, or something with a compatible interface, and Sandstorm will log the method calls (and arguments) being made to the Redis driver.

### Class ids

Include **sandstorm**'s Record module in the class you want to persist data from:

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
SADD post::attrs:ids 'abcde'
```

(along with a few others which we'll discuss shortly).

### Simple instance attributes

A data record without any actual data isn't very useful, so let's add a few simple data fields to the Post model:

```ruby
class Post
  include Sandstorm:Record
  define_attributes :title     => :string,
                    :score     => :integer,
                    :timestamp => :timestamp,
                    :published => :boolean
end
```

and create and save an instance of that model class:

```ruby
post = Post.new(:title => 'Introduction to Sandstorm',
  :score => 100, :timestamp => Time.parse('Jan 1 2000'), :published => false)
post.save
```

An `:id => :string` attribute is implicitly defined, but in this case no id was passed, so **sandstorm** generates a UUID:

```
HMSET post:03c839ac-24af-432e-aa58-fd1d4bf73f24:attrs title 'Introduction to Sandstorm' score 100 timestamp 1384473626.36478 published 'false'
SADD post::attrs:ids 03c839ac-24af-432e-aa58-fd1d4bf73f24
```

which can then be verified by inspection of the object's attributes, e.g.:

```ruby
post.attributes.inpsect # == {:id => '03c839ac-24af-432e-aa58-fd1d4bf73f24', :title => 'Introduction to Sandstorm', :score => 100, :timestamp => '2000-01-01 00:00:00 UTC', :published => false}
```

Sandstorm supports the following simple attribute types, and automatically
validates that the values are of the correct class, casting if possible:

| Type       |  Ruby class                        | Notes |
|------------|-------------------------------|-------|
| :string    |  String                       |       |
| :integer   |  Integer                      |       |
| :float     |  Float                        |       |
| :id        |  String                       |       |
| :timestamp |  Integer or Time or DateTime  | Stored as a float value |
| :boolean   |  TrueClass or FalseClass      | Stored as string 'true' or 'false' |

### Complex instance attributes

**Sandstorm** also provides mappings for the compound data structures supported by Redis.

So if we add tags to the Post data definition:

```ruby
class Post
  include Sandstorm:Record
  define_attributes :title     => :string,
                    :score     => :integer,
                    :timestamp => :timestamp,
                    :published => :boolean,
                    :tags      => :set
end
```

and then create another

```ruby
post = Post.new(:id => 1, :tags => Set.new(['database', 'ORM']))
post.save
```

which would run the following Redis commands:

```
SADD post:1:attrs:tags 'database' 'ORM'
SADD post::attrs:ids 1
```

Sandstorm supports the following complex attribute types, and automatically
validates that the values are of the correct class, casting if possible:

| Type       |  Ruby class   | Notes                                                   |
|------------|---------------|---------------------------------------------------------|
| :list      |  Enumerable   | Stored as a Redis [LIST](http://redis.io/commands#list) |
| :set       |  Array or Set | Stored as a Redis [SET](http://redis.io/commands#set)   |
| :hash      |  Hash         | Stored as a Redis [HASH](http://redis.io/commands#hash) |

Structure data members must be primitives that will cast OK to and from Redis via the
driver, thus String, Integer and Float.

Redis [sorted sets](http://redis.io/commands#sorted_set) are only supported through associations, for which see later on.

### Validations

All of the [validations](http://api.rubyonrails.org/classes/ActiveModel/Validations/ClassMethods.html) offered by ActiveModel are available in **sandstorm** objects.

So an attribute which should be present:

```ruby
class Post
  include Sandstorm:Record
  define_attributes :title     => :string,
                    :score     => :integer
  validates :title, :presence => true
end
```

but isn't:

```ruby
post = Post.new(:score => 85)
post.valid? # == false

post.errors.full_messages # == ["Title can't be blank"]
post.save # calls valid? before saving, fails and returns false
```

produces the results you would expect.

### Callbacks

ActiveModel's [lifecycle callbacks](http://api.rubyonrails.org/classes/ActiveModel/Callbacks.html) are also supported, and **sandstorm** uses similar invocations to ActiveRecord's:

```
before_create,  around_create,  after_create,
before_update,  around_update,  after_update,
before_destroy, around_destroy, after_destroy
```

As noted in the linked documentation, you'll need to `yield` from within an `around_*` callback, or the original action won't be carried out.

### Detecting changes

Another feature added by ActiveModel is the ability to detect changed data in record instances using [ActiveModel::Dirty](http://api.rubyonrails.org/classes/ActiveModel/Dirty.html).

### Locking around changes

**Sandstorm** will lock operations to ensure that changes are applied consistently. The locking code is based on [redis-lock](https://github.com/mlanett/redis-lock), but has been extended and customised to allow **sandstorm** to lock more than one class at a time. Record saving and destroying is implicitly locked, while if you want to carry out complex queries or changes without worring about what else may be changing data at the same time, you can use the `lock` class method as follows:

```ruby
class Author
  include Sandstorm:Record
end

class Post
  include Sandstorm:Record
end

class Comment
  include Sandstorm:Record
end

Author.lock(Post, Comment) do
  # ... complicated data operations ...
end
```

### Loading data

Assuming a saved `Post` instance has been created:

```ruby
class Post
  include Sandstorm:Record
  define_attributes :title     => :string,
                    :score     => :integer,
                    :timestamp => :timestamp,
                    :published => :boolean
end

post = Post.new(:id => '1234', :title => 'Introduction to Sandstorm',
  :score => 100, :timestamp => Time.parse('Jan 1 2000')), :published => false)
post.save
```

which executes the following Redis calls:

```
HMSET post:1234:attrs title 'Introduction to Sandstorm' score 100 timestamp 1384473626.36478 published 'false'
SADD post::attrs:ids 1234
```

This data can be loaded into a fresh `Post` instance using the `find_by_id(ID)` class method:

```ruby
same_post = Post.find_by_id('1234')
same_post.attributes # == {:id => '1234', :score => 100, :timestamp => '2000-01-01 00:00:00 UTC', :published => false}
```

You can load more than one record using the `find_by_ids(ID, ID, ...)` class method (returns an array), and raise exceptions if records matching the ids are not found using `find_by_id!(ID)` and `find_by_ids!(ID, ID, ...)`.

### Class methods

Classes that include `Sandstorm::Record` have the following class methods made available to them.

|Name                     | Arguments     | Returns |
|-------------------------|---------------|---------|
|`all`                    |               | Returns an Array of all the records stored for this class |
|`each`                   |               | Yields all records to the provided block, returns the same Array as .all(): [Array#each](http://ruby-doc.org/core-2.1.2/Array.html#method-i-each)   |
|`collect` / `map`        |               | Yields all records to the provided block, returns an Array with the values returned from the block: [Array#collect](http://ruby-doc.org/core-2.1.2/Array.html#method-i-collect)  |
|`select` / `find_all`    |               | Yields all records to the provided block, returns an Array with each record where the block returned true: [Array#select](http://ruby-doc.org/core-2.1.2/Array.html#method-i-select)  |
|`reject`                 |               | Yields all records to the provided block, returns an Array with each record where the block returned false: [Array#reject](http://ruby-doc.org/core-2.1.2/Array.html#method-i-reject) |
|`ids`                    |               | Returns an Array with the ids of all stored records |
|`count`                  |               | Returns an Integer count of the number of stored records |
|`empty?`                 |               | Returns true if no records are stored, false otherwise |
|`destroy_all`            |               | Removes all stored records |
|`exists?`                | ID            | Returns true if the record with the id is present, false if not |
|`find_by_id`             | ID            | Returns the instantiated record for the id, or nil if not present |
|`find_by_ids`            | ID, ID, ...   | Returns an Array of instantiated records for the ids, with nils if the respective record is not present |
|`find_by_id!`            | ID            | Returns the instantiated record for the id, or raises a Sandstorm::Records::RecordNotFound exception if not present |
|`find_by_ids!`           | ID, ID, ...   | Returns an Array of instantiated records for the ids, or raises a Sandstorm::Records::RecordsNotFound exception if any are not present |
|`associated_ids_for`     | association   | (Defined in the `Associations` section below) |

### Instance methods

Instances of classes including `Sandstorm::Record` have the following methods:

|Name                 | Arguments     | Returns |
|---------------------|---------------|---------|
|`persisted?`         |               | returns true if the record has been saved, false if not |
|`load`               | ID            | loads the record with the provided ID, discarding current state |
|`refresh`            |               | refreshes the record from saved data, discarding current changes |
|`save`               |               | returns false if validations fail, true and saves data if valid |
|`update_attributes`  | HASH          | mass assignment of attribute accessors, calls `save()` after attribute changes have been applied |
|`destroy`            |              | removes the saved data for the record |

Instances also have attribute accessors and the various methods included from the ActiveModel classes mentioned earlier.

### Associations

**Sandstorm** supports multiple association types, which are named similarly to those provided by ActiveRecord:

|Name                       | Type                      | Redis data structure | Notes |
|---------------------------|---------------------------|----------------------|-------|
| `has_many`                | one-to-many               | [SET](http://redis.io/commands#set) | |
| `has_sorted_set`          | one-to-many               | [ZSET](http://redis.io/commands#sorted_set) | |
| `has_one`                 | one-to-one                | [HASH](http://redis.io/commands#hash) | |
| `belongs_to`              | many-to-one or one-to-one | [HASH](http://redis.io/commands#hash) or [STRING](http://redis.io/commands#string)  | Inverse of any of the above three |
| `has_and_belongs_to_many` | many-to-many              | 2 [SET](http://redis.io/commands#set)s | Mirrored by an inverse HaBtM association on the other side. |

```ruby
class Post
  include Sandstorm:Record
  has_many :comments, :class_name => 'Comment', :inverse_of => :post
end

class Comment
  include Sandstorm:Record
  belongs_to :post, :class_name => 'Post', :inverse_of => :comments
end
```

Class names of the associated class are used, instead of a reference to the class itself, to avoid circular dependencies being established. The inverse association is provided in order that multiple associations between the same two classes can be created.

Records are added and removed from their parent one-to-many or many-to-many associations like so:

```ruby
post.comments.add(comment) # or post.comments << comment
```

Associations' `.add` can also take more than one argument:

```ruby
post.comments.add(comment1, comment2, comment3)
```

`has_one` associations are simply set with an `=` method on the association:

```ruby
class User
  include Sandstorm:Record
  has_one :preferences, :class_name => 'Preferences', :inverse_of => :user
end

class Preferences
  include Sandstorm:Record
  belongs_to :user, :class_name => 'User', :inverse_of => :preferences
end

user  = User.new
user.save
prefs = Preferences.new
prefs.save

user.preferences = prefs
```

The class methods defined above can be applied to associations references as well, so the resulting data will be filtered by the data relationships applying in the association, e.g.

```ruby
post     = Post.new(:id => 'a')
post.save
comment1 = Comment.new(:id => '1')
comment1.save
comment2 = Comment.new(:id => '2')
comment2.save

p post.comments.ids # == []
p Comment.ids       # == [1, 2]
post.comments << comment1
p post.comments.ids # == [1]
```

`associated_ids_for` is somewhat of a special case; it uses the smallest/simplest queries possible to get the ids of the associated records of a set of records, e.g. for the data directly above:

```ruby
Post.associated_ids_for(:comments)                       # => {'a' => ['1']}

post_b     = Post.new(:id => 'b')
post_b.save
post_b.comments << comment2
comment3 = Comment.new(:id => '3')
comment3.save
post.comments << comment3

Post.associated_ids_for(:comments)                       # => {'a' => ['1', '3'], 'b' => ['2']}
Post.intersect(:id => 'a').associated_ids_for(:comments) # => {'a' => ['1', '3']}
```

For `belongs to` associations, you may pass an extra option to `associated_ids_for`, `:inversed => true`, and you'll get the data back as if it were applied from the inverse side; however the data will only cover that used as the query root. Again, assuming the data from the last two code blocks, e.g.

```ruby
Comment.associated_ids_for(:post)                    # => {'1' => 'a', '2' => 'b', '3' => 'a'}
Comment.associated_ids_for(:post, :inversed => true) # => {'a' => ['1', '3'], 'b' => ['2']}

Comment.intersect(:id => ['1', '2']).associated_ids_for(:post) # => {'1' => 'a', '2' => 'b'}
Comment.intersect(:id => ['1', '2']).associated_ids_for(:post, :inversed => true) # => {'a' => ['1'], 'b' => ['2']}
```

### Class data indexing

Simple instance attributes, as defined above, can be indexed by value (and those indices can be queried).

Using the code from the instance attributes section, and adding indexing:

```ruby
class Post
  include Sandstorm:Record
  define_attributes :title     => :string,
                    :score     => :integer,
                    :timestamp => :timestamp,
                    :published => :boolean

  unique_index_by :title
  index_by :published

  validates :title, :presence => true
end
```

when we again create and save our instance of that model class:

```ruby
post = Post.new(:title => 'Introduction to Sandstorm',
  :score => 100, :timestamp => Time.parse('Jan 1 2000'), :published => false)
post.save
```

some extra class-level data is saved, in order that it is able to be queried later:

```
HMSET post:03c839ac-24af-432e-aa58-fd1d4bf73f24:attrs title 'Introduction to Sandstorm' score 100 timestamp 1384473626.36478 published 'false'
SADD post::attrs:ids 03c839ac-24af-432e-aa58-fd1d4bf73f24
HSET post::indices:by_title 'Introduction to Sandstorm' 03c839ac-24af-432e-aa58-fd1d4bf73f24
SADD post::indices:by_published:boolean:false 03c839ac-24af-432e-aa58-fd1d4bf73f24
```


### Queries against these indices

`Sandstorm` will construct Redis queries for you based on higher-level data expressions. Only those properties that are indexed can be queried against, as well as `:id` -- this ensures that most operations are carried out purely within Redis against collections of id values.


| Name            | Input                 | Output       | Arguments                             | Options                                  |
|-----------------|-----------------------|--------------|---------------------------------------|------------------------------------------|
| intersect       | `set` or `sorted_set` | `set`        | Query hash                            |                                          |
| union           | `set` or `sorted_set` | `set`        | Query hash                            |                                          |
| diff            | `set` or `sorted_set` | `set`        | Query hash                            |                                          |
| intersect_range | `sorted_set`          | `sorted_set` | start (`Integer`), finish (`Integer`) | :desc (`Boolean`), :by_score (`Boolean`) |
| union_range     | `sorted_set`          | `sorted_set` | start (`Integer`), finish (`Integer`) | :desc (`Boolean`), :by_score (`Boolean`) |
| diff_range      | `sorted_set`          | `sorted_set` | start (`Integer`), finish (`Integer`) | :desc (`Boolean`), :by_score (`Boolean`) |
| sort            | `set` or `sorted_set` | `list`       | keys (Symbol or Array of Symbols)     | :limit (`Integer`), :offset (`Integer`)  |
| offset          | `list`                | `list`       | amount (`Integer`)                    |                                          |
| limit           | `list`                | `list`       | amount (`Integer`)                    |                                          |

These queries can be applied against all instances of a class, or against associations belonging to an instance, e.g.

```ruby
post.comments.intersect(:title => 'Interesting')
Comment.intersect(:title => 'Interesting')
```

are both valid, and the `Comment` instances returned by the first query would be contained in those returned by the second.

The chained queries are only executed when the results are invoked (lazy evaluation) by the addition of one of the class methods listed above; e.g.

```ruby
Comment.intersect(:title => 'Interesting').all    # -> [Comment, Comment, ...]
Comment.intersect(:title => 'Interesting', :promoted => true).count  # -> Integer
```

Assuming one `Comment` record exists, the first of these (`.all`) will execute the Redis commands

```
SINTER comment::attrs:ids comment::indices:by_title:string:Interesting
HGET comment:ca9e427d-4d81-47f8-bcfe-bb614d40528c:attrs title
```

with the result being an Array with one member, a Comment record with `{:id => 'ca9e427d-4d81-47f8-bcfe-bb614d40528c', :title => 'Interesting'}`

and the second (`.count`) will execute these Redis commands.

```
SINTERSTORE comment::tmp:fe8dd59e4a1197f62d19c8aa942c4ff9 comment::indices:by_title:string:Interesting  comment::indices:by_promoted:boolean:true
SCARD comment::tmp:fe8dd59e4a1197f62d19c8aa942c4ff9
DEL comment::tmp:fe8dd59e4a1197f62d19c8aa942c4ff9
```

(where the name of the temporary Redis `SET` will of course change every time)

The current implementation of the filtering is somewhat ad-hoc, and has these limitations:

* no conversion of `list`s back into `set`s is allowed
* `sort`/`offset`/`limit` can only be used once in a filter chain

I plan to fix these as soon as I possibly can.

### Future

Some possible changes:

* pluggable key naming strategies
* pluggable id generation strategies
* instrumentation for benchmarking etc.
* multiple data backends; there's currently an experimental InfluxDB backend, and more are planned.

## License

Sandstorm is released under the MIT license:

    www.opensource.org/licenses/MIT

## Contributing

1. Fork it
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Add some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create new Pull Request
