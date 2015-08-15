# zermelo

[![Build Status](https://travis-ci.org/flapjack/zermelo.png)](https://travis-ci.org/flapjack/zermelo)

Zermelo is an [ActiveModel](http://yehudakatz.com/2010/01/10/activemodel-make-any-ruby-object-feel-like-activerecord/)-based [Object-Relational Mapper](http://en.wikipedia.org/wiki/Object-relational_mapping) for [Redis](http://redis.io/), written in [Ruby](http://www.ruby-lang.org/).

## Installation

Add this line to your application's Gemfile:

    gem 'zermelo', :github => 'flapjack/zermelo', :branch => 'master'

And then execute:

    $ bundle

Or install it yourself as:

    $ gem install zermelo

## Requirements

  * Redis 2.4.0 or higher (as it uses the multiple arguments provided in 2.4 for some commands). This could probaby be lowered to 2.0 with some branching for backwards compatibility.

  * Ruby 1.8.7 or higher.

## Usage

### Initialisation

Firstly, you'll need to set up **zermelo**'s Redis access, e.g.

```ruby
Zermelo.redis = Redis.new(:host => '127.0.0.1', :db => 8)
```

You can optionally set `Zermelo.logger` to an instance of a Ruby `Logger` class, or something with a compatible interface, and Zermelo will log the method calls (and arguments) being made to the Redis driver.

### Class ids

Include **zermelo**'s `Zermelo::Records::Redis` module in the class you want to persist data from:

```ruby
class Post
  include Zermelo::Records::Redis
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
  include Zermelo::Records::Redis
  define_attributes :title     => :string,
                    :score     => :integer,
                    :timestamp => :timestamp,
                    :published => :boolean
end
```

and create and save an instance of that model class:

```ruby
post = Post.new(:title => 'Introduction to Zermelo',
  :score => 100, :timestamp => Time.parse('Jan 1 2000'), :published => false)
post.save
```

An `:id => :string` attribute is implicitly defined, but in this case no id was passed, so **zermelo** generates a UUID:

```
HMSET post:03c839ac-24af-432e-aa58-fd1d4bf73f24:attrs title 'Introduction to Zermelo' score 100 timestamp 1384473626.36478 published 'false'
SADD post::attrs:ids 03c839ac-24af-432e-aa58-fd1d4bf73f24
```

which can then be verified by inspection of the object's attributes, e.g.:

```ruby
post.attributes.inpsect # == {:id => '03c839ac-24af-432e-aa58-fd1d4bf73f24', :title => 'Introduction to Zermelo', :score => 100, :timestamp => '2000-01-01 00:00:00 UTC', :published => false}
```

Zermelo supports the following simple attribute types, and automatically validates that the values are of the correct class, casting if possible:

| Type       |  Ruby class                        | Notes |
|------------|-------------------------------|-------|
| :string    |  String                       |       |
| :integer   |  Integer                      |       |
| :float     |  Float                        |       |
| :id        |  String                       |       |
| :timestamp |  Integer or Time or DateTime  | Stored as a float value |
| :boolean   |  TrueClass or FalseClass      | Stored as string 'true' or 'false' |

### Complex instance attributes

**Zermelo** also provides mappings for the compound data structures supported by Redis.

So if we add tags to the Post data definition:

```ruby
class Post
  include Zermelo::Records::Redis
  define_attributes :title     => :string,
                    :score     => :integer,
                    :timestamp => :timestamp,
                    :published => :boolean,
                    :tags      => :set
end
```

and then create another `Post` instance:

```ruby
post = Post.new(:id => 1, :tags => Set.new(['database', 'ORM']))
post.save
```

which would run the following Redis commands:

```
SADD post:1:attrs:tags 'database' 'ORM'
SADD post::attrs:ids 1
```

Zermelo supports the following complex attribute types, and automatically validates that the values are of the correct class, casting if possible:

| Type        |  Ruby class   | Notes                                                   |
|-------------|---------------|---------------------------------------------------------|
| :list       |  Enumerable   | Stored as a Redis [LIST](http://redis.io/commands#list) |
| :set        |  Array or Set | Stored as a Redis [SET](http://redis.io/commands#set)   |
| :hash       |  Hash         | Stored as a Redis [HASH](http://redis.io/commands#hash) |
| :sorted_set |  Enumerable   | Stored as a Redis [ZSET](http://redis.io/commands#zset) |

Structure data members must be primitives that will cast OK to and from Redis via the driver, thus String, Integer and Float.

Redis [sorted sets](http://redis.io/commands#sorted_set) are also supported through **zermelo**'s associations (recommended due to the fact that queries can be constructed against them).

### Validations

All of the [validations](http://api.rubyonrails.org/classes/ActiveModel/Validations/ClassMethods.html) offered by ActiveModel are available in **zermelo** objects.

So an attribute which should be present:

```ruby
class Post
  include Zermelo::Records::Redis
  define_attributes :title    => :string,
                    :score    => :integer
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

ActiveModel's [lifecycle callbacks](http://api.rubyonrails.org/classes/ActiveModel/Callbacks.html) are also supported, and **zermelo** uses similar invocations to ActiveRecord's:

```
before_create,  around_create,  after_create,
before_update,  around_update,  after_update,
before_destroy, around_destroy, after_destroy
```

As noted in the linked documentation, you'll need to `yield` from within an `around_*` callback, or the original action won't be carried out.

### Detecting changes

Another feature added by ActiveModel is the ability to detect changed data in record instances using [ActiveModel::Dirty](http://api.rubyonrails.org/classes/ActiveModel/Dirty.html).

### Locking around changes

**Zermelo** will lock operations to ensure that changes are applied consistently. The locking code is based on [redis-lock](https://github.com/mlanett/redis-lock), but has been extended and customised to allow **zermelo** to lock more than one class at a time. Record saving and destroying is implicitly locked, while if you want to carry out complex queries or changes without worring about what else may be changing data at the same time, you can use the `lock` class method as follows:

```ruby
class Author
  include Zermelo::Records::Redis
end

class Post
  include Zermelo::Records::Redis
end

class Comment
  include Zermelo::Records::Redis
end

Author.lock(Post, Comment) do
  # ... complicated data operations ...
end
```

### Loading data

Assuming a saved `Post` instance has been created:

```ruby
class Post
  include Zermelo::Records::Redis
  define_attributes :title     => :string,
                    :score     => :integer,
                    :timestamp => :timestamp,
                    :published => :boolean
end

post = Post.new(:id => '1234', :title => 'Introduction to Zermelo',
  :score => 100, :timestamp => Time.parse('Jan 1 2000')), :published => false)
post.save
```

which executes the following Redis calls:

```
HMSET post:1234:attrs title 'Introduction to Zermelo' score 100 timestamp 1384473626.36478 published 'false'
SADD post::attrs:ids 1234
```

This data can be loaded into a fresh `Post` instance using the `find_by_id(ID)` class method:

```ruby
same_post = Post.find_by_id('1234')
same_post.attributes # == {:id => '1234', :score => 100, :timestamp => '2000-01-01 00:00:00 UTC', :published => false}
```

You can load more than one record using the `find_by_ids(ID, ID, ...)` class method (returns an array), and raise exceptions if records matching the ids are not found using `find_by_id!(ID)` and `find_by_ids!(ID, ID, ...)`.

### Class methods

Classes that include `Zermelo::Record` have the following class methods made available to them.

|Name                     | Arguments     | Returns |
|-------------------------|---------------|---------|
|`all`                    |               | Returns a Set of all the records stored for this class |
|`each`                   |               | Yields all records to the provided block, returns the same Set as .all(): [Enumerable#each](http://ruby-doc.org/core-2.2.2/Enumerable.html#method-i-each)   |
|`collect` / `map`        |               | Yields all records to the provided block, returns an Array with the values returned from the block: [Enumerable#collect](http://ruby-doc.org/core-2.2.2/Enumerable.html#method-i-collect)  |
|`select` / `find_all`    |               | Yields all records to the provided block, returns an Array with each record where the block returned true: [Enumerable#select](http://ruby-doc.org/core-2.2.2/Enumerable.html#method-i-select)  |
|`reject`                 |               | Yields all records to the provided block, returns an Array with each record where the block returned false: [Enumerable#reject](http://ruby-doc.org/core-2.2.2/Enumerable.html#method-i-reject) |
|`ids`                    |               | Returns a Set with the ids of all stored records |
|`count`                  |               | Returns an Integer count of the number of stored records |
|`empty?`                 |               | Returns true if no records are stored, false otherwise |
|`destroy_all`            |               | Removes all stored records |
|`exists?`                | ID            | Returns true if the record with the id is present, false if not |
|`find_by_id`             | ID            | Returns the instantiated record for the id, or nil if not present |
|`find_by_ids`            | ID, ID, ...   | Returns a Set of instantiated records for the ids, with nils if the respective record is not present |
|`find_by_id!`            | ID            | Returns the instantiated record for the id, or raises a Zermelo::Records::RecordNotFound exception if not present |
|`find_by_ids!`           | ID, ID, ...   | Returns a Set of instantiated records for the ids, or raises a Zermelo::Records::RecordsNotFound exception if any are not present |
|`associated_ids_for` &amp; `associations_for` | association   | (Defined in the `Associations` section below) |

### Instance methods

Instances of classes including `Zermelo::Record` have the following methods:

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

**Zermelo** supports multiple association types, which are named similarly to those provided by ActiveRecord:

|Name                       | Type                      | Redis data structure | Notes |
|---------------------------|---------------------------|----------------------|-------|
| `has_many`                | one-to-many               | [SET](http://redis.io/commands#set) | |
| `has_sorted_set`          | one-to-many               | [ZSET](http://redis.io/commands#sorted_set) | Arguments: `:key` (required), `:order` (optional, `:asc` or `:desc`) |
| `has_one`                 | one-to-one                | [HASH](http://redis.io/commands#hash) | |
| `belongs_to`              | many-to-one or one-to-one | [HASH](http://redis.io/commands#hash) or [STRING](http://redis.io/commands#string)  | Inverse of any of the above three |
| `has_and_belongs_to_many` | many-to-many              | 2 [SET](http://redis.io/commands#set)s | Mirrored by an inverse HaBtM association on the other side. |

```ruby
class Post
  include Zermelo::Records::Redis
  has_many :comments, :class_name => 'Comment', :inverse_of => :post
end

class Comment
  include Zermelo::Records::Redis
  belongs_to :post, :class_name => 'Post', :inverse_of => :comments
end
```

Class names of the associated class are used, instead of a reference to the class itself, to avoid circular dependencies being established. The inverse association is provided in order that multiple associations between the same two classes can be created.

Records are added and removed from their parent one-to-many or many-to-many associations like so:

```ruby
post.comments.add(comment) # or post.comments << comment
post.comments.remove(comment)
```

Associations' `.add`/`.remove` can also take more than one argument:

```ruby
post.comments.add(comment1, comment2, comment3)
post.comments.remove(comment1, comment2, comment3)
```

If you only have ids available, you don't need to `.load` the respective objects, you can instead use `.add_ids`/`.remove_ids`:

```ruby
post.comments.add_ids("comment_id")
post.comments.remove_ids("comment_id")
post.comments.add_ids("comment1_id", "comment2_id", "comment3_id")
post.comments.remove_ids("comment1_id", "comment2_id", "comment3_id")
```

`has_one` associations are simply set with an `=` method on the association:

```ruby
class User
  include Zermelo::Records::Redis
  has_one :preferences, :class_name => 'Preferences', :inverse_of => :user
end

class Preferences
  include Zermelo::Records::Redis
  belongs_to :user, :class_name => 'User', :inverse_of => :preferences
end

user  = User.new
user.save
prefs = Preferences.new
prefs.save

user.preferences = prefs
```

and cleared by assigning the association to nil:

```ruby
user.preferences = nil
```

The class methods defined above can be applied to associations references as well, so the resulting data will be filtered by the data relationships applying in the association, e.g.

```ruby
post = Post.new(:id => 'a')
post.save
comment1 = Comment.new(:id => '1')
comment1.save
comment2 = Comment.new(:id => '2')
comment2.save

p post.comments.ids # == #<Set: {}>
p Comment.ids       # == #<Set: {'1', '2'}>
post.comments << comment1
p post.comments.ids # == #<Set: {'1'}>
```

`.associated_ids_for` is somewhat of a special case; it uses the simplest queries possible to get the ids of the associated records of a set of records, e.g. for the data directly above:

```ruby
Post.associated_ids_for(:comments)                       # => {'a' => #<Set: {'1'}>}

post_b = Post.new(:id => 'b')
post_b.save
post_b.comments << comment2
comment3 = Comment.new(:id => '3')
comment3.save
post.comments << comment3

Post.associated_ids_for(:comments)                       # => {'a' => #<Set: {'1', '3'}>, 'b' => #<Set: {'2'}>}
```

For `belongs to` associations, you may pass an extra option to `associated_ids_for`, `:inversed => true`, and you'll get the data back as if it were applied from the inverse side; however the data will only cover that used as the query root. Again, assuming the data from the last two code blocks, e.g.

```ruby
Comment.associated_ids_for(:post)                    # => {'1' => 'a', '2' => 'b', '3' => 'a'}
Comment.associated_ids_for(:post, :inversed => true) # => {'a' => #<Set: {'1', '3'}>, 'b' => #<Set: {'2'}>}
```

`.associations_for` returns chainable Zermelo association proxy objects, rather than sets of ids, as the Hash values. Please note, `.associations_for` only works with multiple associations (`has_many`, `has_and_belongs_to_many`, `has_sorted_set`).

### Class data indexing

Simple instance attributes, as defined above, can be indexed by value (and those indices can be queried).

Using the code from the instance attributes section, and adding indexing:

```ruby
class Post
  include Zermelo::Records::Redis
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
post = Post.new(:title => 'Introduction to Zermelo',
  :score => 100, :timestamp => Time.parse('Jan 1 2000'), :published => false)
post.save
```

some extra class-level data is saved, in order that it is able to be queried later:

```
HMSET post:03c839ac-24af-432e-aa58-fd1d4bf73f24:attrs title 'Introduction to Zermelo' score 100 timestamp 1384473626.36478 published 'false'
SADD post::attrs:ids 03c839ac-24af-432e-aa58-fd1d4bf73f24
HSET post::indices:by_title 'Introduction to Zermelo' 03c839ac-24af-432e-aa58-fd1d4bf73f24
SADD post::indices:by_published:boolean:false 03c839ac-24af-432e-aa58-fd1d4bf73f24
```


### Queries against these indices

`Zermelo` will construct Redis queries for you based on higher-level data expressions. Only those properties that are indexed can be queried against, as well as `:id` -- this ensures that most operations are carried out purely within Redis against collections of id values.


| Name            | Input                 | Output       | Arguments                             | Options                                  |
|-----------------|-----------------------|--------------|---------------------------------------|------------------------------------------|
| intersect       | `set` / `sorted_set`  | (as input)   | Query hash                            |                                          |
| union           | `set` / `sorted_set`  | (as input)   | Query hash                            |                                          |
| diff            | `set` / `sorted_set`  | (as input)   | Query hash                            |                                          |
| sort            | `set` or `sorted_set` | `list`       | keys (Symbol or Array of Symbols)     | :limit (`Integer`), :offset (`Integer`)  |
| offset          | `list` / `sorted_set` | `list`       | amount (`Integer`)                    | :limit (`Integer`)                                        |
| page            | `list` / `sorted_set` | `list`       | page_number (`Integer`)               | :per_page (`Integer`)                                    |

These queries can be applied against all instances of a class, or against associations belonging to an instance, e.g.

```ruby
post.comments.intersect(:title => 'Interesting')
Comment.intersect(:title => 'Interesting')
```

are both valid, and the `Comment` instances returned by the first query would be contained in those returned by the second.

The chained queries are only executed when the results are invoked (lazy evaluation) by the addition of one of the class methods listed above; e.g.

```ruby
Comment.intersect(:title => 'Interesting').all    # -> #<Set: {Comment, Comment, ...}>
Comment.intersect(:title => 'Interesting', :promoted => true).count  # -> Integer
```

Assuming one `Comment` record exists, the first of these (`.all`) will execute the Redis commands

```
SINTER comment::attrs:ids comment::indices:by_title:string:Interesting
HGET comment:ca9e427d-4d81-47f8-bcfe-bb614d40528c:attrs title
```

with the result being a Set with one member, a Comment record with `{:id => 'ca9e427d-4d81-47f8-bcfe-bb614d40528c', :title => 'Interesting'}`

and the second (`.count`) will execute these Redis commands.

```
SINTERSTORE comment::tmp:fe8dd59e4a1197f62d19c8aa942c4ff9 comment::indices:by_title:string:Interesting  comment::indices:by_promoted:boolean:true
SCARD comment::tmp:fe8dd59e4a1197f62d19c8aa942c4ff9
DEL comment::tmp:fe8dd59e4a1197f62d19c8aa942c4ff9
```

(where the name of the temporary Redis `SET` will of course change every time)

---

`has_sorted_set` queries can take exact values, or a range bounded in no, one or both directions. (Regular Ruby `Range` objects can't be used as they don't easily support timestamps, so there's a `Zermelo::Filters::IndexRange` class which can be used as a query value instead.)

```ruby
class Comment
  include Zermelo::Records::Redis
  define_attributes :created_at => :timestamp
end

t = Time.now

comment1 = Comment.new(:id => '1', :created_at => t - 120)
comment1.save
comment2 = Comment.new(:id => '2', :created_at => t - 60)
comment2.save

range = Zermelo::Filters::IndexRange.new(t - 90, t, :by_score => true)
Comment.ids # #<Set: {'1', '2'}>
Comment.intersect(:created_at => range).ids # #<Set: {'2'}>
```

### Future

Some possible changes:

* pluggable id generation strategies
* pluggable key naming strategies
* instrumentation for benchmarking etc.
* multiple data backends

## License

Zermelo is released under the MIT license:

    www.opensource.org/licenses/MIT

## Contributing

1. Fork it
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Add some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create new Pull Request
