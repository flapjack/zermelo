## Zermelo Changelog

# 1.4.3 - 2015-12-10

 * bugfix for multiple adds to redis sorted set

# 1.4.2 - 2015-12-01

 * methods to dump generated keys and backend key names

# 1.4.1 - 2015-11-24

 * bugfix require statements

# 1.4.0 - 2015-10-07

 * store sorted_set ids in zset, makes some queries combine properly that
   previously resulted in errors
 * bugfix transactions, weren't applied properly

# 1.3.0 - 2015-08-15

 * handle some Zermelo objects as query values, less data back-and-forth
 * return Set/OrderedSet objects as appropriate (58318c25)

# 1.2.1 - 2015-08-07

 * 'empty' filter method (637d0c9c)
 * bugfix; temp redis set not deleted when matching against multiple values for a field (9df5241d)
 * use Redis .scan instead of .keys for regex index lookup if available (de75802c)

# 1.2.0 - 2015-06-24

* spec cleanup, apply to multiple backends if not backend-specific
* fix sorted_sets to compose with other queries
* range queries against sorted sets
* improve setting association from its inverse side; all callbacks now called properly
* renamed .delete on associations to .remove
* also support .remove_ids on multiple associations, so record need not be loaded
* stub record type

# 1.1.0 - 2015-04-09

* refactored query builder to improve composability
* bugfix; source key not included when intersecting redis sets
* log redis return values as well when redis log is used

# 1.0.1 - 2015-02-04

* bugfix; has_one class locking (779907d8d)
* add save! command, raise exceptions on error (#13)

# 1.0.0 - 2015-01-28

* Initial release
