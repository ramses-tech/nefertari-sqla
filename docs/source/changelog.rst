Changelog
=========

* :release:`0.4.2 <????-??-??>`
* :bug:`90` Deprecated '_version' field

* :release:`0.4.1 <2015-11-18>`
* :bug:`-` Cosmetic name changes in preparation of engine refactoring

* :release:`0.4.0 <2015-10-07>`
* :feature:`-` Nested relationships are now indexed in bulk in ElasticSearch
* :feature:`-` Added '_nesting_depth' property in models to control the level of nesting, default is 1

* :release:`0.3.3 <2015-09-02>`
* :bug:`-` Fixed a bug when using reserved query params with GET tunneling
* :bug:`-` Fixed ES double indexation bug

* :release:`0.3.2 <2015-08-19>`
* :bug:`-` Fixed a bug whereby objects could not be deleted from within processors
* :bug:`-` Fixed a bug with _update_many() and _delete_many() not working with querysets returned by get_collection()
* :bug:`-` Fixed a bug with BaseMixin.filter_objects() not correctly applying additional filters passed to it

* :release:`0.3.1 <2015-07-07>`
* :bug:`-` Fixed bug with Elasticsearch re-indexing of nested relationships
* :bug:`-` Removed 'updated_at' field from engine
* :bug:`-` Disabled Elasticsearch indexing of DictField to allow storing arbitrary JSON data

* :release:`0.3.0 <2015-06-14>`
* :support:`-` Added python3 support

* :release:`0.2.4 <2015-06-05>`
* :bug:`-` Forward compatibility with nefertari releases

* :release:`0.2.3 <2015-06-03>`
* :bug:`-` Fixed password minimum length support by adding before and after validation processors
* :bug:`-` Fixed bug with Elasticsearch indexing of nested relationships
* :bug:`-` Fixed race condition in Elasticsearch indexing

* :release:`0.2.2 <2015-05-27>`
* :bug:`-` Fixed login issue
* :bug:`-` Fixed posting to singular resources e.g. /api/users/<username>/profile
* :bug:`-` Fixed multiple foreign keys to same model
* :bug:`-` Fixed ES mapping error when values of field were all null
* :bug:`-` Fixed a bug whereby Relationship could not be created without a backref

* :release:`0.2.1 <2015-05-20>`
* :bug:`-` Fixed slow queries to backrefs

* :release:`0.2.0 <2015-05-19>`
* :feature:`-` Relationship indexing

* :release:`0.1.1 <2015-04-01>`

* :release:`0.1.0 <2015-04-01>`
