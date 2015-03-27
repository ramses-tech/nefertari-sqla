SQLA Engine
===========

Common API
==========

**BaseMixin**
    Mixin with a most of the API of *BaseDocument*. *BaseDocument* subclasses from this mixin.

**BaseDocument**
    Base for regular models defined in your application. Just subclass it to define your model's fields. Relevant attributes:
        * **_auth_fields**: String names of fields meant to be displayed to authenticated users.
        * **_public_fields**: String names of fields meant to be displayed to non-authenticated users.
        * **_nested_relationships**: String names of relationship fields that should be included in JSON data of an object as full included documents. If relationship field is not present in this list, this field's value in JSON will be an object's ID or list of IDs.

**ESBaseDocument**
    Subclass of *BaseDocument* instances of which are indexed on create/update/delete.

**ESMetaclass**
    Document metaclass which is used in *ESBaseDocument* to enable automatic indexation to Elasticsearch of documents.

**get_document_cls(name)**
    Helper function used to get the class of document by the name of the class.

**JSONEncoder**
    JSON encoder that should be used to encode output of views.

**ESJSONSerializer**
    JSON encoder used to encode documents prior indexing them in Elasticsearch.

**relationship_fields**
    Tuple of classes that represent relationship fields in specific engine.

**is_relationship_field(field, model_cls)**
    Helper function to determine whether *field* is a relationship field at *model_cls* class.

**relationship_cls(field, model_cls)**
    Return class which is pointed to by relationship field *field* from model *model_cls*.

Fields abstractions
===================

* BigIntegerField
* BooleanField
* DateField
* DateTimeField
* ChoiceField
* FloatField
* IntegerField
* IntervalField
* BinaryField
* DecimalField
* PickleField
* SmallIntegerField
* StringField
* TextField
* TimeField
* UnicodeField
* UnicodeTextField
* Relationship
* PrimaryKeyField
* ForeignKeyField


Documents
---------

.. autoclass:: nefertari_sqla.documents.BaseMixin
    :members:
    :special-members:
    :private-members:

.. autoclass:: nefertari_sqla.documents.BaseDocument
    :members:
    :special-members:
    :private-members:

.. autoclass:: nefertari_sqla.documents.ESBaseDocument
    :members:
    :special-members:
    :private-members:


Serializers
-----------

.. autoclass:: nefertari_sqla.serializers.JSONEncoder
    :members:
    :special-members:
    :private-members:

.. autoclass:: nefertari_sqla.serializers.ESJSONSerializer
    :members:
    :special-members:
    :private-members:


Fields
------


.. autoclass:: nefertari_sqla.fields.IntegerField
    :members:
    :special-members:
    :private-members:

.. autoclass:: nefertari_sqla.fields.BigIntegerField
    :members:
    :special-members:
    :private-members:

.. autoclass:: nefertari_sqla.fields.SmallIntegerField
    :members:
    :special-members:
    :private-members:

.. autoclass:: nefertari_sqla.fields.BooleanField
    :members:
    :special-members:
    :private-members:

.. autoclass:: nefertari_sqla.fields.DateField
    :members:
    :special-members:
    :private-members:

.. autoclass:: nefertari_sqla.fields.DateTimeField
    :members:
    :special-members:
    :private-members:

.. autoclass:: nefertari_sqla.fields.FloatField
    :members:
    :special-members:
    :private-members:

.. autoclass:: nefertari_sqla.fields.StringField
    :members:
    :special-members:
    :private-members:

.. autoclass:: nefertari_sqla.fields.TextField
    :members:
    :special-members:
    :private-members:

.. autoclass:: nefertari_sqla.fields.UnicodeField
    :members:
    :special-members:
    :private-members:

.. autoclass:: nefertari_sqla.fields.UnicodeTextField
    :members:
    :special-members:
    :private-members:

.. autoclass:: nefertari_sqla.fields.ChoiceField
    :members:
    :special-members:
    :private-members:

.. autoclass:: nefertari_sqla.fields.BinaryField
    :members:
    :special-members:
    :private-members:

.. autoclass:: nefertari_sqla.fields.DecimalField
    :members:
    :special-members:
    :private-members:

.. autoclass:: nefertari_sqla.fields.TimeField
    :members:
    :special-members:
    :private-members:

.. autoclass:: nefertari_sqla.fields.PickleField
    :members:
    :special-members:
    :private-members:

.. autoclass:: nefertari_sqla.fields.IntervalField
    :members:
    :special-members:
    :private-members:

.. autoclass:: nefertari_sqla.fields.PrimaryKeyField
    :members:
    :special-members:
    :private-members:

.. autoclass:: nefertari_sqla.fields.ForeignKeyField
    :members:
    :special-members:
    :private-members:

.. autoclass:: nefertari_sqla.fields.Relationship
    :members:
    :special-members:
    :private-members:
