import copy
import logging

import six
from sqlalchemy.orm import (
    class_mapper, object_session, properties, attributes)
from sqlalchemy.orm.collections import InstrumentedList
from sqlalchemy.exc import (
    InvalidRequestError, IntegrityError, DataError)
from sqlalchemy.orm.exc import MultipleResultsFound, NoResultFound
from sqlalchemy.orm.query import Query
from sqlalchemy.orm.properties import RelationshipProperty
from pyramid_sqlalchemy import Session, BaseObject
from sqlalchemy_utils.types.json import JSONType

from nefertari.json_httpexceptions import (
    JHTTPBadRequest, JHTTPNotFound, JHTTPConflict)
from nefertari.utils import (
    process_fields, process_limit, _split, dictset,
    DataProxy)
from .signals import ESMetaclass, on_bulk_delete
from .fields import (
    ListField, DictField, IntegerField, apply_column_processors)
from . import types


log = logging.getLogger(__name__)


def get_document_cls(name):
    try:
        return BaseObject._decl_class_registry[name]
    except KeyError:
        raise ValueError('SQLAlchemy model `{}` does not exist'.format(name))


def get_document_classes():
    """ Get all defined not abstract document classes

    Class is assumed to be non-abstract if it has `__table__` or
    `__tablename__` attributes defined.
    """
    document_classes = {}
    registry = BaseObject._decl_class_registry
    for model_name, model_cls in registry.items():
        tablename = (getattr(model_cls, '__table__', None) is not None or
                     getattr(model_cls, '__tablename__', None) is not None)
        if tablename:
            document_classes[model_name] = model_cls
    return document_classes


def process_lists(_dict):
    for k in _dict:
        new_k, _, _t = k.partition('__')
        if _t == 'in' or _t == 'all':
            _dict[k] = _dict.aslist(k)
    return _dict


def process_bools(_dict):
    for k in _dict:
        new_k, _, _t = k.partition('__')
        if _t == 'bool':
            _dict[new_k] = _dict.pop_bool_param(k)
    return _dict


TYPES_MAP = {
    types.LimitedString: {'type': 'string'},
    types.LimitedText: {'type': 'string'},
    types.LimitedUnicode: {'type': 'string'},
    types.LimitedUnicodeText: {'type': 'string'},
    types.Choice: {'type': 'string'},

    types.Boolean: {'type': 'boolean'},
    types.LargeBinary: {'type': 'object'},
    JSONType: {'type': 'object', 'enabled': False},

    types.LimitedNumeric: {'type': 'double'},
    types.LimitedFloat: {'type': 'double'},

    types.LimitedInteger: {'type': 'long'},
    types.LimitedBigInteger: {'type': 'long'},
    types.LimitedSmallInteger: {'type': 'long'},
    types.Interval: {'type': 'long'},

    types.DateTime: {'type': 'date', 'format': 'dateOptionalTime'},
    types.Date: {'type': 'date', 'format': 'dateOptionalTime'},
    types.Time: {'type': 'date', 'format': 'HH:mm:ss'},
}


class BaseMixin(object):
    """ Represents mixin class for models.

    Attributes:
        _auth_fields: String names of fields meant to be displayed to
            authenticated users.
        _public_fields: String names of fields meant to be displayed to
            non-authenticated users.
        _nested_relationships: String names of relationship fields
            that should be included in JSON data of an object as full
            included documents. If relationship field is not
            present in this list, this field's value in JSON will be an
            object's ID or list of IDs.
    """
    _public_fields = None
    _auth_fields = None
    _nested_relationships = ()

    _type = property(lambda self: self.__class__.__name__)

    @classmethod
    def get_es_mapping(cls):
        """ Generate ES mapping from model schema. """
        from nefertari.elasticsearch import ES
        properties = {}
        mapping = {
            ES.src2type(cls.__name__): {
                'properties': properties
            }
        }
        mapper = class_mapper(cls)
        columns = {c.name: c for c in mapper.columns}
        relationships = {r.key: r for r in mapper.relationships}

        for name, column in columns.items():
            column_type = column.type
            if isinstance(column_type, types.ChoiceArray):
                column_type = column_type.impl.item_type
            column_type = type(column_type)
            if column_type not in TYPES_MAP:
                continue
            properties[name] = TYPES_MAP[column_type]

        for name, column in relationships.items():
            if name in cls._nested_relationships:
                column_type = {'type': 'object'}
            else:
                rel_pk_field = column.mapper.class_.pk_field_type()
                column_type = TYPES_MAP[rel_pk_field]
            properties[name] = column_type

        properties['_pk'] = {'type': 'string'}
        return mapping

    @classmethod
    def autogenerate_for(cls, model, set_to):
        """ Setup `after_insert` event handler.

        Event handler is registered for class :model: and creates a new
        instance of :cls: with a field :set_to: set to an instance on
        which the event occured.
        """
        from sqlalchemy import event

        def generate(mapper, connection, target):
            cls(**{set_to: target})

        event.listen(model, 'after_insert', generate)

    @classmethod
    def pk_field(cls):
        """ Get a primary key field name. """
        return class_mapper(cls).primary_key[0].name

    @classmethod
    def pk_field_type(cls):
        return class_mapper(cls).primary_key[0].type.__class__

    @classmethod
    def check_fields_allowed(cls, fields):
        """ Check if `fields` are allowed to be used on this model. """
        fields = [f.split('__')[0] for f in fields]
        fields_to_query = set(cls.fields_to_query())
        if not set(fields).issubset(fields_to_query):
            not_allowed = set(fields) - fields_to_query
            raise JHTTPBadRequest(
                "'%s' object does not have fields: %s" % (
                    cls.__name__, ', '.join(not_allowed)))

    @classmethod
    def filter_fields(cls, params):
        """ Filter out fields with invalid names. """
        fields = cls.fields_to_query()
        return dictset({
            name: val for name, val in params.items()
            if name.split('__')[0] in fields
        })

    @classmethod
    def apply_fields(cls, query_set, _fields):
        """ Apply fields' restrictions to `query_set`.

        First, fields are split to fields that should only be included and
        fields that should be excluded. Then excluded fields are removed
        from included fields.
        """
        fields_only, fields_exclude = process_fields(_fields)
        if not (fields_only or fields_exclude):
            return query_set
        try:
            fields_only = fields_only or cls.native_fields()
            fields_exclude = fields_exclude or []
            if fields_exclude:
                # Remove fields_exclude from fields_only
                fields_only = [
                    f for f in fields_only if f not in fields_exclude]
            if fields_only:
                # Add PK field
                fields_only.append(cls.pk_field())
                fields_only = [
                    getattr(cls, f) for f in sorted(set(fields_only))]
                query_set = query_set.with_entities(*fields_only)

        except InvalidRequestError as e:
            raise JHTTPBadRequest('Bad _fields param: %s ' % e)

        return query_set

    @classmethod
    def apply_sort(cls, query_set, _sort):
        if not _sort:
            return query_set
        sorting_fields = []
        for field in _sort:
            if field.startswith('-'):
                sorting_fields.append(getattr(cls, field[1:]).desc())
            else:
                sorting_fields.append(getattr(cls, field))
        return query_set.order_by(*sorting_fields)

    @classmethod
    def count(cls, query_set):
        return query_set.count()

    @classmethod
    def filter_objects(cls, objects, first=False, **params):
        """ Perform query with :params: on instances sequence :objects:

        Arguments:
            :object: Sequence of :cls: instances on which query should be run.
            :params: Query parameters to filter :objects:.
        """
        id_name = cls.pk_field()
        ids = [getattr(obj, id_name, None) for obj in objects]
        ids = [str(id_) for id_ in ids if id_ is not None]
        field_obj = getattr(cls, id_name)

        query_set = Session().query(cls).filter(field_obj.in_(ids))

        if params:
            params['_limit'] = len(ids)
            params['query_set'] = query_set.from_self()
            query_set = cls.get_collection(**params)

        if first:
            first_obj = query_set.first()
            if not first_obj:
                msg = "'{}({})' resource not found".format(
                    cls.__name__, params)
                raise JHTTPNotFound(msg)
            return first_obj

        return query_set

    @classmethod
    def _pop_iterables(cls, params):
        """ Pop iterable fields' parameters from :params: and generate
        SQLA expressions to query the database.

        Iterable values are found by checking which keys from :params:
        correspond to names of List fields on model.
        If ListField uses the `postgresql.ARRAY` type, the value is
        wrapped in a list.
        """
        iterables = {}
        columns = class_mapper(cls).columns
        columns = {c.name: c for c in columns
                   if isinstance(c, (ListField, DictField))}

        for key, val in params.items():
            col = columns.get(key)
            if col is None:
                continue

            field_obj = getattr(cls, key)
            is_postgres = getattr(col.type, 'is_postgresql', False)

            if isinstance(col, ListField):
                val = [val] if is_postgres else val
                expr = field_obj.contains(val)

            if isinstance(col, DictField):
                raise Exception('DictField database querying is not '
                                'supported')

            iterables[key] = expr

        for key in iterables.keys():
            params.pop(key)

        return list(iterables.values()), params

    @classmethod
    def get_collection(cls, **params):
        """
        Params may include '_limit', '_page', '_sort', '_fields'.
        Returns paginated and sorted query set.
        Raises JHTTPBadRequest for bad values in params.
        """
        log.debug('Get collection: {}, {}'.format(cls.__name__, params))
        params.pop('__confirmation', False)
        __strict = params.pop('__strict', True)
        _item_request = params.pop('_item_request', False)

        _sort = _split(params.pop('_sort', []))
        _fields = _split(params.pop('_fields', []))
        _limit = params.pop('_limit', None)
        _page = params.pop('_page', None)
        _start = params.pop('_start', None)
        query_set = params.pop('query_set', None)

        _count = '_count' in params
        params.pop('_count', None)
        _explain = '_explain' in params
        params.pop('_explain', None)
        __raise_on_empty = params.pop('__raise_on_empty', False)

        if query_set is None:
            query_set = Session().query(cls)

        # Remove any __ legacy instructions from this point on
        params = dictset({
            key: val for key, val in params.items()
            if not key.startswith('__')
        })

        iterables_exprs, params = cls._pop_iterables(params)

        if __strict:
            _check_fields = [
                f.strip('-+') for f in list(params.keys()) + _fields + _sort]
            cls.check_fields_allowed(_check_fields)
        else:
            params = cls.filter_fields(params)

        process_lists(params)
        process_bools(params)

        # If param is _all then remove it
        params.pop_by_values('_all')

        try:

            query_set = query_set.filter_by(**params)

            # Apply filtering by iterable expressions
            for expr in iterables_exprs:
                query_set = query_set.from_self().filter(expr)

            _total = query_set.count()
            if _count:
                return _total

            if _limit is None:
                raise JHTTPBadRequest('Missing _limit')

            _start, _limit = process_limit(_start, _page, _limit)

            # Filtering by fields has to be the first thing to do on
            # the query_set!
            query_set = cls.apply_fields(query_set, _fields)
            query_set = cls.apply_sort(query_set, _sort)
            query_set = query_set.offset(_start).limit(_limit)

            if not query_set.count():
                msg = "'%s(%s)' resource not found" % (cls.__name__, params)
                if __raise_on_empty:
                    raise JHTTPNotFound(msg)
                else:
                    log.debug(msg)

        except DataError as ex:
            if _item_request:
                msg = "'{}({})' resource not found".format(
                    cls.__name__, params)
                raise JHTTPNotFound(msg, explanation=ex.message)
            else:
                raise JHTTPBadRequest(str(ex), extra={'data': ex})
        except (InvalidRequestError,) as ex:
            raise JHTTPBadRequest(str(ex), extra={'data': ex})

        query_sql = str(query_set).replace('\n', '')
        if _explain:
            return query_sql

        log.debug('get_collection.query_set: %s (%s)', cls.__name__, query_sql)

        if _fields:
            query_set = cls.add_field_names(query_set, _fields)

        query_set._nefertari_meta = dict(
            total=_total,
            start=_start,
            fields=_fields)
        return query_set

    @classmethod
    def add_field_names(cls, query_set, requested_fields):
        """ Convert list of tuples to dict with proper field keys. """
        from .utils import FieldsQuerySet
        fields = [col['name'] for col in query_set.column_descriptions] + [
            '_type']
        add_vals = (cls.__name__,)
        pk_field = cls.pk_field()

        def _convert(val):
            return dict(zip(fields, val+add_vals))

        def _add_pk(obj):
            if pk_field in obj:
                obj['_pk'] = obj[pk_field]
                if pk_field not in requested_fields:
                    obj.pop(pk_field)
            return obj

        values = query_set.all()
        converted = [_add_pk(_convert(val)) for val in values]
        return FieldsQuerySet(converted)

    @classmethod
    def has_field(cls, field):
        return field in cls.native_fields()

    @classmethod
    def native_fields(cls):
        columns = list(cls._mapped_columns().keys())
        relationships = list(cls._mapped_relationships().keys())
        return columns + relationships

    @classmethod
    def _mapped_columns(cls):
        return {c.name: c for c in class_mapper(cls).columns}

    @classmethod
    def _mapped_relationships(cls):
        return {c.key: c for c in class_mapper(cls).relationships}

    @classmethod
    def fields_to_query(cls):
        query_fields = [
            'id', '_limit', '_page', '_sort', '_fields', '_count', '_start']
        return list(set(query_fields + cls.native_fields()))

    @classmethod
    def get_resource(cls, **params):
        params.setdefault('__raise_on_empty', True)
        params['_limit'] = 1
        params['_item_request'] = True
        query_set = cls.get_collection(**params)
        return query_set.first()

    @classmethod
    def get(cls, **kw):
        return cls.get_resource(
            __raise_on_empty=kw.pop('__raise', False), **kw)

    def unique_fields(self):
        native_fields = class_mapper(self.__class__).columns
        return [f for f in native_fields if f.unique or f.primary_key]

    @classmethod
    def get_or_create(cls, **params):
        defaults = params.pop('defaults', {})
        _limit = params.pop('_limit', 1)
        query_set = cls.get_collection(_limit=_limit, **params)
        try:
            obj = query_set.one()
            return obj, False
        except NoResultFound:
            defaults.update(params)
            new_obj = cls(**defaults)
            new_obj.save()
            return new_obj, True
        except MultipleResultsFound:
            raise JHTTPBadRequest('Bad or Insufficient Params')

    def _update(self, params, **kw):
        process_bools(params)
        self.check_fields_allowed(list(params.keys()))
        columns = {c.name: c for c in class_mapper(self.__class__).columns}
        iter_columns = set(
            k for k, v in columns.items()
            if isinstance(v, (DictField, ListField)))
        pk_field = self.pk_field()

        for key, new_value in params.items():
            # Can't change PK field
            if key == pk_field:
                continue
            if key in iter_columns:
                self.update_iterables(new_value, key, unique=True, save=False)
            else:
                setattr(self, key, new_value)
        return self

    @classmethod
    def _delete_many(cls, items, request=None,
                     synchronize_session=False):
        """ Delete :items: queryset or objects list.

        When queryset passed, Query.delete() is used to delete it. Note that
        queryset may not have limit(), offset(), order_by(), group_by(), or
        distinct() called on it.

        If some of the methods listed above were called, or :items: is not
        a Query instance, one-by-one items update is performed.

        `on_bulk_delete` function is called to delete objects from index
        and to reindex relationships. This is done explicitly because it is
        impossible to get access to deleted objects in signal handler for
        'after_bulk_delete' ORM event.
        """
        if isinstance(items, Query):
            items_count = cls.count(items)
            try:
                delete_items = items.all()
                items.delete(
                    synchronize_session=synchronize_session)
                on_bulk_delete(cls, delete_items, request)
                return items_count
            except Exception as ex:
                log.error(str(ex))
        items_count = len(items)
        session = Session()
        for item in items:
            item._request = request
            session.delete(item)
        session.flush()
        return items_count

    @classmethod
    def _update_many(cls, items, params, request=None,
                     synchronize_session='fetch'):
        """ Update :items: queryset or objects list.

        When queryset passed, Query.update() is used to update it. Note that
        queryset may not have limit(), offset(), order_by(), group_by(), or
        distinct() called on it.

        If some of the methods listed above were called, or :items: is not
        a Query instance, one-by-one items update is performed.
        """
        if isinstance(items, Query):
            items._request = request
            items.update(
                params, synchronize_session=synchronize_session)
            return cls.count(items)
        items_count = len(items)
        for item in items:
            item.update(params, request)
        return items_count

    def __repr__(self):
        pk_field = self.pk_field()
        parts = [
            '{}={}'.format(pk_field, getattr(self, pk_field)),
        ]
        if hasattr(self, '_version'):
            parts.append('v=%s' % self._version)

        return '<{}: {}>'.format(self.__class__.__name__, ', '.join(parts))

    @classmethod
    def get_by_ids(cls, ids, **params):
        query_set = cls.get_collection(**params)
        cls_id = getattr(cls, cls.pk_field())
        return query_set.from_self().filter(cls_id.in_(ids)).limit(len(ids))

    @classmethod
    def get_null_values(cls):
        """ Get null values of :cls: fields. """
        null_values = {}
        columns = cls._mapped_columns()
        columns.update(cls._mapped_relationships())
        for name, col in columns.items():
            if isinstance(col, RelationshipProperty) and col.uselist:
                value = []
            else:
                value = None
            null_values[name] = value
        return null_values

    def to_dict(self, **kwargs):
        native_fields = self.__class__.native_fields()
        _data = {}
        for field in native_fields:
            value = getattr(self, field, None)
            include = field in self._nested_relationships
            if not include:
                get_id = lambda v: getattr(v, v.pk_field(), None)
                if isinstance(value, BaseMixin):
                    value = get_id(value)
                elif isinstance(value, InstrumentedList):
                    value = [get_id(val) for val in value]
            _data[field] = value
        _dict = DataProxy(_data).to_dict(**kwargs)
        _dict['_type'] = self._type
        _dict['_pk'] = str(getattr(self, self.pk_field()))
        return _dict

    def update_iterables(self, params, attr, unique=False,
                         value_type=None, save=True,
                         request=None):
        self._request = request
        mapper = class_mapper(self.__class__)
        columns = {c.name: c for c in mapper.columns}
        is_dict = isinstance(columns.get(attr), DictField)
        is_list = isinstance(columns.get(attr), ListField)

        def split_keys(keys):
            neg_keys, pos_keys = [], []

            for key in keys:
                if key.startswith('__'):
                    continue
                if key.startswith('-'):
                    neg_keys.append(key[1:])
                else:
                    pos_keys.append(key.strip())
            return pos_keys, neg_keys

        def update_dict(update_params):
            final_value = getattr(self, attr, {}) or {}
            final_value = final_value.copy()
            if update_params is None or update_params == '':
                if not final_value:
                    return
                update_params = {
                    '-' + key: val for key, val in final_value.items()}
            positive, negative = split_keys(list(update_params.keys()))

            # Pop negative keys
            for key in negative:
                final_value.pop(key, None)

            # Set positive keys
            for key in positive:
                final_value[str(key)] = update_params[key]

            setattr(self, attr, final_value)
            if save:
                self.save(request)

        def update_list(update_params):
            final_value = getattr(self, attr, []) or []
            final_value = copy.deepcopy(final_value)
            if update_params is None or update_params == '':
                if not final_value:
                    return
                update_params = ['-' + val for val in final_value]
            if isinstance(update_params, dict):
                keys = list(update_params.keys())
            else:
                keys = update_params

            positive, negative = split_keys(keys)

            if not (positive + negative):
                raise JHTTPBadRequest('Missing params')

            if positive:
                if unique:
                    positive = [v for v in positive if v not in final_value]
                final_value += positive

            if negative:
                final_value = list(set(final_value) - set(negative))

            setattr(self, attr, final_value)
            if save:
                self.save(request)

        if is_dict:
            update_dict(params)
        elif is_list:
            update_list(params)

    def get_reference_documents(self):
        # TODO: Make lazy load of documents
        iter_props = class_mapper(self.__class__).iterate_properties
        backref_props = [p for p in iter_props
                         if isinstance(p, properties.RelationshipProperty)]
        for prop in backref_props:
            value = getattr(self, prop.key)
            # Do not index empty values and 'Many' side in OneToMany,
            # when 'One' side is indexed.
            # If 'Many' side should be indexed, its value is already a list.
            if value is None or isinstance(value, list):
                continue
            yield (value.__class__, [value.to_dict()])

    def _is_modified(self):
        """ Determine if instance is modified.

        For instance to be marked as 'modified', it should:
          * Have state marked as modified
          * Have state marked as persistent
          * Any of modified fields have new value
        """
        state = attributes.instance_state(self)
        if state.persistent and state.modified:
            for field in state.committed_state.keys():
                history = state.get_history(field, self)
                if history.added or history.deleted:
                    return True


class BaseDocument(BaseObject, BaseMixin):
    """ Base class for SQLA models.

    Subclasses of this class that do not define a model schema
    should be abstract as well (__abstract__ = True).
    """
    __abstract__ = True

    _version = IntegerField(default=0)

    def _bump_version(self):
        if self._is_modified():
            self._version = (self._version or 0) + 1

    def save(self, request=None):
        session = object_session(self)
        self._bump_version()
        self._request = request
        session = session or Session()
        try:
            self.apply_before_validation()
            session.add(self)
            session.flush()
            session.expire(self)
            self.apply_after_validation()
            return self
        except (IntegrityError,) as e:
            if 'duplicate' not in e.args[0]:
                raise  # Other error, not duplicate

            raise JHTTPConflict(
                detail='Resource `{}` already exists.'.format(
                    self.__class__.__name__),
                extra={'data': e})

    def update(self, params, request=None):
        self._request = request
        try:
            self._update(params)
            self._bump_version()
            self.apply_before_validation()
            session = object_session(self)
            session.add(self)
            session.flush()
            self.apply_after_validation()
            return self
        except (IntegrityError,) as e:
            if 'duplicate' not in e.args[0]:
                raise  # other error, not duplicate

            raise JHTTPConflict(
                detail='Resource `{}` already exists.'.format(
                    self.__class__.__name__),
                extra={'data': e})

    def delete(self, request=None):
        self._request = request
        object_session(self).delete(self)

    def apply_processors(self, column_names=None, before=False, after=False):
        """ Apply processors to columns with :column_names: names.

        Arguments:
          :column_names: List of string names of changed columns.
          :before: Boolean indicating whether to apply before_validation
            processors.
          :after: Boolean indicating whether to apply after_validation
            processors.
        """
        columns = self._mapped_columns()
        columns.update(self._mapped_relationships())

        if column_names is None:
            column_names = columns.keys()

        for name in column_names:
            column = columns.get(name)
            if column is not None and hasattr(column, 'before_validation'):
                new_value = getattr(self, name)
                proc_kwargs = {
                    'new_value': new_value,
                    'instance': self,
                    'field': name,
                    'request': getattr(self, '_request', None),
                }
                processed_value = apply_column_processors(
                    column, before=before, after=after,
                    **proc_kwargs)
                if new_value != processed_value:
                    setattr(self, name, processed_value)

    def apply_before_validation(self):
        """ Determine changed columns and run `self.apply_processors` to
        apply needed processors.

        Note that at this stage, field values are in the exact same state
        you posted/set them. E.g. if you set time_field='11/22/2000',
        self.time_field will be equal to '11/22/2000' here.
        """
        state = attributes.instance_state(self)

        if state.persistent:
            changed_columns = list(state.committed_state.keys())
        else:  # New object
            columns = self._mapped_columns()
            columns.update(self._mapped_relationships())
            changed_columns = list(columns.keys())

        changed_columns = sorted(changed_columns)
        self._columns_to_process = changed_columns
        self.apply_processors(changed_columns, before=True)

    def apply_after_validation(self):
        """ Run `self.apply_processors` with columns names determined by
        `self.apply_before_validation`.

        Note that at this stage, field values are in the exact same state
        you posted/set them. E.g. if you set time_field='11/22/2000',
        self.time_field will be equal to '11/22/2000' here.
        """
        self.apply_processors(self._columns_to_process, after=True)


class ESBaseDocument(six.with_metaclass(ESMetaclass, BaseDocument)):
    """ Base class for SQLA models that use Elasticsearch.

    Subclasses of this class that do not define a model schema
    should be abstract as well (__abstract__ = True).
    """
    __abstract__ = True
