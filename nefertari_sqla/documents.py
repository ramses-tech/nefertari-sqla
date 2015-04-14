import copy
import logging
from datetime import datetime

from sqlalchemy.orm import class_mapper, object_session, properties
from sqlalchemy.orm.collections import InstrumentedList
from sqlalchemy.exc import InvalidRequestError, IntegrityError
from sqlalchemy.orm.exc import MultipleResultsFound, NoResultFound
from pyramid_sqlalchemy import Session, BaseObject

from nefertari.json_httpexceptions import (
    JHTTPBadRequest, JHTTPNotFound, JHTTPConflict)
from nefertari.utils import (
    process_fields, process_limit, _split, dictset,
    DataProxy)
from .signals import ESMetaclass
from .fields import DateTimeField, IntegerField, DictField, ListField

log = logging.getLogger(__name__)


def get_document_cls(name):
    try:
        return BaseObject._decl_class_registry[name]
    except KeyError:
        raise ValueError('SQLAlchemy model `{}` does not exist'.format(name))


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


class BaseMixin(object):
    """ Represents mixin class for models.

    Attributes:
        _auth_fields: String names of fields meant to be displayed to
            authenticated users.
        _public_fields: String names of fields meant to be displayed to
            NOT authenticated users.
        _nested_fields: ?
        _nested_relationships: String names of relationship fields
            that should be included in JSON data of an object as full
            included documents. If relationship field is not
            present in this list, this field's value in JSON will be an
            object's ID or list of IDs.
    """
    _auth_fields = None
    _public_fields = None
    _nested_fields = None
    _nested_relationships = ()

    _type = property(lambda self: self.__class__.__name__)

    @classmethod
    def id_field(cls):
        """ Get a primary key field name. """
        return class_mapper(cls).primary_key[0].name

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
                fields_only = [f for f in fields_only if f not in fields_exclude]
            if fields_only:
                fields_only = [getattr(cls, f) for f in sorted(set(fields_only))]
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
            :params: Query parameters.
        """
        if first:
            params['_limit'] = 1
            params['__raise_on_empty'] = True
        queryset = cls.get_collection(**params)

        id_name = cls.id_field()
        ids = [getattr(obj, id_name, None) for obj in objects]
        ids = [str(id_) for id_ in ids if id_ is not None]
        field_obj = getattr(cls, id_name)
        queryset = queryset.from_self().filter(field_obj.in_(ids))

        if first:
            first_obj = queryset.first()
            if not first_obj:
                msg = "'{}({}={})' resource not found".format(
                    cls.__name__, id_name, params[id_name])
                raise JHTTPNotFound(msg)
            return first_obj

        return queryset

    @classmethod
    def _pop_iterables(cls, params):
        """ Pop iterable fields' parameters from :params: and generate
        SQLA expressions to query the database.

        Iterable values are found by checking what keys from :params:
        correspond to names of Dict/List fields on model.
        In case ListField uses `postgresql.ARRAY` type, value is
        wrapped in list.
        """
        from .fields import ListField, DictField
        iterables = {}
        columns = class_mapper(cls).columns
        columns = {c.name: c for c in columns
                   if isinstance(c, (ListField, DictField))}

        for key, val in params.items():
            suffix = None
            if '.' in key:
                key, suffix = key.split('.')[:2]
            col = columns.get(key)
            if col is None:
                continue

            field_obj = getattr(cls, key)
            is_postgres = getattr(col.type, 'is_postgresql', False)

            if isinstance(col, ListField):
                val = [val] if is_postgres else val
                expr = field_obj.contains(val)

            if isinstance(col, DictField):
                if is_postgres:
                    if suffix is not None:
                        # Check that field contains {suffix: val} pair
                        expr = field_obj.contains({suffix: val})
                    else:
                        # Check that field contains `val` key
                        expr = field_obj.has_key(val)
                else:
                    expr = field_obj.contains(val)

            key = key if suffix is None else '.'.join([key, suffix])
            iterables[key] = expr

        for key in iterables.keys():
            params.pop(key)

        return iterables.values(), params

    @classmethod
    def get_collection(cls, **params):
        """
        params may include '_limit', '_page', '_sort', '_fields'
        returns paginated and sorted query set
        raises JHTTPBadRequest for bad values in params
        """
        log.debug('Get collection: {}, {}'.format(cls.__name__, params))
        params.pop('__confirmation', False)
        __strict = params.pop('__strict', True)

        _sort = _split(params.pop('_sort', []))
        _fields = _split(params.pop('_fields', []))
        _limit = params.pop('_limit', None)
        _page = params.pop('_page', None)
        _start = params.pop('_start', None)

        _count = '_count' in params; params.pop('_count', None)
        _explain = '_explain' in params; params.pop('_explain', None)
        __raise_on_empty = params.pop('__raise_on_empty', False)

        session = Session()

        # Remove any __ legacy instructions from this point on
        params = dictset(filter(lambda item: not item[0].startswith('__'), params.items()))

        iterables_exprs, params = cls._pop_iterables(params)

        if __strict:
            _check_fields = [f.strip('-+') for f in params.keys() + _fields + _sort]
            cls.check_fields_allowed(_check_fields)
        else:
            params = cls.filter_fields(params)

        process_lists(params)
        process_bools(params)

        # If param is _all then remove it
        params.pop_by_values('_all')

        try:
            query_set = session.query(cls).filter_by(**params)

            # Apply filtering by iterable expressions
            for expr in iterables_exprs:
                query_set = query_set.from_self().filter(expr)

            _total = query_set.count()
            if _count:
                return _total

            if _limit is None:
                raise JHTTPBadRequest('Missing _limit')

            _start, _limit = process_limit(_start, _page, _limit)

            # Filtering by fields has to be the first thing to do on the query_set!
            query_set = cls.apply_fields(query_set, _fields)
            query_set = cls.apply_sort(query_set, _sort)
            query_set = query_set.offset(_start).limit(_limit)

            if not query_set.count():
                msg = "'%s(%s)' resource not found" % (cls.__name__, params)
                if __raise_on_empty:
                    raise JHTTPNotFound(msg)
                else:
                    log.debug(msg)

        except (InvalidRequestError,) as e:
            raise JHTTPBadRequest(str(e), extra={'data': e})

        query_sql = str(query_set).replace('\n', '')
        if _explain:
            return query_sql

        log.debug('get_collection.query_set: %s (%s)', cls.__name__, query_sql)

        query_set._nefertari_meta = dict(
            total=_total,
            start=_start,
            fields=_fields)

        return query_set

    @classmethod
    def has_field(cls, field):
        return field in cls.native_fields()

    @classmethod
    def native_fields(cls):
        mapper = class_mapper(cls)
        columns = [c.name for c in mapper.columns]
        relationships = [r.key for r in mapper.relationships]
        return columns + relationships

    @classmethod
    def fields_to_query(cls):
        query_fields = ['id', '_limit', '_page', '_sort', '_fields', '_count', '_start']
        return query_fields + cls.native_fields()

    @classmethod
    def get_resource(cls, **params):
        params.setdefault('__raise_on_empty', True)
        params['_limit'] = 1
        query_set = cls.get_collection(**params)
        return query_set.first()

    @classmethod
    def get(cls, **kw):
        return cls.get_resource(__raise_on_empty=kw.pop('__raise', False), **kw)

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
            query_set.session.add(new_obj)
            query_set.session.flush()
            return new_obj, True
        except MultipleResultsFound:
            raise JHTTPBadRequest('Bad or Insufficient Params')

    def _update(self, params, **kw):
        process_bools(params)
        self.check_fields_allowed(params.keys())
        id_field = self.id_field()
        for key, value in params.items():
            # Can't change PK field
            if key == id_field:
                continue
            setattr(self, key, value)
        session = object_session(self)
        session.add(self)
        session.flush()
        return self

    @classmethod
    def _delete(cls, **params):
        obj = cls.get(**params)
        object_session(obj).delete(obj)

    @classmethod
    def _delete_many(cls, items):
        session = Session()
        for item in items:
            session.delete(item)
        session.flush()

    @classmethod
    def _update_many(cls, items, **params):
        for item in items:
            item._update(params)

    def __repr__(self):
        parts = ['%s:' % self.__class__.__name__]

        if hasattr(self, 'id'):
            parts.append('id=%s' % self.id)

        if hasattr(self, '_version'):
            parts.append('v=%s' % self._version)

        return '<%s>' % ', '.join(parts)

    @classmethod
    def get_by_ids(cls, ids, **params):
        query_set = cls.get_collection(**params)
        cls_id = getattr(cls, cls.id_field())
        return query_set.filter(cls_id.in_(ids)).limit(len(ids))

    def to_dict(self, **kwargs):
        native_fields = self.__class__.native_fields()
        _data = {}
        for field in native_fields:
            value = getattr(self, field, None)
            include = field in self._nested_relationships
            if not include:
                get_id = lambda v: getattr(v, v.id_field(), None)
                if isinstance(value, BaseMixin):
                    value = get_id(value)
                elif isinstance(value, InstrumentedList):
                    value = [get_id(val) for val in value]
            _data[field] = value
        _dict = DataProxy(_data).to_dict(**kwargs)
        _dict['_type'] = self._type
        if not _dict.get('id'):
            _dict['id'] = getattr(self, self.id_field())
        return _dict

    def update_iterables(self, params, attr, unique=False, value_type=None):
        mapper = class_mapper(self.__class__)
        fields = {c.name: c for c in mapper.columns}
        is_dict = isinstance(fields.get(attr), DictField)
        is_list = isinstance(fields.get(attr), ListField)

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

        def update_dict():
            final_value = getattr(self, attr, {}) or {}
            final_value = final_value.copy()
            positive, negative = split_keys(params.keys())

            # Pop negative keys
            for key in negative:
                final_value.pop(key, None)

            # Set positive keys
            for key in positive:
                final_value[unicode(key)] = params[key]
            self.update({attr: final_value})

        def update_list():
            final_value = getattr(self, attr, []) or []
            final_value = copy.deepcopy(final_value)
            positive, negative = split_keys(params.keys())

            if not (positive + negative):
                raise JHTTPBadRequest('Missing params')

            if positive:
                if unique:
                    positive = [v for v in positive if v not in final_value]
                final_value += positive

            if negative:
                final_value = list(set(final_value) - set(negative))

            self.update({attr: final_value})

        if is_dict:
            update_dict()
        elif is_list:
            update_list()

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


class BaseDocument(BaseObject, BaseMixin):
    """ Base class for SQLA models.

    Subclasses of this class that do not define model schema,
    should be abstract as well (__abstract__ = True).
    """
    __abstract__ = True

    updated_at = DateTimeField()
    _version = IntegerField(default=0)

    def _bump_version(self):
        if getattr(self, self.id_field(), None):
            self.updated_at = datetime.utcnow()
            self._version = (self._version or 0) + 1

    def save(self, *arg, **kw):
        session = object_session(self)
        self._bump_version()
        session = session or Session()
        try:
            session.add(self)
            session.flush()
            return self
        except (IntegrityError,) as e:
            if 'duplicate' not in e.message:
                raise  # Other error, not duplicate

            raise JHTTPConflict(
                detail='Resource `%s` already exists.' % self.__class__.__name__,
                extra={'data': e})

    def update(self, params):
        self._bump_version()
        try:
            return self._update(params)
        except (IntegrityError,) as e:
            if 'duplicate' not in e.message:
                raise  # other error, not duplicate

            raise JHTTPConflict(
                detail='Resource `%s` already exists.' % self.__class__.__name__,
                extra={'data': e})


class ESBaseDocument(BaseDocument):
    """ Base class for SQLA models that use Elasticsearch.

    Subclasses of this class that do not define model schema,
    should be abstract as well (__abstract__ = True).
    """
    __abstract__ = True
    __metaclass__ = ESMetaclass
