from sqlalchemy.orm import backref, relationship
from sqlalchemy.schema import Column, ForeignKey
from sqlalchemy_utils.types.json import JSONType

# Since SQLAlchemy 1.0.0
# from sqlalchemy.types import MatchType
from .types import (
    LimitedString,
    LimitedText,
    LimitedUnicode,
    LimitedBigInteger,
    LimitedInteger,
    LimitedSmallInteger,
    LimitedFloat,
    LimitedNumeric,
    LimitedUnicodeText,
    DateTime,
    Boolean,
    Date,
    Interval,
    LargeBinary,
    PickleType,
    Time,
    Choice,
    ChoiceArray,
)


class BaseField(Column):
    """ Base plain column that otherwise would be created as
    sqlalchemy.Column(sqlalchemy.Type())

    Attributes:
        _sqla_type_cls: SQLAlchemy type class used to instantiate the column
            type.
        _type_unchanged_kwargs: sequence of strings that represent arguments
            received by `_sqla_type_cls`, the names of which have not been
            changed. Values of field init arguments with these names will
            be extracted from field init kwargs and passed to Type init
            as is.
        _column_valid_kwargs: sequence of string names of valid kwargs that
            a Column may receive.
    """
    _sqla_type_cls = None
    _type_unchanged_kwargs = ()
    _column_valid_kwargs = (
        'name', 'type_', 'autoincrement', 'default', 'doc', 'key', 'index',
        'info', 'nullable', 'onupdate', 'primary_key', 'server_default',
        'server_onupdate', 'quote', 'unique', 'system', '_proxies')

    def __init__(self, *args, **kwargs):
        """ Responsible for:
        * Filter out type-specific kwargs and init Type using these.
        * Filter out column-slecific kwargs and init column using them.
        * If `args` are provided, that means column proxy is being created.
          In this case Type does not need to be created.
        """
        if not hasattr(self, '_kwargs_backup'):
            self._kwargs_backup = kwargs.copy()

        type_args, type_kw, cleaned_kw = self.process_type_args(kwargs)
        col_kw = self.process_column_args(cleaned_kw)
        # Column proxy is created by declarative extension
        if args:
            col_kw['name'], col_kw['type_'] = args
        # Column init when defining a schema
        else:
            col_kw['type_'] = self._sqla_type_cls(*type_args, **type_kw)
            if 'type_' not in kwargs:
                self._init_kwargs = self._kwargs_backup.copy()
        super(BaseField, self).__init__(**col_kw)

    def __setattr__(self, key, value):
        """ Store column name on 'self.type'

        This allows error messages in custom types' validation be more
        explicit.
        """
        if value is not None and key == 'name':
            self.type._column_name = value
        return super(BaseField, self).__setattr__(key, value)

    def process_type_args(self, kwargs):
        """ Process arguments of a sqla Type.

        http://docs.sqlalchemy.org/en/rel_0_9/core/type_basics.html#generic-types

        Process `kwargs` to extract type-specific arguments.
        If some arguments' names should be changed, extend this method
        with a manual args processor.

        Returns:
            * type_args: sequence of type-specific posional arguments
            * type_kw: dict of type-specific kwargs
            * cleaned_kw: input kwargs cleaned from type-specific args
        """
        type_kw = dict()
        type_args = ()
        cleaned_kw = kwargs.copy()
        for arg in self._type_unchanged_kwargs:
            if arg in cleaned_kw:
                type_kw[arg] = cleaned_kw.pop(arg)
        return type_args, type_kw, cleaned_kw

    def _drop_invalid_kwargs(self, kwargs):
        """ Drop keys from `kwargs` that are not present in
        `self._column_valid_kwargs`, thus are not valid kwargs to
        be passed to Column.
        """
        return {k: v for k, v in kwargs.items() if
                k in self._column_valid_kwargs}

    def process_column_args(self, kwargs):
        """ Process/extract/rename Column arguments.

        http://docs.sqlalchemy.org/en/rel_0_9/core/metadata.html#column-table-metadata-api

        Changed:
            required -> nullable
            help_text -> doc
        """
        col_kw = kwargs.copy()
        col_kw['nullable'] = not col_kw.pop('required', False)
        col_kw['doc'] = col_kw.pop('help_text', None)
        col_kw = self._drop_invalid_kwargs(col_kw)
        return col_kw

    def _constructor(self, *args, **kwargs):
        """ Update default Column kwargs with missing kwargs from
        first init to supports non-standard kwargs.
        """
        additional_kw = {
            key: val for key, val in self._kwargs_backup.items()
            if not kwargs.get(key)}
        kwargs.update(additional_kw)
        obj = self.__class__(*args, **kwargs)
        obj._init_kwargs = self._init_kwargs
        return obj


class BigIntegerField(BaseField):
    _sqla_type_cls = LimitedBigInteger
    _type_unchanged_kwargs = ('min_value', 'max_value')


class BooleanField(BaseField):
    _sqla_type_cls = Boolean
    _type_unchanged_kwargs = ('create_constraint')

    def process_type_args(self, kwargs):
        """
        Changed:
            constraint_name -> name
        """
        type_args, type_kw, cleaned_kw = super(
            BooleanField, self).process_type_args(kwargs)
        type_kw.update({
            'name': cleaned_kw.pop('constraint_name', None),
        })
        return type_args, type_kw, cleaned_kw


class DateField(BaseField):
    _sqla_type_cls = Date
    _type_unchanged_kwargs = ()


class DateTimeField(BaseField):
    _sqla_type_cls = DateTime
    _type_unchanged_kwargs = ('timezone',)


class ChoiceField(BaseField):
    _sqla_type_cls = Choice
    _type_unchanged_kwargs = (
        'collation', 'convert_unicode', 'unicode_error',
        '_warn_on_bytestring', 'choices')


class FloatField(BaseField):
    _sqla_type_cls = LimitedFloat
    _type_unchanged_kwargs = (
        'precision', 'asdecimal', 'decimal_return_scale',
        'min_value', 'max_value')


class IntegerField(BaseField):
    _sqla_type_cls = LimitedInteger
    _type_unchanged_kwargs = ('min_value', 'max_value')


class IdField(IntegerField):
    """ Just a subclass of IntegerField that must be used for fields
    that represent database-specific 'id' field.
    """
    pass


class IntervalField(BaseField):
    _sqla_type_cls = Interval
    _type_unchanged_kwargs = (
        'native', 'second_precision', 'day_precision')


class BinaryField(BaseField):
    _sqla_type_cls = LargeBinary
    _type_unchanged_kwargs = ('length',)

# Since SQLAlchemy 1.0.0
# class MatchField(BooleanField):
#     _sqla_type_cls = MatchType


class DecimalField(BaseField):
    _sqla_type_cls = LimitedNumeric
    _type_unchanged_kwargs = (
        'precision', 'scale', 'decimal_return_scale', 'asdecimal',
        'min_value', 'max_value')


class PickleField(BaseField):
    _sqla_type_cls = PickleType
    _type_unchanged_kwargs = (
        'protocol', 'pickler', 'comparator')


class SmallIntegerField(BaseField):
    _sqla_type_cls = LimitedSmallInteger
    _type_unchanged_kwargs = ('min_value', 'max_value')


class StringField(BaseField):
    _sqla_type_cls = LimitedString
    _type_unchanged_kwargs = (
        'collation', 'convert_unicode', 'unicode_error',
        '_warn_on_bytestring', 'min_length', 'max_length')

    def process_type_args(self, kwargs):
        """
        Changed:
            max_length -> length
        """
        type_args, type_kw, cleaned_kw = super(
            StringField, self).process_type_args(kwargs)
        type_kw.update({
            'length': type_kw.get('max_length'),
        })
        return type_args, type_kw, cleaned_kw


class TextField(StringField):
    _sqla_type_cls = LimitedText


class TimeField(DateTimeField):
    _sqla_type_cls = Time


class UnicodeField(StringField):
    _sqla_type_cls = LimitedUnicode


class UnicodeTextField(StringField):
    _sqla_type_cls = LimitedUnicodeText


class DictField(BaseField):
    _sqla_type_cls = JSONType
    _type_unchanged_kwargs = ()

    def process_type_args(self, kwargs):
        type_args, type_kw, cleaned_kw = super(
            DictField, self).process_type_args(kwargs)
        cleaned_kw['default'] = cleaned_kw.get('default') or {}
        return type_args, type_kw, cleaned_kw


class ListField(BaseField):
    _sqla_type_cls = ChoiceArray
    _type_unchanged_kwargs = (
        'as_tuple', 'dimensions', 'zero_indexes', 'choices')

    def process_type_args(self, kwargs):
        """ Covert field class to its `_sqla_type_cls`.

        StringField & UnicodeField are replaced with corresponding
        Text fields because when String* fields are used, SQLA creates
        db column of postgresql type 'varying[]'. But when querying that
        column with text, requested text if submited as 'text[]'.

        Changed:
            item_type field class -> item_type field type
        """
        type_args, type_kw, cleaned_kw = super(
            ListField, self).process_type_args(kwargs)

        if 'item_type' in cleaned_kw:
            item_type_field = cleaned_kw['item_type']

            if item_type_field is StringField:
                item_type_field = TextField
            if item_type_field is UnicodeField:
                item_type_field = UnicodeTextField

            type_kw['item_type'] = item_type_field._sqla_type_cls

        cleaned_kw['default'] = cleaned_kw.get('default') or []

        return type_args, type_kw, cleaned_kw


class BaseSchemaItemField(BaseField):
    """ Base class for fields/columns that accept a schema item/constraint
    on column init. E.g. Column(Integer, ForeignKey('user.id'))

    It differs from regular columns in that an item/constraint passed to the
    Column on init has to be passed as a positional argument and should
    also receive arguments. Thus 3 objects need to be created on init:
    Column, Type, and SchemaItem/Constraint.

    Attributes:
        _schema_class: Class to be instantiated to create a schema item.
        _schema_kwarg_prefix: Prefix schema item's kwargs should have. This
            is used to avoid making a mess, as both column, type and schemaitem
            kwargs may be passed at once.
        _schema_valid_kwargs: Sequence of strings that represent names of
            kwargs `_schema_class` may receive. Should not include prefix.
    """
    _schema_class = None
    _schema_kwarg_prefix = ''
    _schema_valid_kwargs = ()

    def __init__(self, *args, **kwargs):
        """ Responsible for:
        * Filter out type-specific kwargs and init Type using these.
        * Filter out `_schema_class` kwargs and init `_schema_class`.
        * Filter out column-slecific kwargs and init column using them.
        * If `args` are provided, that means column proxy is being created.
          In this case Type does not need to be created.
        """
        if not hasattr(self, '_kwargs_backup'):
            self._kwargs_backup = kwargs.copy()

        type_args, type_kw, cleaned_kw = self.process_type_args(kwargs)
        if not args:
            schema_item, cleaned_kw = self._generate_schema_item(cleaned_kw)
        column_kw = self.process_column_args(cleaned_kw)
        # Column proxy is created by declarative extension
        if args:
            column_kw['name'], column_kw['type_'], schema_item = args
        # Column init when defining a schema
        else:
            column_kw['type_'] = self._sqla_type_cls(*type_args, **type_kw)
            if 'type_' not in kwargs:
                self._init_kwargs = self._kwargs_backup.copy()
        column_args = (schema_item,)
        return Column.__init__(self, *column_args, **column_kw)

    def _generate_schema_item(self, cleaned_kw):
        """ Generate SchemaItem using `_schema_class` and kwargs
        filtered out from `cleaned_kw`.
        Returns created instance and cleaned kwargs.
        """
        schema_kwargs = {}
        for key in self._schema_valid_kwargs:
            prefixed_key = self._schema_kwarg_prefix + key
            if prefixed_key in cleaned_kw:
                schema_kwargs[key] = cleaned_kw.pop(prefixed_key)
        schema_item = self._schema_class(**schema_kwargs)
        return schema_item, cleaned_kw


class ForeignKeyField(BaseSchemaItemField):
    """ Integer ForeignKey field.

    This is the place where `ondelete` rules kwargs should be passed.
    If you switched from the mongodb engine, copy the same `ondelete`
    rules you passed to mongo's `Relationship` constructor.

    `ondelete` kwargs may be kept in both fields with no side-effects
    when switching between the sqla and mongo engines.

    Developers are not encouraged to change the value of this field on
    model to add/update relationship. Use `Relationship` constructor
    with backreference settings instead.
    """
    _sqla_type_cls = None
    _type_unchanged_kwargs = ()
    _schema_class = ForeignKey
    _schema_kwarg_prefix = 'ref_'
    _schema_valid_kwargs = (
        'column', '_constraint', 'use_alter', 'name', 'onupdate',
        'ondelete', 'deferrable', 'initially', 'link_to_name', 'match')

    def __init__(self, *args, **kwargs):
        """ Override to determine `self._sqla_type_cls`.

        Type is determined using 'ref_column_type' value from :kwargs:.
        Its value must be a *Field class of a field that is being
        referenced by FK field or a `_sqla_type_cls` of that *Field cls.
        """
        if not args:
            field_type = kwargs.pop(self._schema_kwarg_prefix + 'column_type')
            if hasattr(field_type, '_sqla_type_cls'):
                field_type = field_type._sqla_type_cls
            self._sqla_type_cls = field_type
        super(ForeignKeyField, self).__init__(*args, **kwargs)

    def _get_referential_action(self, kwargs, key):
        """ Determine/translate generic rule name to SQLA-specific rule.

        Output rule name is a valid SQL Referential action name.
        If `ondelete` kwarg is not provided, no referential action will be
        created.

        Valid kwargs for `ondelete` kwarg are:
            CASCADE     Translates to SQL as `CASCADE`
            RESTRICT    Translates to SQL as `RESTRICT`
            NULLIFY     Translates to SQL as `SET NULL

        Not supported SQL referential actions: `NO ACTION`, `SET DEFAULT`
        """
        key = self._schema_kwarg_prefix + key
        action = kwargs.pop(key, None)
        if action is None:
            return action
        rules = {
            'CASCADE': 'CASCADE',
            'RESTRICT': 'RESTRICT',
            'NULLIFY': 'SET NULL',
        }
        action = action.upper()
        if action not in rules:
            raise KeyError('Invalid `{}` argument value. Must be '
                           'one of: {}'.format(key, ', '.join(rules.keys())))
        return rules[action]

    def _generate_schema_item(self, cleaned_kw):
        """ Override default implementation to generate 'ondelete' and
        'onupdate' arguments.
        """
        pref = self._schema_kwarg_prefix
        cleaned_kw[pref + 'ondelete'] = self._get_referential_action(
            cleaned_kw, 'ondelete')
        cleaned_kw[pref + 'onupdate'] = self._get_referential_action(
            cleaned_kw, 'onupdate')
        return super(ForeignKeyField, self)._generate_schema_item(cleaned_kw)


relationship_kwargs = {
    'secondary', 'primaryjoin', 'secondaryjoin',
    'foreign_keys', 'uselist', 'order_by',
    'backref', 'back_populates', 'post_update',
    'cascade', 'extension', 'viewonly',
    'lazy', 'collection_class', 'passive_deletes',
    'passive_updates', 'remote_side', 'enable_typechecks',
    'join_depth', 'comparator_factory', 'single_parent',
    'innerjoin', 'distinct_target_key', 'doc',
    'active_history', 'cascade_backrefs', 'load_on_pending',
    'strategy_class', '_local_remote_pairs', 'query_class', 'info',
    'document', 'name',
}


def Relationship(**kwargs):
    """ Thin wrapper around relationship.

    The goal of this wrapper is to allow passing both relationship and
    backref arguments to a single function.
    Backref arguments should be prefixed with 'backref_'.
    This function splits relationship-specific and backref-specific arguments
    and makes a call like:
        relationship(..., ..., backref=backref(...))

    :lazy: setting is set to 'immediate' on the 'One' side of One2One or
    One2Many relationships. This is done both for relationship itself
    and backref so ORM 'after_update' events are fired when relationship
    is updated. For backref 'uselist' is assumed to be False by default.

    From SQLAlchemy docs: immediate - items should be loaded as the parents
    are loaded, using a separate SELECT statement, or identity map fetch for
    simple many-to-one references.
    """
    _init_kwargs = kwargs.copy()
    backref_pre = 'backref_'
    backref_pre_len = len(backref_pre)
    if 'help_text' in kwargs:
        kwargs['doc'] = kwargs.pop('help_text', None)
    if (backref_pre + 'help_text') in kwargs:
        kwargs[backref_pre + 'doc'] = kwargs.pop(
            backref_pre + 'help_text', None)

    kwargs = {k: v for k, v in kwargs.items()
              if k in relationship_kwargs
              or k[backref_pre_len:] in relationship_kwargs}
    rel_kw, backref_kw = {}, {}

    for key, val in kwargs.items():
        if key.startswith(backref_pre):
            key = key[backref_pre_len:]
            backref_kw[key] = val
        else:
            rel_kw[key] = val

    rel_document = rel_kw.pop('document')
    if 'uselist' in rel_kw and not rel_kw['uselist']:
        rel_kw['lazy'] = 'immediate'

    if backref_kw:
        if not backref_kw.get('uselist'):
            backref_kw['lazy'] = 'immediate'
        backref_name = backref_kw.pop('name')
        rel_kw['backref'] = backref(backref_name, **backref_kw)

    rel_field = relationship(rel_document, **rel_kw)
    rel_field._init_kwargs = _init_kwargs
    return rel_field
