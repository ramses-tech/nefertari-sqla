import json
import datetime

from sqlalchemy import types
from sqlalchemy.dialects.postgresql import ARRAY


class LengthLimitedStringMixin(object):
    """ Mixin for custom string types which may be length limited. """
    _column_name = None

    def __init__(self, *args, **kwargs):
        self.min_length = kwargs.pop('min_length', None)
        self.max_length = kwargs.pop('max_length', None)
        if ('length' not in kwargs) and self.max_length:
            kwargs['length'] = self.max_length
        super(LengthLimitedStringMixin, self).__init__(*args, **kwargs)

    def process_bind_param(self, value, dialect):
        if value is not None:
            if (self.min_length is not None) and len(value) < self.min_length:
                raise ValueError(
                    'Field `{}`: Value length must be more than {}'.format(
                        self._column_name, self.min_length))
            if (self.max_length is not None) and len(value) > self.max_length:
                raise ValueError(
                    'Field `{}`: Value length must be less than {}'.format(
                        self._column_name, self.max_length))
        return value


class SizeLimitedNumberMixin(object):
    """ Mixin for custom string types which may be size limited. """
    _column_name = None

    def __init__(self, *args, **kwargs):
        self.min_value = kwargs.pop('min_value', None)
        self.max_value = kwargs.pop('max_value', None)
        super(SizeLimitedNumberMixin, self).__init__(*args, **kwargs)

    def process_bind_param(self, value, dialect):
        if value is None:
            return value
        if (self.min_value is not None) and value < self.min_value:
            raise ValueError(
                'Field `{}`: Value must be bigger than {}'.format(
                    self._column_name, self.min_value))
        if (self.max_value is not None) and value > self.max_value:
            raise ValueError(
                'Field `{}`: Value must be less than {}'.format(
                    self._column_name, self.max_value))
        return value


class LimitedString(LengthLimitedStringMixin, types.TypeDecorator):
    """ String type, min and max length limits. """
    impl = types.String


class LimitedText(LengthLimitedStringMixin, types.TypeDecorator):
    """ Text type, min and max length limits. """
    impl = types.Text


class LimitedUnicode(LengthLimitedStringMixin, types.TypeDecorator):
    """ Unicode type, min and max length limits. """
    impl = types.Unicode


class LimitedUnicodeText(LengthLimitedStringMixin, types.TypeDecorator):
    """ UnicideText type, min and max length limits. """
    impl = types.UnicodeText


class LimitedBigInteger(SizeLimitedNumberMixin, types.TypeDecorator):
    """ BigInteger type, min and max value if which may be limited. """
    impl = types.BigInteger


class LimitedInteger(SizeLimitedNumberMixin, types.TypeDecorator):
    """ Integer type, min and max value if which may be limited. """
    impl = types.Integer


class LimitedSmallInteger(SizeLimitedNumberMixin, types.TypeDecorator):
    """ SmallInteger type, min and max value if which may be limited. """
    impl = types.SmallInteger


class LimitedFloat(SizeLimitedNumberMixin, types.TypeDecorator):
    """ Float type, min and max value if which may be limited. """
    impl = types.Float


class LimitedNumeric(SizeLimitedNumberMixin, types.TypeDecorator):
    """ Numeric type, min and max value if which may be limited. """
    impl = types.Numeric


class DateTime(types.TypeDecorator):
    impl = types.DateTime


class Boolean(types.TypeDecorator):
    impl = types.Boolean


class Date(types.TypeDecorator):
    impl = types.Date


class Choice(types.TypeDecorator):
    """ Type that represents value from a particular set of choices.

    Value may be any number of choices from a provided set of
    valid choices.
    """
    _column_name = None
    impl = types.String

    def __init__(self, *args, **kwargs):
        self.choices = kwargs.pop('choices', ())
        if not isinstance(self.choices, (list, tuple, set)):
            self.choices = [self.choices]
        super(Choice, self).__init__(*args, **kwargs)

    def process_bind_param(self, value, dialect):
        if (value is not None) and (value not in self.choices):
            err = 'Field `{}`: Got an invalid choice `{}`. Valid choices: ({})'
            err_ctx = [self._column_name, value, ', '.join(self.choices)]
            raise ValueError(err.format(*err_ctx))
        return value


class Interval(types.TypeDecorator):
    impl = types.Interval

    def process_bind_param(self, value, dialect):
        """ Convert seconds(int) :value: to `datetime.timedelta` instance. """
        if isinstance(value, int):
            value = datetime.timedelta(seconds=value)
        return value


class LargeBinary(types.TypeDecorator):
    impl = types.LargeBinary


class PickleType(types.TypeDecorator):
    impl = types.PickleType


class Time(types.TypeDecorator):
    impl = types.Time


class ChoiceArray(types.TypeDecorator):
    """ Represents a list of values.

    If 'postgresql' is used, postgress.ARRAY type is used for db column
    type. Otherwise `UnicodeText` is used.

    Supports providing :choices: argument which limits the set of values
    that may be stored in this field.
    """
    _column_name = None
    impl = ARRAY

    def __init__(self, *args, **kwargs):
        self.choices = kwargs.pop('choices', None)
        if self.choices is not None and not isinstance(
                self.choices, (list, tuple, set)):
            self.choices = [self.choices]
        self.kwargs = kwargs
        super(ChoiceArray, self).__init__(*args, **kwargs)

    def load_dialect_impl(self, dialect):
        """ Based on :dialect.name: determine type to be used.

        `postgresql.ARRAY` is used in case `postgresql` database is used.
        Otherwise `types.UnicodeText` is used.
        """
        if dialect.name == 'postgresql':
            self.is_postgresql = True
            return dialect.type_descriptor(ARRAY(**self.kwargs))
        else:
            self.is_postgresql = False
            self.kwargs.pop('item_type', None)
            return dialect.type_descriptor(types.UnicodeText(**self.kwargs))

    def _validate_choices(self, value):
        """ Perform :value: validation checking if its items are contained
        in :self.choices:
        """
        if self.choices is None or value is None:
            return value

        invalid_choices = set(value) - set(self.choices)
        if invalid_choices:
            err = 'Field `{}`: Got invalid choices: ({}). Valid choices: ({})'
            err_ctx = [self._column_name, ', '.join(invalid_choices),
                       ', '.join(self.choices)]
            raise ValueError(err.format(*err_ctx))
        return value

    def process_bind_param(self, value, dialect):
        value = self._validate_choices(value)
        if dialect.name == 'postgresql':
            return value
        if value is not None:
            value = json.dumps(value)
        return value

    def process_result_value(self, value, dialect):
        if dialect.name == 'postgresql':
            return value
        if value is not None:
            value = json.loads(value)
        return value
