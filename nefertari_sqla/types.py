import json
import datetime

from sqlalchemy import types
from sqlalchemy.dialects.postgresql import ARRAY, HSTORE


class ProcessableMixin(object):
    """ Mixin that allows running callables on a value that
    is being set to a field.
    """
    def __init__(self, *args, **kwargs):
        self.processors = kwargs.pop('processors', ())
        super(ProcessableMixin, self).__init__(*args, **kwargs)

    def process_bind_param(self, value, dialect):
        for proc in self.processors:
            value = proc(value)
        return value


class LengthLimitedStringMixin(ProcessableMixin):
    """ Mixin for custom string types which may be length limited. """
    def __init__(self, *args, **kwargs):
        self.min_length = kwargs.pop('min_length', None)
        self.max_length = kwargs.pop('max_length', None)
        if ('length' not in kwargs) and self.max_length:
            kwargs['length'] = self.max_length
        super(LengthLimitedStringMixin, self).__init__(*args, **kwargs)

    def process_bind_param(self, value, dialect):
        value = super(LengthLimitedStringMixin, self).process_bind_param(
            value, dialect)
        if value is not None:
            if (self.min_length is not None) and len(value) < self.min_length:
                raise ValueError('Value length must be more than {}'.format(
                    self.min_length))
            if (self.max_length is not None) and len(value) > self.max_length:
                raise ValueError('Value length must be less than {}'.format(
                    self.max_length))
        return value


class SizeLimitedNumberMixin(ProcessableMixin):
    """ Mixin for custom string types which may be size limited. """
    def __init__(self, *args, **kwargs):
        self.min_value = kwargs.pop('min_value', None)
        self.max_value = kwargs.pop('max_value', None)
        super(SizeLimitedNumberMixin, self).__init__(*args, **kwargs)

    def process_bind_param(self, value, dialect):
        value = super(SizeLimitedNumberMixin, self).process_bind_param(
            value, dialect)
        if value is None:
            return value
        if (self.min_value is not None) and value < self.min_value:
            raise ValueError('Value must be bigger than {}'.format(
                self.min_value))
        if (self.max_value is not None) and value > self.max_value:
            raise ValueError('Value must be less than {}'.format(
                self.max_value))
        return value


class LimitedString(LengthLimitedStringMixin, types.TypeDecorator):
    """ String type, min and max length if which may be limited. """
    impl = types.String


class LimitedText(LengthLimitedStringMixin, types.TypeDecorator):
    """ Text type, min and max length if which may be limited. """
    impl = types.Text


class LimitedUnicode(LengthLimitedStringMixin, types.TypeDecorator):
    """ Unicode type, min and max length if which may be limited. """
    impl = types.Unicode


class LimitedUnicodeText(LengthLimitedStringMixin, types.TypeDecorator):
    """ UnicideText type, min and max length if which may be limited. """
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


# Types that support running processors

class ProcessableDateTime(ProcessableMixin, types.TypeDecorator):
    impl = types.DateTime


class ProcessableBoolean(ProcessableMixin, types.TypeDecorator):
    impl = types.Boolean


class ProcessableDate(ProcessableMixin, types.TypeDecorator):
    impl = types.Date


class ProcessableChoice(ProcessableMixin, types.TypeDecorator):
    """ Type that represents value from a particular set of choices.

    Value may be any number of choices from a provided set of
    valid choices.
    """
    impl = types.String

    def __init__(self, *args, **kwargs):
        self.choices = kwargs.pop('choices', None)
        if not isinstance(self.choices, (list, tuple, list)):
            self.choices = [self.choices]
        super(ProcessableChoice, self).__init__(*args, **kwargs)

    def process_bind_param(self, value, dialect):
        value = super(ProcessableChoice, self).process_bind_param(
            value, dialect)
        if (value is not None) and (value not in self.choices):
            err = 'Got an invalid choice `{}`. Valid choices: ({})'.format(
                value, ', '.join(self.choices))
            raise ValueError(err)
        return value


class ProcessableInterval(ProcessableMixin, types.TypeDecorator):
    impl = types.Interval

    def process_bind_param(self, value, dialect):
        """ Convert seconds(int) :value: to `datetime.timedelta` instance. """
        value = super(ProcessableInterval, self).process_bind_param(
            value, dialect)
        if isinstance(value, int):
            value = datetime.timedelta(seconds=value)
        return value


class ProcessableLargeBinary(ProcessableMixin, types.TypeDecorator):
    impl = types.LargeBinary


class ProcessablePickleType(ProcessableMixin, types.TypeDecorator):
    impl = types.PickleType


class ProcessableTime(ProcessableMixin, types.TypeDecorator):
    impl = types.Time


class ProcessableDict(ProcessableMixin, types.TypeDecorator):
    """ Represents a dictionary of values.


    If 'postgresql' is used, postgress.HSTORE type is used for db column
    type. Otherwise `UnicodeText` is used.
    """
    impl = HSTORE

    def load_dialect_impl(self, dialect):
        """ Based on :dialect.name: determine type to be used.

        `postgresql.HSTORE` is used in case `postgresql` database is used.
        Otherwise `types.UnicodeText` is used.
        """
        if dialect.name == 'postgresql':
            self.is_postgresql = True
            return dialect.type_descriptor(HSTORE)
        else:
            return dialect.type_descriptor(types.UnicodeText)

    def process_bind_param(self, value, dialect):
        value = super(ProcessableDict, self).process_bind_param(
            value, dialect)
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


class ProcessableChoiceArray(ProcessableMixin, types.TypeDecorator):
    """ Represents a list of values.

    If 'postgresql' is used, postgress.ARRAY type is used for db column
    type. Otherwise `UnicodeText` is used.

    Supports providing :choices: argument which limits the set of values
    that may be stored in this field.
    """
    impl = ARRAY

    def __init__(self, *args, **kwargs):
        self.kwargs = kwargs
        self.choices = kwargs.pop('choices', ()) or ()
        if not isinstance(self.choices, (list, tuple, list)):
            self.choices = [self.choices]
        super(ProcessableChoiceArray, self).__init__(*args, **kwargs)

    def load_dialect_impl(self, dialect):
        """ Based on :dialect.name: determine type to be used.

        `postgresql.ARRAY` is used in case `postgresql` database is used.
        Otherwise `types.UnicodeText` is used.
        """
        if dialect.name == 'postgresql':
            self.is_postgresql = True
            return dialect.type_descriptor(ARRAY(**self.kwargs))
        else:
            self.kwargs.pop('item_type', None)
            return dialect.type_descriptor(types.UnicodeText(**self.kwargs))

    def _validate_choices(self, value):
        """ Perform :value: validation checking if its items are contained
        in :self.choices:
        """
        if not self.choices:
            return value
        if value is not None:
            invalid_choices = set(value) - set(self.choices)
            if invalid_choices:
                raise ValueError(
                    'Got invalid choices: ({}). Valid choices: ({})'.format(
                        ', '.join(invalid_choices), ', '.join(self.choices)))
        return value

    def process_bind_param(self, value, dialect):
        value = super(ProcessableChoiceArray, self).process_bind_param(
            value, dialect)
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
