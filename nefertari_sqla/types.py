from sqlalchemy import types
from sqlalchemy_utils.types.json import JSONType


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
        value_len = 0 if value is None else len(value)
        if (self.min_length is not None) and value_len < self.min_length:
            raise ValueError('Value length must be more than {}'.format(
                self.min_length))
        if (self.max_length is not None) and value_len > self.max_length:
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
    """ Type that represents a list of values.

    Values may be any number of choices from a provided set of
    valid choices.
    """
    impl = types.String

    def __init__(self, *args, **kwargs):
        self.sequence_types = (tuple, list, set)
        self.choices = kwargs.pop('choices', None)
        if not isinstance(self.choices, self.sequence_types):
            raise Exception('Choices must be a sequence of type: {}. Got `{}`'.format(
                self.sequence_types, type(self.choices)))
        self.choices = set(self.choices)
        super(ProcessableChoice, self).__init__(*args, **kwargs)

    def process_bind_param(self, value, dialect):
        value = super(ProcessableChoice, self).process_bind_param(value, dialect)
        if (value is not None) and (value not in self.choices):
            err = 'Got an invalid choice `{}`. Valid choices: ({})'.format(
                value, ', '.join(self.choices))
            raise ValueError(err)
        return value


class ProcessableInterval(ProcessableMixin, types.TypeDecorator):
    impl = types.Interval


class ProcessableLargeBinary(ProcessableMixin, types.TypeDecorator):
    impl = types.LargeBinary


class ProcessablePickleType(ProcessableMixin, types.TypeDecorator):
    impl = types.PickleType


class ProcessableTime(ProcessableMixin, types.TypeDecorator):
    impl = types.Time


class ProcessableJSON(ProcessableMixin, types.TypeDecorator):
    impl = JSONType
