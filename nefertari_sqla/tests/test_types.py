import datetime

import pytest
from mock import patch, Mock
from pyramid.security import (
    Allow, Deny, Everyone, Authenticated, ALL_PERMISSIONS)

from .. import fields
from .. import types
from .fixtures import memory_db, db_session, simple_model


class DemoClass(object):
    def __init__(self, *args, **kwargs):
        pass


class TestLengthLimitedStringMixin(object):

    class Limited(types.LengthLimitedStringMixin, DemoClass):
        _column_name = 'foo'

    def test_none_value(self):
        mixin = self.Limited(min_length=5)
        try:
            mixin.process_bind_param(None, None)
        except ValueError:
            raise Exception('Unexpected exception')

    def test_min_length(self):
        mixin = self.Limited(min_length=5)
        with pytest.raises(ValueError) as ex:
            mixin.process_bind_param('q', None)
        assert str(ex.value) == (
            'Field `foo`: Value length must be more than 5')
        try:
            mixin.process_bind_param('asdasdasd', None)
        except ValueError:
            raise Exception('Unexpected exception')

    def test_max_length(self):
        mixin = self.Limited(max_length=5)
        with pytest.raises(ValueError) as ex:
            mixin.process_bind_param('asdasdasdasdasd', None)
        assert str(ex.value) == (
            'Field `foo`: Value length must be less than 5')
        try:
            mixin.process_bind_param('q', None)
        except ValueError:
            raise Exception('Unexpected exception')

    def test_min_and_max_length(self):
        mixin = self.Limited(max_length=5, min_length=2)
        with pytest.raises(ValueError) as ex:
            mixin.process_bind_param('a', None)
        assert str(ex.value) == (
            'Field `foo`: Value length must be more than 2')
        with pytest.raises(ValueError) as ex:
            mixin.process_bind_param('a12313123123', None)
        assert str(ex.value) == (
            'Field `foo`: Value length must be less than 5')
        try:
            mixin.process_bind_param('12q', None)
        except ValueError:
            raise Exception('Unexpected exception')


class TestSizeLimitedNumberMixin(object):

    class Limited(types.SizeLimitedNumberMixin, DemoClass):
        _column_name = 'foo'

    def test_none_value(self):
        mixin = self.Limited(min_value=5)
        try:
            mixin.process_bind_param(None, None)
        except ValueError:
            raise Exception('Unexpected exception')

    def test_min_value(self):
        mixin = self.Limited(min_value=5)
        with pytest.raises(ValueError) as ex:
            mixin.process_bind_param(1, None)
        assert str(ex.value) == (
            'Field `foo`: Value must be bigger than 5')
        try:
            mixin.process_bind_param(10, None)
        except ValueError:
            raise Exception('Unexpected exception')

    def test_max_value(self):
        mixin = self.Limited(max_value=5)
        with pytest.raises(ValueError) as ex:
            mixin.process_bind_param(10, None)
        assert str(ex.value) == (
            'Field `foo`: Value must be less than 5')
        try:
            mixin.process_bind_param(1, None)
        except ValueError:
            raise Exception('Unexpected exception')

    def test_min_and_max_value(self):
        mixin = self.Limited(max_value=5, min_value=2)
        with pytest.raises(ValueError) as ex:
            mixin.process_bind_param(1, None)
        assert str(ex.value) == (
            'Field `foo`: Value must be bigger than 2')
        with pytest.raises(ValueError) as ex:
            mixin.process_bind_param(10, None)
        assert str(ex.value) == (
            'Field `foo`: Value must be less than 5')
        try:
            mixin.process_bind_param(3, None)
        except ValueError:
            raise Exception('Unexpected exception')


class TestChoice(object):

    def test_no_choices(self):
        field = types.Choice()
        field._column_name = 'foo'
        with pytest.raises(ValueError) as ex:
            field.process_bind_param('foo', None)
        assert str(ex.value) == \
            'Field `foo`: Got an invalid choice `foo`. Valid choices: ()'

    def test_none_value(self):
        field = types.Choice()
        try:
            field.process_bind_param(None, None)
        except ValueError:
            raise Exception('Unexpected error')

    def test_value_not_in_choices(self):
        field = types.Choice(choices=['foo'])
        field._column_name = 'foo'
        with pytest.raises(ValueError) as ex:
            field.process_bind_param('bar', None)
        assert str(ex.value) == \
            'Field `foo`: Got an invalid choice `bar`. Valid choices: (foo)'

    def test_value_in_choices(self):
        field = types.Choice(choices=['foo'])
        try:
            field.process_bind_param('foo', None)
        except ValueError:
            raise Exception('Unexpected error')

    def test_choices_not_sequence(self):
        field = types.Choice(choices='foo')
        try:
            field.process_bind_param('foo', None)
        except ValueError:
            raise Exception('Unexpected error')


class TestInterval(object):

    def test_passing_seconds(self):
        field = types.Interval()
        value = field.process_bind_param(36000, None)
        assert isinstance(value, datetime.timedelta)
        assert value.seconds == 36000

    def test_passing_timedelta(self):
        field = types.Interval()
        value = field.process_bind_param(datetime.timedelta(seconds=60), None)
        assert isinstance(value, datetime.timedelta)


class TestChoiceArray(object):

    @patch.object(types, 'ARRAY')
    @patch.object(types.types, 'UnicodeText')
    def test_load_dialect_impl_postgresql(self, mock_unic, mock_array):
        field = types.ChoiceArray(item_type=fields.StringField)
        dialect = Mock()
        dialect.name = 'postgresql'
        field.load_dialect_impl(dialect=dialect)
        assert field.is_postgresql
        assert not mock_unic.called
        assert mock_array.called

    @patch.object(types, 'ARRAY')
    @patch.object(types.types, 'UnicodeText')
    def test_load_dialect_impl_not_postgresql(self, mock_unic, mock_array):
        field = types.ChoiceArray(item_type=fields.StringField)
        dialect = Mock()
        dialect.name = 'some_other'
        field.load_dialect_impl(dialect=dialect)
        assert not field.is_postgresql
        assert mock_unic.called
        assert not mock_array.called

    def test_choices_not_sequence(self):
        field = types.ChoiceArray(
            item_type=fields.StringField, choices='foo')
        assert field.choices == ['foo']

    def test_validate_choices_no_choices(self):
        field = types.ChoiceArray(item_type=fields.StringField)
        assert field.choices is None
        try:
            field._validate_choices(['foo'])
        except ValueError:
            raise Exception('Unexpected error')

    def test_validate_choices_no_value(self):
        field = types.ChoiceArray(
            item_type=fields.StringField, choices=['foo'])
        try:
            field._validate_choices(None)
        except ValueError:
            raise Exception('Unexpected error')

    def test_validate_choices_valid(self):
        field = types.ChoiceArray(
            item_type=fields.StringField,
            choices=['foo', 'bar'])
        try:
            field._validate_choices(['foo'])
        except ValueError:
            raise Exception('Unexpected error')

    def test_validate_choices_invalid(self):
        field = types.ChoiceArray(
            item_type=fields.StringField,
            choices=['foo', 'bar'])
        field._column_name = 'mycol'
        with pytest.raises(ValueError) as ex:
            field._validate_choices(['qoo', 'foo'])
        assert str(ex.value) == (
            'Field `mycol`: Got invalid choices: (qoo). '
            'Valid choices: (foo, bar)')

    def test_process_bind_param_postgres(self):
        field = types.ChoiceArray(item_type=fields.StringField)
        dialect = Mock()
        dialect.name = 'postgresql'
        assert ['q'] == field.process_bind_param(['q'], dialect)

    def test_process_bind_param_not_postgres(self):
        field = types.ChoiceArray(item_type=fields.StringField)
        dialect = Mock()
        dialect.name = 'some_other'
        assert '["q"]' == field.process_bind_param(['q'], dialect)

    def test_process_result_value_postgres(self):
        field = types.ChoiceArray(item_type=fields.StringField)
        dialect = Mock()
        dialect.name = 'postgresql'
        assert ['q'] == field.process_result_value(['q'], dialect)

    def test_process_result_value_not_postgres(self):
        field = types.ChoiceArray(item_type=fields.StringField)
        dialect = Mock()
        dialect.name = 'some_other'
        assert ['q'] == field.process_result_value('["q"]', dialect)


class TestACLType(object):

    def test_validate_action_valid(self):
        obj = types.ACLType()
        try:
            obj._validate_action(list(obj.ACTIONS.values())[0])
        except ValueError:
            raise Exception('Unexpected error')

    def test_validate_action_invalid(self):
        obj = types.ACLType()
        with pytest.raises(ValueError) as ex:
            obj._validate_action('foobarbaz')
        expected = 'Invalid ACL action value: foobarbaz. Valid values are:'
        assert expected in str(ex.value)

    def test_validate_permission_valid(self):
        obj = types.ACLType()
        try:
            obj._validate_permission(types.NEF_ACTIONS[0])
        except ValueError:
            raise Exception('Unexpected error')

    def test_validate_permission_invalid(self):
        obj = types.ACLType()
        with pytest.raises(ValueError) as ex:
            obj._validate_permission('foobarbaz')
        expected = 'Invalid ACL permission value: foobarbaz. Valid values are:'
        assert expected in str(ex.value)

    @patch.object(types.ACLType, '_validate_action')
    @patch.object(types.ACLType, '_validate_permission')
    def test_validate_acl(self, mock_perm, mock_action):
        obj = types.ACLType()
        obj.validate_acl([{'action': 1, 'identifier': 2, 'permission': 3}])
        mock_action.assert_called_once_with(1)
        mock_perm.assert_called_once_with(3)

    def test_stringify_action_existing(self):
        obj = types.ACLType()
        assert obj._stringify_action(Deny) == 'deny'
        assert obj._stringify_action(Allow) == 'allow'

    def test_stringify_action_nonexisting(self):
        obj = types.ACLType()
        assert obj._stringify_action('not allow') == 'not allow'

    def test_stringify_identifier_special(self):
        obj = types.ACLType()
        assert obj._stringify_identifier(Everyone) == 'everyone'
        assert obj._stringify_identifier(Authenticated) == 'authenticated'

    def test_stringify_identifier(self):
        obj = types.ACLType()
        assert obj._stringify_identifier('g:admin') == 'g:admin'

    def test_stringify_permissions_regular_string(self):
        obj = types.ACLType()
        assert obj._stringify_permissions('Foo  ') == ['foo']

    def test_stringify_permissions_special(self):
        obj = types.ACLType()
        perms = obj._stringify_permissions(['foo', ALL_PERMISSIONS])
        assert sorted(perms) == ['all', 'foo']

    @patch.object(types.ACLType, '_stringify_action')
    @patch.object(types.ACLType, '_stringify_identifier')
    @patch.object(types.ACLType, '_stringify_permissions')
    def test_stringify_acl(self, mock_perm, mock_id, mock_action):
        obj = types.ACLType()
        mock_action.return_value = 1
        mock_id.return_value = 2
        mock_perm.return_value = [3, 4]
        result = obj.stringify_acl([('a', 'b', 'c')])
        assert result == [
            {'action': 1, 'identifier': 2, 'permission': 3},
            {'action': 1, 'identifier': 2, 'permission': 4},
        ]
        mock_action.assert_called_once_with('a')
        mock_id.assert_called_once_with('b')
        mock_perm.assert_called_once_with('c')

    @patch.object(types.ACLType, 'stringify_acl')
    @patch.object(types.ACLType, 'validate_acl')
    def test_process_bind_param(self, mock_validate, mock_str):
        mock_str.return_value = [[1, 2, [3]]]
        obj = types.ACLType()
        obj.process_bind_param([('a', 'b', 'c')], Mock())
        mock_str.assert_called_once_with([('a', 'b', 'c')])
        mock_validate.assert_called_once_with([[1, 2, [3]]])

    def test_objectify_action(self):
        assert types.ACLType._objectify_action('allow') is Allow
        assert types.ACLType._objectify_action('deny') is Deny

    def test_objectify_identifier(self):
        assert types.ACLType._objectify_identifier(
            'everyone') is Everyone
        assert types.ACLType._objectify_identifier(
            'authenticated') is Authenticated
        assert types.ACLType._objectify_identifier('foo') == 'foo'

    def test_objectify_permission(self):
        assert types.ACLType._objectify_permission(
            'all') == ALL_PERMISSIONS
        assert types.ACLType._objectify_permission('foo') == 'foo'

    @patch.object(types.ACLType, '_objectify_action')
    @patch.object(types.ACLType, '_objectify_identifier')
    @patch.object(types.ACLType, '_objectify_permission')
    def test_objectify_acl(self, mock_perm, mock_id, mock_action):
        mock_action.return_value = 1
        mock_id.return_value = 2
        mock_perm.return_value = [3]
        result = types.ACLType.objectify_acl([
            {'action': 'a', 'identifier': 'b', 'permission': 'c'}
        ])
        assert result == [[1, 2, [3]]]
        mock_action.assert_called_once_with('a')
        mock_id.assert_called_once_with('b')
        mock_perm.assert_called_once_with('c')
