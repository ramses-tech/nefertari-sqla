from mock import Mock

from .. import fields


class TestApplyColumnProcessors(object):
    def _column_obj(self):
        return Mock(before_validation=[],
                    after_validation=[])

    def test_no_processors(self):
        column_obj = self._column_obj()
        result = fields.apply_column_processors(
            column_obj, before=True, after=True,
            new_value=1)
        assert result == 1

    def test_not_before_not_after(self):
        column_obj = self._column_obj()
        func_before = Mock()
        func_after = Mock()
        column_obj.before_validation.append(func_before)
        column_obj.after_validation.append(func_after)
        result = fields.apply_column_processors(
            column_obj, before=False, after=False,
            new_value=1)
        assert result == 1
        assert not func_before.called
        assert not func_after.called

    def test_before(self):
        column_obj = self._column_obj()
        func_before = Mock()
        func_after = Mock()
        column_obj.before_validation.append(func_before)
        column_obj.after_validation.append(func_after)
        result = fields.apply_column_processors(
            column_obj, before=True, after=False,
            foo=1)
        func_before.assert_called_once_with(foo=1)
        assert not func_after.called
        assert result == func_before()

    def test_after(self):
        column_obj = self._column_obj()
        func_before = Mock()
        func_after = Mock()
        column_obj.before_validation.append(func_before)
        column_obj.after_validation.append(func_after)
        result = fields.apply_column_processors(
            column_obj, before=False, after=True,
            foo=1)
        func_after.assert_called_once_with(foo=1)
        assert not func_before.called
        assert result == func_after()

    def test_after_before(self):
        column_obj = self._column_obj()
        func_before = Mock()
        func_after = Mock()
        column_obj.before_validation.append(func_before)
        column_obj.after_validation.append(func_after)
        result = fields.apply_column_processors(
            column_obj, before=True, after=True,
            foo=1)
        func_before.assert_called_once_with(foo=1)
        func_after.assert_called_once_with(
            foo=1, new_value=func_before())
        assert result == func_after()


class TestProcessableRelationshipProperty(object):
    def test_init(self):
        kwargs = {
            'after_validation': 1,
            'backref_before_validation': 2,
        }
        prop = fields.ProcessableRelationshipProperty(
            'Foo', **kwargs)
        assert prop.before_validation == ()
        assert prop.after_validation == 1
        assert prop.backref_after_validation == ()
        assert prop.backref_before_validation == 2

    def test_set_backref_processors(self):
        prop = fields.ProcessableRelationshipProperty(
            'Foo', backref_after_validation=1,
            backref_before_validation=2)
        prop.back_populates = 'foo'
        prop.mapper = Mock()
        primary_mapper = Mock(relationships=[Mock(key='foo')])
        prop.mapper.primary_mapper.return_value = primary_mapper
        prop._set_backref_processors()
        backref = primary_mapper.relationships[0]
        assert backref.before_validation == 2
        assert backref.after_validation == 1

    def test_set_backref_processors_keyerror(self):
        prop = fields.ProcessableRelationshipProperty(
            'Foo', backref_after_validation=1,
            backref_before_validation=2)
        prop.back_populates = 'foo'
        prop.mapper = Mock()
        primary_mapper = Mock(relationships=[Mock(key='Zoo')])
        prop.mapper.primary_mapper.return_value = primary_mapper
        prop._set_backref_processors()
        backref = primary_mapper.relationships[0]
        assert backref.before_validation != 2
        assert backref.after_validation != 1
