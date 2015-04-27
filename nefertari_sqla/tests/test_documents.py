import pytest
from mock import patch, Mock

from nefertari.utils.dictset import dictset
from nefertari.json_httpexceptions import (
    JHTTPBadRequest, JHTTPNotFound, JHTTPConflict)

from .. import documents as docs
from .. import fields
from .fixtures import memory_db, db_session


class TestDocumentHelpers(object):

    @patch.object(docs, 'BaseObject')
    def test_get_document_cls(self, mock_obj):
        mock_obj._decl_class_registry = {'foo': 'bar'}
        doc_cls = docs.get_document_cls('foo')
        assert doc_cls == 'bar'

    @patch.object(docs, 'BaseObject')
    def test_get_document_cls_key_error(self, mock_obj):
        mock_obj._decl_class_registry = {}
        with pytest.raises(ValueError) as ex:
            docs.get_document_cls('foo')
        expected = 'SQLAlchemy model `foo` does not exist'
        assert str(ex.value) == expected

    def test_process_lists(self):
        test_dict = dictset(
            id__in='1,   2, 3',
            name__all='foo',
            other__arg='4',
            yet_other_arg=5,
        )
        result_dict = docs.process_lists(test_dict)
        expected = dictset(
            id__in=['1', '2', '3'],
            name__all=['foo'],
            other__arg='4',
            yet_other_arg=5,
        )
        assert result_dict == expected

    def test_process_bools(self):
        test_dict = dictset(
            complete__bool='false',
            other_arg=5,
        )
        result_dict = docs.process_bools(test_dict)
        assert result_dict == dictset(complete=False, other_arg=5)


class TestBaseMixin(object):

    def test_id_field(self, memory_db):
        class MyModel(docs.BaseDocument):
            __tablename__ = 'mymodel'
            my_id_field = fields.IdField(primary_key=True)
            my_int_field = fields.IntegerField()
        memory_db()

        assert MyModel.id_field() == 'my_id_field'

    def test_check_fields_allowed_not_existing_field(self, memory_db):
        class MyModel(docs.BaseDocument):
            __tablename__ = 'mymodel'
            id = fields.IdField(primary_key=True)
            name = fields.StringField()
        memory_db()

        with pytest.raises(JHTTPBadRequest) as ex:
            MyModel.check_fields_allowed(('id__in', 'name', 'description'))
        assert "'MyModel' object does not have fields" in str(ex.value)
        assert 'description' in str(ex.value)
        assert 'name' not in str(ex.value)

    def test_check_fields_allowed(self, memory_db):
        class MyModel(docs.BaseDocument):
            __tablename__ = 'mymodel'
            id = fields.IdField(primary_key=True)
            name = fields.StringField()
        memory_db()
        try:
            MyModel.check_fields_allowed(('id__in', 'name'))
        except JHTTPBadRequest:
            raise Exception('Unexpected JHTTPBadRequest exception raised')

    @patch.object(docs.BaseMixin, 'fields_to_query')
    def test_filter_fields(self, mock_fields):
        mock_fields.return_value = ('description', 'id', 'complete')
        params = docs.BaseMixin.filter_fields(dictset(
            description='nice',
            name='regular name',
            id__in__here=[1, 2, 3],
        ))
        assert params == dictset(
            description='nice',
            id__in__here=[1, 2, 3],
        )

    def test_apply_fields(self, memory_db):
        class MyModel(docs.BaseDocument):
            __tablename__ = 'mymodel'
            id = fields.IdField(primary_key=True)
            name = fields.StringField()
            desc = fields.StringField()
            title = fields.StringField()
        memory_db()

        query_set = Mock()
        _fields = ['name', 'id', '-title']
        MyModel.apply_fields(query_set, _fields)
        query_set.with_entities.assert_called_once_with(
            MyModel.id, MyModel.name)

    def test_apply_fields_no_only_fields(self, memory_db):
        class MyModel(docs.BaseDocument):
            __tablename__ = 'mymodel'
            id = fields.IdField(primary_key=True)
            name = fields.StringField()
            desc = fields.StringField()
            title = fields.StringField()
        memory_db()

        query_set = Mock()
        _fields = ['-title', '-_version', '-updated_at']
        MyModel.apply_fields(query_set, _fields)
        query_set.with_entities.assert_called_once_with(
            MyModel.desc, MyModel.id, MyModel.name)

    def test_apply_fields_no_exclude_fields(self, memory_db):
        class MyModel(docs.BaseDocument):
            __tablename__ = 'mymodel'
            id = fields.IdField(primary_key=True)
            name = fields.StringField()
            desc = fields.StringField()
            title = fields.StringField()
        memory_db()

        query_set = Mock()
        _fields = ['title']
        MyModel.apply_fields(query_set, _fields)
        query_set.with_entities.assert_called_once_with(
            MyModel.title)

    def test_apply_fields_no_any_fields(self, memory_db):
        class MyModel(docs.BaseDocument):
            __tablename__ = 'mymodel'
            id = fields.IdField(primary_key=True)
            name = fields.StringField()
        memory_db()

        query_set = Mock()
        _fields = []
        MyModel.apply_fields(query_set, _fields)
        assert not query_set.with_entities.called

    def test_apply_sort_no_sort(self, memory_db):
        class MyModel(docs.BaseDocument):
            __tablename__ = 'mymodel'
            id = fields.IdField(primary_key=True)
            name = fields.StringField()
        memory_db()

        queryset = ['a', 'b']
        assert MyModel.apply_sort(queryset, []) == queryset

    def test_apply_sort(self, memory_db):
        class MyModel(docs.BaseDocument):
            __tablename__ = 'mymodel'
            id = fields.IdField(primary_key=True)
            name = fields.StringField()
        memory_db()

        MyModel.name.desc = Mock()
        queryset = Mock()
        _sort = ['id', '-name']
        MyModel.apply_sort(queryset, _sort)
        MyModel.name.desc.assert_called_once_with()
        queryset.order_by.assert_called_once_with(
            MyModel.id, MyModel.name.desc())

    def test_count(self):
        query_set = Mock()
        query_set.count.return_value = 12345
        count = docs.BaseMixin.count(query_set)
        query_set.count.assert_called_once_with()
        assert count == 12345

    @patch.object(docs.BaseMixin, 'get_collection')
    def test_filter_objects(self, mock_get, memory_db):
        queryset = Mock()
        mock_get.return_value = queryset

        class MyModel(docs.BaseDocument):
            __tablename__ = 'mymodel'
            id = fields.IdField(primary_key=True)
        memory_db()

        MyModel.id.in_ = Mock()
        MyModel.filter_objects([Mock(id=4)], first=True)

        mock_get.assert_called_once_with(_limit=1, __raise_on_empty=True)
        queryset.from_self.assert_called_once_with()
        assert queryset.from_self().filter.call_count == 1
        queryset.from_self().filter().first.assert_called_once_with()
        MyModel.id.in_.assert_called_once_with(['4'])

    def test_pop_iterables(self, memory_db):
        class MyModel(docs.BaseDocument):
            __tablename__ = 'mymodel'
            id = fields.IdField(primary_key=True)
            groups = fields.ListField(item_type=fields.StringField)
            settings = fields.DictField()
        memory_db()
        MyModel.groups.contains = Mock()
        MyModel.settings.contains = Mock()
        MyModel.settings.has_key = Mock()
        MyModel.groups.type.is_postgresql = True
        MyModel.settings.type.is_postgresql = True

        params = {'settings': 'foo', 'groups': 'bar', 'id': 1}
        iterables, params = MyModel._pop_iterables(params)
        assert params == {'id': 1}
        assert not MyModel.settings.contains.called
        MyModel.settings.has_key.assert_called_once_with('foo')
        MyModel.groups.contains.assert_called_once_with(['bar'])

        params = {'settings.foo': 'foo2', 'groups': 'bar', 'id': 1}
        iterables, params = MyModel._pop_iterables(params)
        assert params == {'id': 1}
        assert MyModel.settings.has_key.call_count == 1
        MyModel.settings.contains.assert_called_once_with({'foo': 'foo2'})

    @patch.object(docs.BaseMixin, 'native_fields')
    def test_has_field(self, mock_fields):
        mock_fields.return_value = ['foo', 'bar']
        assert docs.BaseMixin.has_field('foo')
        assert not docs.BaseMixin.has_field('bazz')

    @patch.object(docs.BaseMixin, 'get_collection')
    def test_get_resource(self, mock_get_coll):
        queryset = Mock()
        mock_get_coll.return_value = queryset
        resource = docs.BaseMixin.get_resource(foo='bar')
        mock_get_coll.assert_called_once_with(
            __raise_on_empty=True, _limit=1, foo='bar')
        mock_get_coll().first.assert_called_once_with()
        assert resource == mock_get_coll().first()

    def test_native_fields(self, memory_db):
        class MyModel(docs.BaseDocument):
            __tablename__ = 'mymodel'
            id = fields.IdField(primary_key=True)
            name = fields.StringField()
        memory_db()
        assert MyModel.native_fields() == [
            'updated_at', '_version', 'id', 'name']

    def test_fields_to_query(self, memory_db):
        class MyModel(docs.BaseDocument):
            __tablename__ = 'mymodel'
            id = fields.IdField(primary_key=True)
            name = fields.StringField()
        memory_db()
        assert MyModel.fields_to_query() == [
            '_count', '_start', 'name', '_sort', 'updated_at',
            '_version', '_limit', '_fields', 'id', '_page']

    @patch.object(docs.BaseMixin, 'get_resource')
    def test_get(self, get_res):
        docs.BaseMixin.get(foo='bar')
        get_res.assert_called_once_with(
            __raise_on_empty=False, foo='bar')

    def test_unique_fields(self, memory_db):
        class MyModel(docs.BaseDocument):
            __tablename__ = 'mymodel'
            id = fields.IdField(primary_key=True)
            name = fields.StringField(unique=True)
            desc = fields.StringField()
        memory_db()
        assert MyModel().unique_fields() == [
            MyModel.id, MyModel.name]
