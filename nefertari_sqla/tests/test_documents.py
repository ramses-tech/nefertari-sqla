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
            my_id_field = fields.IdField()
            my_int_field = fields.IntegerField()
        memory_db()

        assert MyModel.id_field() == 'my_id_field'

    def test_check_fields_allowed_not_existing_field(self, memory_db):
        class MyModel(docs.BaseDocument):
            __tablename__ = 'mymodel'
            id = fields.IdField()
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
            id = fields.IdField()
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
            id = fields.IdField()
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
            id = fields.IdField()
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
            id = fields.IdField()
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
            id = fields.IdField()
            name = fields.StringField()
        memory_db()

        query_set = Mock()
        _fields = []
        MyModel.apply_fields(query_set, _fields)
        assert not query_set.with_entities.called

    def test_apply_sort_no_sort(self, memory_db):
        class MyModel(docs.BaseDocument):
            __tablename__ = 'mymodel'
            id = fields.IdField()
            name = fields.StringField()
        memory_db()

        queryset = ['a', 'b']
        assert MyModel.apply_sort(queryset, []) == queryset

    def test_apply_sort(self, memory_db):
        class MyModel(docs.BaseDocument):
            __tablename__ = 'mymodel'
            id = fields.IdField()
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
