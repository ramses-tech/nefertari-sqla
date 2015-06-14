import pytest
from mock import patch, Mock

from nefertari.utils.dictset import dictset
from nefertari.json_httpexceptions import (
    JHTTPBadRequest, JHTTPNotFound, JHTTPConflict)
from sqlalchemy.orm.exc import MultipleResultsFound, NoResultFound
from sqlalchemy.orm.collections import InstrumentedList
from sqlalchemy.exc import IntegrityError

from .. import documents as docs
from .. import fields
from .fixtures import memory_db, db_session, simple_model


class TestDocumentHelpers(object):

    @patch.object(docs, 'BaseObject')
    def test_get_document_cls(self, mock_obj):
        mock_obj._decl_class_registry = {'foo': 'bar'}
        doc_cls = docs.get_document_cls('foo')
        assert doc_cls == 'bar'

    @patch.object(docs, 'BaseObject')
    def test_get_document_classes(self, mock_obj):
        foo_mock = Mock(__table__='foo')
        baz_mock = Mock(__tablename__='baz')
        mock_obj._decl_class_registry = {
            'Foo': foo_mock,
            'Bar': Mock(__table__=None),
            'Baz': baz_mock,
        }
        document_classes = docs.get_document_classes()
        assert document_classes == {
            'Foo': foo_mock,
            'Baz': baz_mock,
        }

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

    def test_get_es_mapping(self, memory_db):
        class MyModel(docs.BaseDocument):
            __tablename__ = 'mymodel'
            my_id = fields.IdField()
            name = fields.StringField(primary_key=True)
            groups = fields.ListField(
                item_type=fields.StringField,
                choices=['admin', 'user'])

        class MyModel2(docs.BaseDocument):
            _nested_relationships = ['myself']
            __tablename__ = 'mymodel2'
            name = fields.StringField(primary_key=True)
            myself = fields.Relationship(
                document='MyModel', backref_name='parent',
                uselist=False, backref_uselist=False)
            child_id = fields.ForeignKeyField(
                ref_document='MyModel', ref_column='mymodel.name',
                ref_column_type=fields.StringField)
        memory_db()

        assert MyModel.get_es_mapping() == {
            'mymodel': {
                'properties': {
                    '_type': {'type': 'string'},
                    '_version': {'type': 'long'},
                    'groups': {'type': 'string'},
                    'id': {'type': 'string'},
                    'my_id': {'type': 'long'},
                    'name': {'type': 'string'},
                    'parent': {'type': 'string'},
                    'updated_at': {'format': 'dateOptionalTime',
                                   'type': 'date'}
                }
            }
        }
        assert MyModel2.get_es_mapping() == {
            'mymodel2': {
                'properties': {
                    '_type': {'type': 'string'},
                    '_version': {'type': 'long'},
                    'child_id': {'type': 'string'},
                    'id': {'type': 'string'},
                    'name': {'type': 'string'},
                    'myself': {'type': 'object'},
                    'updated_at': {'format': 'dateOptionalTime',
                                   'type': 'date'}
                }
            }
        }

    def test_pk_field(self, memory_db):
        class MyModel(docs.BaseDocument):
            __tablename__ = 'mymodel'
            my_pk_field = fields.IdField(primary_key=True)
            my_int_field = fields.IntegerField()
        memory_db()

        assert MyModel.pk_field() == 'my_pk_field'

    def test_check_fields_allowed_not_existing_field(
            self, simple_model, memory_db):
        memory_db()

        with pytest.raises(JHTTPBadRequest) as ex:
            simple_model.check_fields_allowed((
                'id__in', 'name', 'description'))
        assert "'MyModel' object does not have fields" in str(ex.value)
        assert 'description' in str(ex.value)
        assert 'name' not in str(ex.value)

    def test_check_fields_allowed(self, simple_model, memory_db):
        memory_db()
        try:
            simple_model.check_fields_allowed(('id__in', 'name'))
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

    def test_apply_fields_no_any_fields(self, simple_model, memory_db):
        memory_db()

        query_set = Mock()
        _fields = []
        simple_model.apply_fields(query_set, _fields)
        assert not query_set.with_entities.called

    def test_apply_sort_no_sort(self, simple_model, memory_db):
        memory_db()

        queryset = ['a', 'b']
        assert simple_model.apply_sort(queryset, []) == queryset

    def test_apply_sort(self, simple_model, memory_db):
        memory_db()

        simple_model.name.desc = Mock()
        queryset = Mock()
        _sort = ['id', '-name']
        simple_model.apply_sort(queryset, _sort)
        simple_model.name.desc.assert_called_once_with()
        queryset.order_by.assert_called_once_with(
            simple_model.id, simple_model.name.desc())

    def test_count(self):
        query_set = Mock()
        query_set.count.return_value = 12345
        count = docs.BaseMixin.count(query_set)
        query_set.count.assert_called_once_with()
        assert count == 12345

    @patch.object(docs, 'Session')
    @patch.object(docs.BaseMixin, 'get_collection')
    def test_filter_objects(
            self, mock_get, mock_sess, simple_model, memory_db):
        memory_db()
        queryset1 = mock_sess().query().filter()
        queryset2 = Mock()
        mock_get.return_value = queryset2
        simple_model.id.in_ = Mock()
        simple_model.filter_objects([Mock(id=4)], first=True)

        mock_sess().query.assert_called_with(simple_model)
        assert mock_sess().query().filter.call_count == 2

        mock_get.assert_called_once_with(
            _limit=1, __raise_on_empty=True,
            query_set=queryset1.from_self())
        simple_model.id.in_.assert_called_once_with(['4'])

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

    def test_native_fields(self, simple_model, memory_db):
        memory_db()
        assert simple_model.native_fields() == [
            'updated_at', '_version', 'id', 'name']

    def test_fields_to_query(self, simple_model, memory_db):
        memory_db()
        assert sorted(simple_model.fields_to_query()) == [
            '_count', '_fields', '_limit', '_page', '_sort',
            '_start', '_version', 'id', 'name', 'updated_at']

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

    @patch.object(docs.BaseMixin, 'get_collection')
    def test_get_or_create_existing(self, get_coll, simple_model, memory_db):
        memory_db()

        get_coll.return_value = Mock()
        one, created = simple_model.get_or_create(
            defaults={'foo': 'bar'}, _limit=2, name='q')
        get_coll.assert_called_once_with(_limit=2, name='q')
        get_coll().one.assert_called_once_with()
        assert not created
        assert one == get_coll().one()

    @patch.object(docs.BaseMixin, 'get_collection')
    def test_get_or_create_existing_multiple(
            self, get_coll, simple_model, memory_db):
        memory_db()

        queryset = Mock()
        get_coll.return_value = queryset
        queryset.one.side_effect = MultipleResultsFound
        with pytest.raises(JHTTPBadRequest) as ex:
            one, created = simple_model.get_or_create(
                defaults={'foo': 'bar'}, _limit=2, name='q')
        assert 'Bad or Insufficient Params' == str(ex.value)

    @patch.object(docs.BaseMixin, 'get_collection')
    def test_get_or_create_existing_created(
            self, get_coll, simple_model, memory_db):
        memory_db()

        queryset = Mock()
        get_coll.return_value = queryset
        queryset.one.side_effect = NoResultFound
        one, created = simple_model.get_or_create(
            defaults={'id': 7}, _limit=2, name='q')
        assert created
        assert one.id == 7
        assert one.name == 'q'

    def test_underscore_update(self, memory_db):
        class MyModel(docs.BaseDocument):
            __tablename__ = 'mymodel'
            id = fields.IdField(primary_key=True)
            name = fields.StringField()
            settings = fields.DictField()
        memory_db()

        myobj = MyModel(id=4, name='foo')
        newobj = myobj._update(
            {'id': 5, 'name': 'bar', 'settings': {'sett1': 'val1'}})
        assert newobj.id == 4
        assert newobj.name == 'bar'
        assert newobj.settings == {'sett1': 'val1'}

    @patch.object(docs, 'Session')
    def test_underscore_delete_many(self, mock_session):
        foo = Mock()
        docs.BaseMixin._delete_many([foo])
        mock_session.assert_called_once_with()
        mock_session().delete.assert_called_with(foo)
        assert mock_session().delete.call_count == 1
        mock_session().flush.assert_called_once_with()

    @patch.object(docs, 'on_bulk_delete')
    def test_underscore_delete_many_query(self, mock_on_bulk):
        from sqlalchemy.orm.query import Query
        items = Query('asd')
        items.all = Mock(return_value=[1, 2, 3])
        items.delete = Mock()
        docs.BaseMixin._delete_many(items)
        items.delete.assert_called_once_with(
            synchronize_session=False)
        mock_on_bulk.assert_called_once_with(
            docs.BaseMixin, [1, 2, 3], refresh_index=None)

    def test_underscore_update_many(self):
        item = Mock()
        docs.BaseMixin._update_many([item], foo='bar')
        item.update.assert_called_once_with(
            {'foo': 'bar'}, refresh_index=None)

    def test_underscore_update_many_query(self):
        from sqlalchemy.orm.query import Query
        items = Query('asd')
        items.update = Mock()
        docs.BaseMixin._update_many(items, foo='bar')
        items.update.assert_called_once_with(
            {'foo': 'bar'}, synchronize_session='fetch')

    def test_repr(self):
        obj = docs.BaseMixin()
        obj.id = 3
        obj._version = 12
        assert str(obj) == '<BaseMixin: id=3, v=12>'

    @patch.object(docs.BaseMixin, 'get_collection')
    def test_get_by_ids(self, mock_coll, memory_db):
        class MyModel(docs.BaseDocument):
            __tablename__ = 'mymodel'
            name = fields.StringField(primary_key=True)
        memory_db()
        MyModel.name = Mock()
        MyModel.get_by_ids([1, 2, 3], foo='bar')
        mock_coll.assert_called_once_with(foo='bar')
        MyModel.name.in_.assert_called_once_with([1, 2, 3])
        assert mock_coll().from_self().filter.call_count == 1
        mock_coll().from_self().filter().limit.assert_called_once_with(3)

    def test_get_null_values(self, memory_db):
        class MyModel1(docs.BaseDocument):
            __tablename__ = 'mymodel1'
            name = fields.StringField(primary_key=True)
            fk_field = fields.ForeignKeyField(
                ref_document='MyModel2', ref_column='mymodel2.name',
                ref_column_type=fields.StringField)

        class MyModel2(docs.BaseDocument):
            __tablename__ = 'mymodel2'
            name = fields.StringField(primary_key=True)
            models1 = fields.Relationship(
                document='MyModel1', backref_name='model2')

        assert MyModel1.get_null_values() == {
            '_version': None,
            'fk_field': None,
            'name': None,
            'model2': None,
            'updated_at': None,
        }

        assert MyModel2.get_null_values() == {
            '_version': None,
            'models1': [],
            'name': None,
            'updated_at': None,
        }

    def test_to_dict(self, memory_db):
        class MyModel(docs.BaseDocument):
            __tablename__ = 'mymodel'
            _nested_relationships = ['other_obj3']
            id = fields.IdField(primary_key=True)
            other_obj = fields.StringField()
            other_obj2 = fields.StringField()
            other_obj3 = fields.StringField()
        memory_db()
        myobj1 = MyModel(id=1)
        myobj1.other_obj = MyModel(id=2)
        myobj1.other_obj2 = InstrumentedList([MyModel(id=3)])
        myobj1.other_obj3 = MyModel(id=4)

        result = myobj1.to_dict()
        assert list(sorted(result.keys())) == [
            '_type', '_version', 'id', 'other_obj', 'other_obj2', 'other_obj3',
            'updated_at']
        assert result['_type'] == 'MyModel'
        assert result['id'] == 1
        # Not nester one-to-one
        assert result['other_obj'] == 2
        # Not nester many-to-one
        assert result['other_obj2'] == [3]
        # Nested one-to-one
        assert isinstance(result['other_obj3'], dict)
        assert result['other_obj3']['_type'] == 'MyModel'
        assert result['other_obj3']['id'] == 4

    @patch.object(docs, 'object_session')
    def test_update_iterables_dict(self, obj_session, memory_db):
        class MyModel(docs.BaseDocument):
            __tablename__ = 'mymodel'
            id = fields.IdField(primary_key=True)
            settings = fields.DictField()
        memory_db()
        myobj = MyModel(id=1)

        # No existing value
        myobj.update_iterables(
            {'setting1': 'foo', 'setting2': 'bar', '__boo': 'boo'},
            attr='settings', save=False)
        assert not obj_session.called
        assert myobj.settings == {'setting1': 'foo', 'setting2': 'bar'}

        # New values to existing value
        myobj.update_iterables(
            {'-setting1': 'foo', 'setting3': 'baz'}, attr='settings',
            save=False)
        assert not obj_session.called
        assert myobj.settings == {'setting2': 'bar', 'setting3': 'baz'}

        # With save
        myobj.update_iterables({}, attr='settings', save=True)
        obj_session.assert_called_once_with(myobj)
        obj_session().add.assert_called_once_with(myobj)
        obj_session().flush.assert_called_once_with()

        # Nulify
        myobj.update_iterables("", attr='settings', unique=False)
        assert myobj.settings == {}
        myobj.update_iterables(None, attr='settings', unique=False)
        assert myobj.settings == {}

    @patch.object(docs, 'object_session')
    def test_update_iterables_list(self, obj_session, memory_db):
        class MyModel(docs.BaseDocument):
            __tablename__ = 'mymodel'
            id = fields.IdField(primary_key=True)
            settings = fields.ListField(item_type=fields.StringField)
        memory_db()
        myobj = MyModel(id=1)

        # No existing value
        myobj.update_iterables(
            {'setting1': '', 'setting2': '', '__boo': 'boo'},
            attr='settings', save=False)
        assert not obj_session.called
        assert sorted(myobj.settings) == ['setting1', 'setting2']

        # New values to existing value
        myobj.update_iterables(
            {'-setting1': '', 'setting3': ''}, attr='settings',
            unique=True, save=False)
        assert not obj_session.called
        assert sorted(myobj.settings) == ['setting2', 'setting3']

        # With save
        myobj.update_iterables(
            {'setting2': ''}, attr='settings', unique=False, save=True)
        assert sorted(myobj.settings) == ['setting2', 'setting2', 'setting3']
        obj_session.assert_called_once_with(myobj)
        obj_session().add.assert_called_once_with(myobj)
        obj_session().flush.assert_called_once_with()

        # Nulify
        myobj.update_iterables(None, attr='settings', unique=False)
        assert myobj.settings == []
        myobj.update_iterables("", attr='settings', unique=False)
        assert myobj.settings == []

    @patch.object(docs, 'object_session')
    def test_get_reference_documents(self, mock_sess, memory_db):

        class Child(docs.BaseDocument):
            __tablename__ = 'child'
            id = fields.IdField(primary_key=True)
            parent_id = fields.ForeignKeyField(
                ref_document='Parent', ref_column='parent.id',
                ref_column_type=fields.IdField)

        class Parent(docs.BaseDocument):
            __tablename__ = 'parent'
            id = fields.IdField(primary_key=True)
            children = fields.Relationship(
                document='Child', backref_name='parent')

        memory_db()

        parent = Parent(id=1)
        child = Child(id=1, parent=parent)
        result = [v for v in child.get_reference_documents()]
        assert len(result) == 1
        assert result[0][0] is Parent
        assert result[0][1] == [parent.to_dict()]

        mock_sess.assert_called_with(parent)
        mock_sess().refresh.assert_called_with(parent)

        # 'Many' side of relationship values are not returned
        assert child in parent.children
        result = [v for v in parent.get_reference_documents()]
        assert len(result) == 0

    def test_is_modified_id_not_persistent(self, memory_db, simple_model):
        memory_db()
        obj = simple_model()
        assert not obj._is_modified()

    def test_is_modified_no_modified_fields(self, memory_db, simple_model):
        memory_db()
        obj = simple_model(id=1).save()
        assert not obj._is_modified()

    def test_is_modified_same_value_set(self, memory_db, simple_model):
        memory_db()
        obj = simple_model(id=1, name='foo').save()
        obj = simple_model.get(id=1)
        obj.name = 'foo'
        assert not obj._is_modified()

    def test_is_modified(self, memory_db, simple_model):
        memory_db()
        obj = simple_model(id=1, name='foo').save()
        obj = simple_model.get(id=1)
        obj.name = 'bar'
        assert obj._is_modified()


class TestBaseDocument(object):

    def test_bump_version(self, simple_model, memory_db):
        from datetime import datetime
        memory_db()

        myobj = simple_model(id=None)
        assert myobj._version is None
        assert myobj.updated_at is None
        myobj._bump_version()

        myobj.save()
        myobj.name = 'foo'
        myobj._bump_version()
        assert myobj._version == 1
        assert isinstance(myobj.updated_at, datetime)

    @patch.object(docs, 'object_session')
    def test_save(self, obj_session, simple_model, memory_db):
        memory_db()

        myobj = simple_model(id=4)
        newobj = myobj.save()
        assert newobj == myobj
        assert myobj._version is None
        obj_session.assert_called_once_with(myobj)
        obj_session().add.assert_called_once_with(myobj)
        obj_session().flush.assert_called_once_with()

    @patch.object(docs, 'object_session')
    def test_save_error(self, obj_session, simple_model, memory_db):
        memory_db()

        err = IntegrityError(None, None, None, None)
        err.args = ('duplicate',)
        obj_session().flush.side_effect = err

        with pytest.raises(JHTTPConflict) as ex:
            simple_model(id=4).save()
        assert 'There was a conflict' in str(ex.value)

    @patch.object(docs, 'object_session')
    @patch.object(docs.BaseMixin, '_update')
    def test_update(self, mock_upd, mock_sess, simple_model, memory_db):
        memory_db()

        myobj = simple_model(id=4)
        myobj.update({'name': 'q'})
        mock_upd.assert_called_once_with({'name': 'q'})
        mock_sess.assert_called_once_with(myobj)
        mock_sess().add.assert_called_once_with(myobj)
        mock_sess().flush.assert_called_once_with()

    @patch.object(docs.BaseMixin, '_update')
    def test_update_error(self, mock_upd, simple_model, memory_db):
        memory_db()

        err = IntegrityError(None, None, None, None)
        err.args = ('duplicate',)
        mock_upd.side_effect = err

        with pytest.raises(JHTTPConflict) as ex:
            simple_model(id=4).update({'name': 'q'})
        assert 'There was a conflict' in str(ex.value)

    @patch.object(docs, 'object_session')
    def test_delete(self, obj_session):
        obj = docs.BaseDocument()
        obj.delete()
        obj_session.assert_called_once_with(obj)
        obj_session().delete.assert_called_once_with(obj)

    def test_apply_before_validation_new_object(self, memory_db):
        processor = lambda instance, new_value: 'foobar'
        processor2 = lambda instance, new_value: new_value + '+'

        class MyModel(docs.BaseDocument):
            __tablename__ = 'mymodel'
            id = fields.IdField(primary_key=True)
            name = fields.StringField(
                before_validation=[processor],
                after_validation=[processor2])
            email = fields.StringField(before_validation=[processor])
        memory_db()

        obj = MyModel(name='myname')
        obj.apply_before_validation()
        assert obj.name == 'foobar'
        assert obj.email == 'foobar'

    def test_apply_before_validation_existing_object(self, memory_db):
        processor = lambda instance, new_value: new_value + '-'
        processor2 = lambda instance, new_value: new_value + '+'

        class MyModel(docs.BaseDocument):
            __tablename__ = 'mymodel'
            id = fields.IdField(primary_key=True)
            name = fields.StringField(
                before_validation=[processor],
                after_validation=[processor2])
            email = fields.StringField(before_validation=[processor])
        memory_db()

        obj = MyModel(id=1, name='myname', email='FOO').save()
        assert obj.name == 'myname-+'
        assert obj.email == 'FOO-'

        obj = MyModel.get(id=1)
        assert obj.name == 'myname-+'
        obj.name = 'supername'
        obj.apply_before_validation()
        assert obj.name == 'supername-'
        assert obj.email == 'FOO-'

    def test_apply_after_validation(self, memory_db):
        memory_db()
        obj = docs.BaseDocument()
        obj.apply_processors = Mock()
        obj._columns_to_process = [1, 2, 3]
        obj.apply_after_validation()
        obj.apply_processors.assert_called_once_with(
            [1, 2, 3], after=True)

    def test_apply_before_validation(self, memory_db):
        class MyModel(docs.BaseDocument):
            __tablename__ = 'mymodel'
            id = fields.IdField(primary_key=True)
        memory_db()

        obj = MyModel(id=1)
        obj.apply_processors = Mock()
        obj.apply_before_validation()
        obj.apply_processors.assert_called_once_with(
            ['_version', 'id', 'updated_at'], before=True)

    def test_apply_processors(self, memory_db):
        class MyModel(docs.BaseDocument):
            __tablename__ = 'mymodel'
            name = fields.StringField(
                primary_key=True,
                before_validation=[lambda instance, new_value: new_value + '-'],
                after_validation=[
                    lambda instance, new_value: new_value + '+'])
        memory_db()
        obj = MyModel(name='foo')

        obj.apply_processors(before=True)
        assert obj.name == 'foo-'

        obj.apply_processors(after=True)
        assert obj.name == 'foo-+'

        obj.apply_processors()
        assert obj.name == 'foo-+'

        obj.apply_processors(column_names=['name'], before=True, after=True)
        assert obj.name == 'foo-+-+'


class TestGetCollection(object):

    def test_input_queryset(self, memory_db):
        class MyModel(docs.BaseDocument):
            __tablename__ = 'mymodel'
            id = fields.IdField(primary_key=True)
            name = fields.StringField()
            foo = fields.StringField()
        memory_db()
        MyModel(id=1, name='foo', foo=2).save()
        MyModel(id=2, name='boo', foo=2).save()
        MyModel(id=3, name='boo', foo=1).save()
        queryset1 = MyModel.get_collection(_limit=50, name='boo')
        assert queryset1.count() == 2
        queryset2 = MyModel.get_collection(
            _limit=50, foo=2, query_set=queryset1.from_self())
        assert queryset2.count() == 1
        assert queryset2.first().id == 2

    def test_sort_param(self, simple_model, memory_db):
        memory_db()

        simple_model(id=1, name='foo').save()
        simple_model(id=2, name='bar').save()

        result = simple_model.get_collection(_limit=2, _sort=['-id'])
        assert result[0].id == 2
        assert result[1].id == 1

        result = simple_model.get_collection(_limit=2, _sort=['id'])
        assert result[1].id == 2
        assert result[0].id == 1

    def test_limit_param(self, simple_model, memory_db):
        memory_db()

        simple_model(id=1, name='foo').save()
        simple_model(id=2, name='bar').save()

        result = simple_model.get_collection(_limit=1, _sort=['id'])
        assert result.count() == 1
        assert result[0].id == 1

    def test_fields_param(self, simple_model, memory_db):
        memory_db()
        simple_model(id=1, name='foo').save()
        result = simple_model.get_collection(_limit=1, _fields=['name'])
        assert result.all() == [('foo',)]

    def test_offset(self, simple_model, memory_db):
        memory_db()
        simple_model(id=1, name='foo').save()
        simple_model(id=2, name='bar').save()

        result = simple_model.get_collection(_limit=2, _sort=['id'], _start=1)
        assert result.count() == 1
        assert result[0].id == 2

        result = simple_model.get_collection(_limit=1, _sort=['id'], _page=1)
        assert result.count() == 1
        assert result[0].id == 2

    def test_count_param(self, simple_model, memory_db):
        memory_db()
        simple_model(id=1, name='foo').save()
        result = simple_model.get_collection(_limit=2, _count=True)
        assert result == 1

    def test_explain_param(self, simple_model, memory_db):
        memory_db()
        simple_model(id=1, name='foo').save()
        result = simple_model.get_collection(_limit=2, _explain=True)
        assert result.startswith('SELECT mymodel')

    def test_strict_param(self, simple_model, memory_db):
        memory_db()
        simple_model(id=1, name='foo').save()
        with pytest.raises(JHTTPBadRequest):
            simple_model.get_collection(
                _limit=2, __strict=True, name='foo', qwe=1)

        result = simple_model.get_collection(
            _limit=2, __strict=False, name='foo', qwe=1)
        assert result.all()[0].name == 'foo'

    def test_raise_on_empty_param(self, simple_model, memory_db):
        memory_db()
        with pytest.raises(JHTTPNotFound):
            simple_model.get_collection(_limit=1, __raise_on_empty=True)

        try:
            simple_model.get_collection(_limit=1, __raise_on_empty=False)
        except JHTTPNotFound:
            raise Exception('Unexpected JHTTPNotFound exception')

    def test_queryset_metadata(self, simple_model, memory_db):
        memory_db()
        simple_model(id=1, name='foo').save()
        queryset = simple_model.get_collection(_limit=1)
        assert queryset._nefertari_meta['total'] == 1
        assert queryset._nefertari_meta['start'] == 0
        assert queryset._nefertari_meta['fields'] == []

    def test_no_limit(self, simple_model, memory_db):
        memory_db()
        with pytest.raises(JHTTPBadRequest):
            simple_model.get_collection()
