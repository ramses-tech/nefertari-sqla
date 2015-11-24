from .. import fields
from .. import documents as docs
from .fixtures import memory_db


class TestDocMeta(object):

    def test_tablename_present_concrete(self, memory_db):
        class MyModel(docs.BaseDocument):
            __tablename__ = 'mymodel'
            my_id = fields.IdField(primary_key=True)
        memory_db()

        assert MyModel.__tablename__ == 'mymodel'

    def test_tablename_missing_abstract(self, memory_db):
        class MyModel(docs.BaseDocument):
            __abstract__ = True
            my_id = fields.IdField(primary_key=True)
        memory_db()

        assert not hasattr(MyModel, '__tablename__')

    def test_tablename_missing_concrete(self, memory_db):
        class MyModel(docs.BaseDocument):
            my_id = fields.IdField(primary_key=True)
        memory_db()

        assert MyModel.__tablename__ == 'mymodel'
