import pytest


@pytest.fixture()
def memory_db(request):
    from sqlalchemy import create_engine
    from pyramid_sqlalchemy import Session, BaseObject

    def creator():
        """ Create all registered models and connect to engine. """
        engine = create_engine('sqlite://')
        BaseObject.metadata.create_all(engine)
        connection = engine.connect()
        Session.registry.clear()
        Session.configure(bind=connection)
        BaseObject.metadata.bind = engine
        return connection

    def clear():
        """ Drop all tables and clear models registry. """
        BaseObject.metadata.drop_all()
        BaseObject.metadata.clear()

    request.addfinalizer(clear)
    return creator


@pytest.fixture
def simple_model(request):
    from .. import fields, documents as docs

    class MyModel(docs.BaseDocument):
        __tablename__ = 'mymodel'
        id = fields.IdField(primary_key=True)
        name = fields.StringField()
    return MyModel


# Not used yet, because memory_db is called for each test
@pytest.fixture
def db_session(request):
    from transaction import abort
    from pyramid_sqlalchemy import Session

    def creator(connection):
        """ Begin transaction and connect rollbacks.

        `connection` should be obrained by calling 'memory_db()' inside test.
        """
        trans = connection.begin()
        request.addfinalizer(trans.rollback)
        request.addfinalizer(abort)
        return Session

    return creator
