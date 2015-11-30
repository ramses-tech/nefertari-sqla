from sqlalchemy.ext.declarative import DeclarativeMeta

from nefertari import engine


class DocMeta(engine.common.MultiEngineMeta, DeclarativeMeta):
    """ Metaclass that generates __tablename__ if it or '__table__'
    aren't explicitly defined.
    """
    def __new__(cls, name, bases, attrs):
        table_specified = ('__tablename__' in attrs or
                           '__table__' in attrs)
        if not table_specified and not attrs.get('__abstract__', False):
            attrs['__tablename__'] = name.lower()
        return super(DocMeta, cls).__new__(cls, name, bases, attrs)

    def __init__(self, name, bases, attrs):
        if engine.secondary is not None:
            from .signals import setup_signals_for
            setup_signals_for(self)
        return super(DocMeta, self).__init__(name, bases, attrs)
