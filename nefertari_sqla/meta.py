from sqlalchemy.ext.declarative import DeclarativeMeta

from nefertari.engine.common import MultiEngineMeta


class DocMeta(MultiEngineMeta, DeclarativeMeta):
    """ Metaclass that generates __tablename__ if it or '__table__'
    aren't explicitly defined.
    """
    def __new__(cls, name, bases, attrs):
        table_specified = ('__tablename__' in attrs or
                           '__table__' in attrs)
        if not table_specified and not attrs.get('__abstract__', False):
            attrs['__tablename__'] = name.lower()
        return super(DocMeta, cls).__new__(cls, name, bases, attrs)


class ESMetaclass(DocMeta):
    def __init__(self, name, bases, attrs):
        from .signals import setup_es_signals_for
        self._index_enabled = True
        setup_es_signals_for(self)
        return super(ESMetaclass, self).__init__(name, bases, attrs)
