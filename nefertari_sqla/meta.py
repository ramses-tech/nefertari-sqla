from sqlalchemy.ext.declarative import DeclarativeMeta

from nefertari.engine.common import MultiEngineMeta


class DocMeta(MultiEngineMeta, DeclarativeMeta):
    pass


class ESMetaclass(DocMeta):
    def __init__(self, name, bases, attrs):
        from .signals import setup_es_signals_for
        self._index_enabled = True
        setup_es_signals_for(self)
        return super(ESMetaclass, self).__init__(name, bases, attrs)
