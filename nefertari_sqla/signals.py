import logging

from sqlalchemy import event
from sqlalchemy.ext.declarative import DeclarativeMeta


log = logging.getLogger(__name__)


def on_after_insert(mapper, connection, target):
    from nefertari.elasticsearch import ES
    model_cls = target.__class__
    es = ES(model_cls.__name__)
    es.index(target.to_dict())
    # Reload `target` to get access to back references
    id_field = target.id_field()
    reloaded = model_cls.get(**{id_field: getattr(target, id_field)})
    es.index_refs(reloaded)


def on_after_update(mapper, connection, target):
    from nefertari.elasticsearch import ES
    es = ES(target.__class__.__name__)
    es.index(target.to_dict())
    es.index_refs(target)


def on_after_delete(mapper, connection, target):
    from nefertari.elasticsearch import ES
    es = ES(target.__class__.__name__)
    es.delete(target.id)
    es.index_refs(target)


def setup_es_signals_for(source_cls):
    event.listen(source_cls, 'after_insert', on_after_insert)
    event.listen(source_cls, 'after_update', on_after_update)
    event.listen(source_cls, 'after_delete', on_after_delete)
    log.info('setup_sqla_es_signals_for: %r' % source_cls)


class ESMetaclass(DeclarativeMeta):
    def __init__(self, name, bases, attrs):
        self._index_enabled = True
        setup_es_signals_for(self)
        return super(ESMetaclass, self).__init__(name, bases, attrs)
