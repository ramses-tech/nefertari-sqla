import logging

from sqlalchemy import event
from sqlalchemy.ext.declarative import DeclarativeMeta
from sqlalchemy.orm import object_session, class_mapper


log = logging.getLogger(__name__)


def on_after_insert(mapper, connection, target):
    from nefertari.elasticsearch import ES
    # Reload `target` to get access to back references and processed
    # fields values
    model_cls = target.__class__
    pk_field = target.pk_field()
    reloaded = model_cls.get(**{pk_field: getattr(target, pk_field)})

    es = ES(model_cls.__name__)
    es.index(reloaded.to_dict())
    es.index_refs(reloaded)


def on_after_update(mapper, connection, target):
    # Do not index on collections update. Use 'ES.index_refs' on
    # insert & delete instead.
    session = object_session(target)
    if not session.is_modified(target, include_collections=False):
        return

    # Reload `target` to get access to processed fields values
    attributes = [c.name for c in class_mapper(target.__class__).columns]
    session.expire(target, attribute_names=attributes)

    from nefertari.elasticsearch import ES
    es = ES(target.__class__.__name__)
    es.index(target.to_dict())
    es.index_refs(target)


def on_after_delete(mapper, connection, target):
    from nefertari.elasticsearch import ES
    model_cls = target.__class__
    es = ES(model_cls.__name__)
    obj_id = getattr(target, model_cls.pk_field())
    es.delete(obj_id)
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
