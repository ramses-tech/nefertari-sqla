import logging

from sqlalchemy import event
from sqlalchemy.ext.declarative import DeclarativeMeta
from sqlalchemy.orm import object_session, class_mapper
from pyramid_sqlalchemy import Session

from nefertari.utils import to_dicts


log = logging.getLogger(__name__)


def index_object(obj, with_refs=True, **kwargs):
    from nefertari.elasticsearch import ES
    es = ES(obj.__class__.__name__)
    es.index(obj.to_dict(), **kwargs)
    if with_refs:
        es.index_refs(obj, **kwargs)


def on_after_insert(mapper, connection, target):
    # Reload `target` to get access to back references and processed
    # fields values
    refresh_index = getattr(target, '_refresh_index', None)
    model_cls = target.__class__
    pk_field = target.pk_field()
    reloaded = model_cls.get(**{pk_field: getattr(target, pk_field)})
    index_object(reloaded, refresh_index=refresh_index)


def on_after_update(mapper, connection, target):
    refresh_index = getattr(target, '_refresh_index', None)
    session = object_session(target)

    # Reload `target` to get access to processed fields values
    attributes = [c.name for c in class_mapper(target.__class__).columns]
    session.expire(target, attribute_names=attributes)
    index_object(target, refresh_index=refresh_index)


def on_after_delete(mapper, connection, target):
    from nefertari.elasticsearch import ES
    refresh_index = getattr(target, '_refresh_index', None)
    model_cls = target.__class__
    es = ES(model_cls.__name__)
    obj_id = getattr(target, model_cls.pk_field())
    es.delete(obj_id, refresh_index=refresh_index)
    es.index_refs(target, refresh_index=refresh_index)


def on_bulk_update(update_context):
    refresh_index = getattr(
        update_context.query, '_refresh_index', None)
    model_cls = update_context.mapper.entity
    if not getattr(model_cls, '_index_enabled', False):
        return

    objects = update_context.query.all()
    if not objects:
        return

    from nefertari.elasticsearch import ES
    es = ES(source=model_cls.__name__)
    documents = to_dicts(objects)
    es.index(documents, refresh_index=refresh_index)

    # Reindex relationships
    for obj in objects:
        es.index_refs(obj, refresh_index=refresh_index)


def on_bulk_delete(model_cls, objects, refresh_index=None):
    if not getattr(model_cls, '_index_enabled', False):
        return

    pk_field = model_cls.pk_field()
    ids = [getattr(obj, pk_field) for obj in objects]

    from nefertari.elasticsearch import ES
    es = ES(source=model_cls.__name__)
    es.delete(ids, refresh_index=refresh_index)

    # Reindex relationships
    for obj in objects:
        es.index_refs(obj, refresh_index=refresh_index)


def setup_es_signals_for(source_cls):
    event.listen(source_cls, 'after_insert', on_after_insert)
    event.listen(source_cls, 'after_update', on_after_update)
    event.listen(source_cls, 'after_delete', on_after_delete)
    log.info('setup_sqla_es_signals_for: %r' % source_cls)


event.listen(Session, 'after_bulk_update', on_bulk_update)


class ESMetaclass(DeclarativeMeta):
    def __init__(self, name, bases, attrs):
        self._index_enabled = True
        setup_es_signals_for(self)
        return super(ESMetaclass, self).__init__(name, bases, attrs)
