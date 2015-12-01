import logging

from sqlalchemy import event
from sqlalchemy.orm import object_session, class_mapper, attributes
from pyramid_sqlalchemy import Session

from nefertari.engine import sync_events


log = logging.getLogger(__name__)


def on_after_insert(mapper, connection, target):
    # Reload `target` to get access to back references and processed
    # fields values
    request = getattr(target, '_request', None)
    model_cls = target.__class__
    pk_field = target.pk_field()
    reloaded = model_cls.get_item(**{
        pk_field: getattr(target, pk_field),
        '_query_secondary': False})

    if request is not None:
        event = sync_events.ItemCreated(item=reloaded)
        request.registry.notify(event)


def on_after_update(mapper, connection, target):
    request = getattr(target, '_request', None)
    from .documents import BaseDocument

    # Reindex old one-to-one related object
    committed_state = attributes.instance_state(target).committed_state
    for field, value in committed_state.items():
        if isinstance(value, BaseDocument):
            obj_session = object_session(value)
            # Make sure object is not updated yet
            if not obj_session.is_modified(value):
                obj_session.expire(value)

            if request is not None:
                event = sync_events.ItemUpdated(item=value)
                request.registry.notify(event)

    # Reload `target` to get access to processed fields values
    columns = [c.name for c in class_mapper(target.__class__).columns]
    object_session(target).expire(target, attribute_names=columns)
    if request is not None:
        event = sync_events.ItemUpdated(item=target)
        request.registry.notify(event)


def on_after_delete(mapper, connection, target):
    request = getattr(target, '_request', None)
    if request is not None:
        event = sync_events.ItemDeleted(item=target)
        request.registry.notify(event)


def on_bulk_update(update_context):
    request = getattr(
        update_context.query, '_request', None)
    objects = update_context.query.all()
    if not objects:
        return

    if request is not None:
        event = sync_events.BulkUpdated(items=list(objects))
        request.registry.notify(event)


def on_bulk_delete(model_cls, objects, request):
    if request is not None:
        event = sync_events.BulkDeleted(items=list(objects))
        request.registry.notify(event)


def setup_signals_for(source_cls):
    event.listen(source_cls, 'after_insert', on_after_insert)
    event.listen(source_cls, 'after_update', on_after_update)
    event.listen(source_cls, 'after_delete', on_after_delete)
    log.info('setup_signals_for: %r' % source_cls)


event.listen(Session, 'after_bulk_update', on_bulk_update)
