import logging

import elasticsearch

from nefertari.renderers import _JSONEncoder


log = logging.getLogger(__name__)


class JSONEncoder(_JSONEncoder):
    def default(self, obj):
        if hasattr(obj, 'to_dict'):
            # If it got to this point, it means its a nested object.
            # Outter objects would have been handled with DataProxy.
            return obj.to_dict(__nested=True)

        return super(JSONEncoder, self).default(obj)


class ESJSONSerializer(elasticsearch.serializer.JSONSerializer):
    def default(self, data):
        try:
            return super(ESJSONSerializer, self).default(data)
        except:
            import traceback
            log.error(traceback.format_exc())
