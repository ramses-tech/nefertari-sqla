import datetime
import decimal
import logging

import elasticsearch

from nefertari.renderers import _JSONEncoder


log = logging.getLogger(__name__)


class JSONEncoderMixin(object):
    def default(self, obj):
        if isinstance(obj, (datetime.datetime, datetime.date)):
            return obj.strftime("%Y-%m-%dT%H:%M:%SZ")  # iso
        if isinstance(obj, datetime.time):
            return obj.strftime('%H:%M:%S')
        if isinstance(obj, datetime.timedelta):
            return obj.seconds
        if isinstance(obj, decimal.Decimal):
            return float(obj)
        return super(JSONEncoderMixin, self).default(obj)


class JSONEncoder(JSONEncoderMixin, _JSONEncoder):
    def default(self, obj):
        if hasattr(obj, 'to_dict'):
            # If it got to this point, it means its a nested object.
            # Outter objects would have been handled with DataProxy.
            return obj.to_dict(__nested=True)
        return super(JSONEncoder, self).default(obj)


class ESJSONSerializer(JSONEncoderMixin,
                       elasticsearch.serializer.JSONSerializer):
    def default(self, obj):
        try:
            return super(ESJSONSerializer, self).default(obj)
        except:
            import traceback
            log.error(traceback.format_exc())
