import datetime
import decimal
import logging

import elasticsearch

from nefertari.renderers import _JSONEncoder


log = logging.getLogger(__name__)


class JSONEncoder(_JSONEncoder):
    def default(self, obj):
        if (isinstance(obj, datetime.date) and
                not isinstance(obj, datetime.datetime)):
            return obj.strftime('%Y-%m-%d')

        if isinstance(obj, decimal.Decimal):
            return float(obj)

        if hasattr(obj, 'to_dict'):
            # If it got to this point, it means its a nested object.
            # Outter objects would have been handled with DataProxy.
            return obj.to_dict(__nested=True)

        return super(JSONEncoder, self).default(obj)


class ESJSONSerializer(elasticsearch.serializer.JSONSerializer):
    def default(self, data):
        if (isinstance(data, datetime.date) and
                not isinstance(data, datetime.datetime)):
            return data.strftime('%Y-%m-%d')
        if isinstance(data, datetime.time):
            return data.strftime('%H:%M:%S')
        if isinstance(data, datetime.timedelta):
            return str(data)
        if isinstance(data, decimal.Decimal):
            return float(data)
        try:
            return super(ESJSONSerializer, self).default(data)
        except:
            import traceback
            log.error(traceback.format_exc())
