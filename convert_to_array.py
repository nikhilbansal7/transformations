from io.hevo.api import Event
import json
from io.hevo.api import Utils
import re

def transform(event):

    properties = event.getProperties()

    for key in properties.keys():
        if "varchar" in Utils.getType(properties[key]):
            if re.match("\\[", properties[key]):
                properties[key] = json.loads(properties[key])

    return event

