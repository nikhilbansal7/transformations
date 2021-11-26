from io.hevo.api import Event
import re
import json
from io.hevo.api import Utils


def transform(event):
   # Get event name from the event #
    eventName = event.getEventName()
    properties = event.getProperties()
    for key in properties.keys():
        if is_camel_case(key):
            new_key = camel_to_snake(key)
            if "varchar" in Utils.getType(properties[key]):
                if re.match("\\[", properties[key]):
                    properties[new_key] = json.loads(properties[key])
                if re.match("\\{",properties[key]):
                    properties[new_key] = Utils.jsonStringToDict(properties[key])
            else:
                properties[new_key] = properties[key]
            del properties[key]
    #dropping events on the basis of eventName and field value
    if eventName == "vibenomics.impressionTracking" and properties['status'] != "confirmed":
        return None
    else:
        return event   
#check if given key is in camel case
def is_camel_case(s):
    return s != s.lower() and s != s.upper() and "_" not in s


#convert camel case to snake case
def camel_to_snake(key):
    name = key
    name = re.sub(r'(?<!^)(?=[A-Z])', '_', name).lower()
    return (name)