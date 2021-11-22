from io.hevo.api import Event
import re

def transform(event):
    # Get event name from the event #
    # eventName = event.getEventName()
    properties = event.getProperties()
    
    for key in properties.keys():
        if is_camel_case(key):
            new_key = camel_to_snake(key)
            properties[new_key]=properties[key]
            del properties[key]
    return event

def is_camel_case(s):
    return s != s.lower() and s != s.upper() and "_" not in s

def camel_to_snake(key):
    name = key
    name = re.sub(r'(?<!^)(?=[A-Z])', '_', name).lower()
    return (name)  