from io.hevo.api import Event
import re


def transform(event):
    properties = event.getProperties()

    if re.search("Campaign",properties["__hevo_id"]):
        return event


