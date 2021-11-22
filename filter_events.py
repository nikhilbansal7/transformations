from io.hevo.api import Event

"""
event: each record streaming through Hevo pipeline is an event

returns: 
    - The modified event object.
    - Array of event objects if new events are generated from the incoming event.
    - None if the event is supposed to be dropped from the pipeline.

Read complete documentation at: https://docs.hevodata.com/pipelines/transformations/
"""


def transform(event):
    eventName = event.getEventName()
    properties = event.getProperties()
    
    if eventName == "Ecommerce.testpip_persons":
        return None
    else:
        return event
    
#     if properties['name']=="Azalia":
#         return None
#     else:
#         return event