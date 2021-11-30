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
    # Get event name from the event #
    # eventName = event.getEventName()

    # Get properties from the event #
    properties = event.getProperties()
    count=0
    for i in (properties['__hevo_id'][::-1]):
        if i == "_":
            break
        else :
            count+=1
    properties['source_directory'] = properties['__hevo_id'][:-(count+1)]

    # Rename event
    # event.rename(newName)

    return event

