from io.hevo.api import Event


def transform(event):

    # Get properties from the event #
    properties = event.getProperties()
    count=0
    for i in (properties['__hevo_id'][::-1]):
        if i == "_":
            break
        else :
            count+=1
    properties['source_directory'] = properties['__hevo_id'][:-(count+1)]

    return event

