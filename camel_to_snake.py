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
    properties = event.getProperties()
    
    for key in properties.keys():
        if is_camel_case(key):
        #if "_" in key:
            new_key = camel_to_snake(key)
            properties[new_key]=properties[key]
            del properties[key]
    return event

def camel_to_snake(str):
	res = str[0].lower()
	for c in str[1:]:
		if c in ('ABCDEFGHIJKLMNOPQRSTUVWXYZ'):
			res+=('_')
			res+=(c.lower())
		else:
			res+=(c)
	return (res)

# def snake_to_camel(key):    
#     temp = key.split('_')
#     newkey = temp[0] + ''.join(ele.title() for ele in temp[1:])
#     return newkey
#check camel

def is_camel_case(s):
    return s != s.lower() and s != s.upper() and "_" not in s


# tests = [
#     "camel",
#     "camelCase",
#     "CamelCase",
#     "CAMELCASE",
#     "camelcase",
#     "Camelcase",
#     "Case",
#     "camel_case",
# ]

# for test in tests:
#     print(test, is_camel_case(test))

