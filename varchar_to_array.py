from io.hevo.api import Event
import json
from io.hevo.api import Utils

"""
event: each record streaming through Hevo pipeline is an event

returns: 
    - The modified event object.
    - Array of event objects if new events are generated from the incoming event.
    - None if the event is supposed to be dropped from the pipeline.

Read complete documentation at: https://docs.hevodata.com/pipelines/transformations/
"""


def transform(event):
#     # Get event name from the event #
#     # eventName = event.getEventName()
#     key='array_of_strings'
#     # Get properties from the event #
    properties = event.getProperties()
    list_of_arrays = ['array_of_strings','array_of_integers']
    new_array = []
    for key in properties.keys():
        if key in list_of_arrays:
            for i in range(len(properties[key].split(','))):
                if i == 0 :
                    new_array.append(properties[key].split(',')[i][1:])
                    
                elif i == len(properties[key].split(',')) - 1:
                    new_array.append(properties[key].split(',')[i][:-1])
                else:
                    new_array.append(properties[key].split(',')[i])
            properties[key]=new_array
            new_array=[]    
    #properties['type']=Utils.getType(properties['array_of_strings'])
    #properties['new']=new_array
    properties['type']=Utils.getType(properties['array_of_strings'])
#     properties['type']=Utils.getType(properties[key])
                         
#     # Add a new field to the event #
#     # properties['foo'] = 'bar'
    return event


    # Rename event
    # event.rename(newName)

