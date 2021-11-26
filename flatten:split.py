from io.hevo.api import Event
from io.hevo.api import Utils
import json
import re


def transform(event):       
    properties = event.getProperties()     
    dict_targets = select_wrap_json(event)   
    while len(dict_targets) > 0 :    
        event = unwind_json(event , dict_targets)
        dict_targets = select_wrap_json(event)
    array_targets, array_obj_targets = select_wrap_arrays(event)     
    event = unwind_array(event,array_targets,array_obj_targets)
    
    #properties['count']=len(array_obj_targets)
    return event

# Function to scan the record for JSON objects and list them
def select_wrap_json(input_event):        
    input_properties = input_event.getProperties()   
    dict_targets = [] 
    for key in input_properties.keys():
        if "varchar" in Utils.getType(input_properties[key]):
            if re.match("\\{", input_properties[key]):
                dict_targets.append(key)               
        if Utils.isDict(input_properties[key]):
            dict_targets.append(key)  
    
    return dict_targets


# Function to scan the record for Array and list them
def select_wrap_arrays(input_event):        
    input_properties = input_event.getProperties()   
    array_obj_targets=[]
    array_targets = []
    for key in input_properties.keys():
        if "varchar" in Utils.getType(input_properties[key]):
            if re.match("\\[", input_properties[key]):
                if re.match("\\[{", input_properties[key]):
                    array_obj_targets.append(key)
                else :
                    array_targets.append(key) 
        if input_event.getType(key) == 'array':                
            array_obj_targets.append(key)    
    
    return array_targets, array_obj_targets


#Function to flatten the JSON fields
def unwind_json(input_event, targets):            
    input_properties = input_event.getProperties()    
    for target in targets:
        if Utils.isDict(input_properties[target]):
            nested = input_properties[target]
        else:
            nested = Utils.jsonStringToDict(input_properties[target])
        for key in nested.keys():
            input_properties[target+"_"+key] = nested[key]
        del input_properties[target]         
    
    return input_event


#Function to unnest the Array fields
def unwind_array(input_event, targets , obj_targets):
    properties = input_event.getProperties()
    eventName = input_event.getEventName()
    events = [input_event]
    for target in targets:
        if "varchar" in Utils.getType(properties[target]):
            new_array = json.loads(properties[target])
        else:
            new_array = properties[target]
        for i in range(len(new_array)):
            new_target = {}
            new_target['ref_id'] = properties.get('_id')
            new_target['index'] = i
            new_target[target] = new_array[i]
            events.append(Event(eventName + '_' + target, new_target))
        del properties[target]
    for target in obj_targets:
        if "varchar" in Utils.getType(properties[target]):
            new_array = json.loads(properties[target])
        else:
            new_array = properties[target]
        for i in range(len(new_array)):
            new_target = new_array[i]
            new_target['ref_id'] = properties.get('_id')
            new_target['index'] = i
            events.append(Event(eventName + '_' + target, new_target))
        del properties[target]
    
    for event in events:
        dict_targets = select_wrap_json(event)
        while len(dict_targets) > 0 :    
            event = unwind_json(event , dict_targets)
            dict_targets = select_wrap_json(event)    
    
    return events


