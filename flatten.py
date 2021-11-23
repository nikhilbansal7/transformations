from io.hevo.api import Event
from io.hevo.api import Utils
import re

def transform(event):       
    properties = event.getProperties()    
    dict_targets = select_wrap_json(event)
    while len(dict_targets) > 0 :    
        dict_targets = select_wrap_json(event)
        event = unwind_json(event , dict_targets)        
    return event


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