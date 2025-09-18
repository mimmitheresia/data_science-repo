import os
import pandas as pd 
import json

def load_local_data(file_path='../data/bronze/', file_name='jobs.csv'):
    
    if os.path.exists(file_path): 
        data = pd.read_csv(file_path+file_name)  
    else:
        data = pd.DataFrame()
    return data

def load_local_dict(file_path = '../data/bronze/', file_name='jobs_payloads.json'):
  
    with open(file_path+file_name, 'r', encoding='utf-8') as f:
        payload_dict = json.load(f)
    return payload_dict


def unload_local_data(data, file_path='../data/bronze/', file_name='jobs.csv'):
    data.to_csv(file_path+file_name, index=False)  


    
def unload_local_dict(payload_dict, file_path = '../data/bronze/', file_name='jobs_payloads.json'):
  
    with open(file_path+file_name, 'w', encoding='utf-8') as f:
        json.dump(payload_dict, f, ensure_ascii=False, indent=2)
