import os
import pandas as pd 

def load_local_data(file_path='../data/bronze/jobs.csv'):
    
    if os.path.exists(file_path): 
        data = pd.read_csv(file_path, index_col=0)  
    else:
        data = pd.DataFrame()
    return data


def unload_local_data(data, file_path='../data/bronze/jobs.csv'):
    data.to_csv(file_path, index=True)   

