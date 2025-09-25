import json
import numpy as np
import pandas as pd

def convert_hospital_jsonl_dataset(jdata):
    with open(jdata, 'r') as json_file:
        lines = json_file.read().splitlines()
        
    # One JSON data per element in a tuple
    jsons_objs = tuple(json.loads(json_line) for json_line in lines) 
    
    # Main method of getting each JSON line into one row of df
    dfs = []

    for obj in jsons_objs:
        data = pd.json_normalize(obj)
        dfs.append(data)

    # Convert JSONL to dataframe
    df = pd.concat(dfs, ignore_index = True)
    
    # Cleaning Phone Numbers
    phones = df['npi_address.phone_numbers'].tolist()
    
    hold_phones = []

    for i in phones:
        if len(i) == 0:
            hold_phones.append(None)
        elif len(i) == 1:
            hold_phones.append(*i)
            
    df['npi_address.phone_numbers'] = hold_phones
    phone_df = pd.json_normalize(df['npi_address.phone_numbers'])
    final_df = df.merge(phone_df, left_index = True, right_index = True)
    final_df.rename(columns = {'created_at' : 'npi_address_phone_numbers.created_at', 'number' : 'npi_address_phone_numbers.number', 'updated_at' : 'npi_address_phone_numbers.updated_at'}, inplace = True)
    
    # Cleaning NPIs
    final_df = final_df.explode("npis")
    final_df = final_df.drop(columns = ['licenses', 'networks', 'npi_address.phone_numbers'])
    
    return final_df

# Example Usage
jsonl = "11152023_100CAhospital_sample.jsonl"
cleaned_data = convert_hospital_jsonl_dataset(jsonl)
cleaned_data.to_csv("converted_hospital_sample.csv", index = False)
