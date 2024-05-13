
from datetime import datetime
import config
import pandas as pd
import requests
import os

current_date = datetime.today().strftime('%Y%m%d')

def extract():
    
    url = config.UNIVERSITY_API
    data_req = requests.get(url)
    data_json = data_req.json()
    
    return data_json    

def save_to_csv(data_json):
    
    df = pd.DataFrame(data_json)    
    filename = "university_{}.csv".format(current_date)
    df.to_csv(os.path.join(os.path.dirname(os.path.abspath(__file__)), config.CSV_FILE_DIR, filename), index=False)

if __name__ == "__main__":

    api_data = extract()
    data_csv = save_to_csv(api_data)