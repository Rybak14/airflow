from sqlalchemy import create_engine
import pandas as pd
import config
import sqlalchemy
import uuid
import os
from datetime import datetime

# from airflow.providers.postgres.hooks.postgres import PostgresHook

current_date = datetime.today().strftime('%Y%m%d')

def define_type(data):

    if data.find("College") != -1:
        return "College"
    elif data.find("University") != -1:
        return "University"
    elif data.find("Institute") != -1:
        return "Institute"
    
def transform():

    df = pd.read_csv(os.path.join(os.path.dirname(os.path.abspath(__file__)), config.CSV_FILE_DIR) +"/university_" + current_date + ".csv")

    df.rename(columns={'state-province': 'state_province'}, inplace=True)

    #create new column based on function
    df['type'] = df['name'].apply(lambda row: define_type(row))

    #choose only necessary columns from df
    df = df[["country", "alpha_two_code", "state_province", "name", "type"]]
    df.drop_duplicates(inplace=True)

    return df

def upsert_df(df: pd.DataFrame, table_name: str):

    db_psql = 'postgresql://{}:{}@{}:{}/{}'.format(config.PSQL_USER, 
                                            config.PSQL_PASSWORD, 
                                            config.PSQL_HOST,
                                            config.PSQL_PORT,
                                            config.PSQL_DB)
    
    engine = sqlalchemy.create_engine(db_psql)

    #insert dataframe into temp table
    temp_table = f"temp_{uuid.uuid4().hex[:6]}"
    df.to_sql(temp_table, engine, index=False)

    columns = list(df.columns)
    headers_sql_txt = ", ".join([f'"{i}"' for i in columns])  
    update_column_stmt = ", ".join([f'"{col}" = EXCLUDED."{col}"' for col in columns])

    query_upsert = f"""
        INSERT INTO "{table_name}" ({headers_sql_txt}) 
        SELECT {headers_sql_txt} FROM "{temp_table}"
        ON CONFLICT ON CONSTRAINT country_university 
        DO UPDATE SET {update_column_stmt};
        """
    engine.execute(query_upsert)
    engine.execute(f'DROP TABLE "{temp_table}"')

    return True

if __name__ == "__main__":

    data = transform()
    upsert_df(df=data, table_name="universities")