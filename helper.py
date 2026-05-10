import requests
import os
from dotenv import load_dotenv

load_dotenv()

def run_sql(sql: str):
    service_key = os.getenv("SUPABASE_SERVICE_KEY")  # read here, not at top
    print(f"Service key loaded: {service_key[:20] if service_key else 'NONE'}")
    
    project_ref = "kznxhkljevsgqihentnl"
    headers = {
        "Authorization": f"Bearer {service_key}",
        "Content-Type": "application/json"
    }
    response = requests.post(
        f"https://api.supabase.com/v1/projects/{project_ref}/database/query",
        headers=headers,
        json={"query": sql}
    )
    if response.status_code != 200:
        raise RuntimeError(f"SQL execution failed: {response.text}")
    return response.json()

def table_exists(table_name: str) -> bool:
    sql = f"""
        SELECT EXISTS (
            SELECT 1 FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_name = '{table_name}'
        );
    """
    result = run_sql(sql)
    return result[0]["exists"]

def create_table_from_df(table_name: str, df):
    type_mapping = {
        "int64": "BIGINT",
        "int32": "INTEGER",
        "float64": "FLOAT",
        "float32": "FLOAT",
        "bool": "BOOLEAN",
        "object": "TEXT",
        "datetime64[ns]": "TIMESTAMP",
    }
    columns = []
    for col, dtype in df.dtypes.items():
        pg_type = type_mapping.get(str(dtype), "TEXT")
        columns.append(f'"{col}" {pg_type}')
    
    sql = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(columns)});"
    print(f"Creating table {table_name} with SQL:\n{sql}")
    run_sql(sql)