import pandas as pd
import os
import asyncio
from dotenv import load_dotenv
from helper import run_sql, table_exists, create_table_from_df
from supabase import create_client
from prefect import task, flow

# --- CONFIG ---
load_dotenv()
DATA_DIR = r'imdb_raw_data'
SUPABASE_URL = os.getenv("SUPABASE_PROJECT_URL")
SUPABASE_KEY = os.getenv("SUPABASE_ANON_KEY")

@task(name="Load Gzip TSV")
def load_imdb_file(file_name):
    path = os.path.join(DATA_DIR, file_name)
    print(f"Loading {file_name}...")
    # Note: Removing nrows limit for the "Full ETL"
    return pd.read_csv(path, sep='\t', low_memory=False, 
                       na_values='\\N', compression='gzip',
                       nrows=10000)

@task(name="Transform IMDb Data")
def transform_data(df_akas, df_crew, df_ratings, df_basics, df_names):
    print("Starting Transformation...")

    # 1. Basics
    df_basics = df_basics.drop(columns=['isAdult']).copy()
    df_basics['startYear'] = pd.to_numeric(df_basics['startYear'], errors='coerce').astype('Int64')
    df_basics['endYear'] = pd.to_numeric(df_basics['endYear'], errors='coerce').astype('Int64')
    df_basics['runtimeMinutes'] = pd.to_numeric(df_basics['runtimeMinutes'], errors='coerce').astype('Int64')

    string_cols = ['tconst', 'titleType', 'primaryTitle', 'originalTitle', 'genres']
    for col in string_cols:
        if col == 'genres':
            df_basics[col] = df_basics[col].fillna('Unknown')
        df_basics[col] = df_basics[col].astype('string')

    # 2. Ratings
    df_ratings['tconst'] = df_ratings['tconst'].astype('string')
    df_ratings['averageRating'] = df_ratings['averageRating'].astype('float64')
    df_ratings['numVotes'] = df_ratings['numVotes'].astype('Int64')

    # 3. Crew
    df_crew['tconst'] = df_crew['tconst'].astype('string')
    df_crew['directors'] = df_crew['directors'].astype('string')
    df_crew['writers'] = df_crew['writers'].astype('string')

    # 4. Names
    df_names['birthYear'] = pd.to_numeric(df_names['birthYear'], errors='coerce').astype('Int64')
    df_names['deathYear'] = pd.to_numeric(df_names['deathYear'], errors='coerce').astype('Int64')
    df_names['nconst'] = df_names['nconst'].astype('string')
    df_names['primaryName'] = df_names['primaryName'].astype('string')

    # 5. Bridge Professions (needs primaryProfession - must be before df_names trim)
    bp = df_names[['nconst', 'primaryProfession']].dropna().copy()
    bp['primaryProfession'] = bp['primaryProfession'].str.split(',')
    bridge_professions = bp.explode('primaryProfession').reset_index(drop=True)
    bridge_professions.columns = ['nconst', 'profession']
    bridge_professions = bridge_professions.astype('string')

    # 6. Bridge Known For (needs knownForTitles - must be before df_names trim)
    bkf = df_names[['nconst', 'knownForTitles']].dropna().copy()
    bkf['knownForTitles'] = bkf['knownForTitles'].str.split(',')
    bridge_known_for = bkf.explode('knownForTitles').reset_index(drop=True)
    bridge_known_for.columns = ['nconst', 'tconst']
    bridge_known_for = bridge_known_for.astype('string')

    # NOW trim df_names to only columns that exist in Supabase table
    df_names = df_names[['nconst', 'primaryName', 'birthYear', 'deathYear']].copy()

    # 7. Akas
    df_akas = df_akas[['titleId', 'ordering', 'title', 'region', 'language', 'isOriginalTitle']].copy()
    df_akas[['titleId', 'title', 'region', 'language']] = df_akas[['titleId', 'title', 'region', 'language']].astype('string')
    df_akas['ordering'] = df_akas['ordering'].astype('int64')
    df_akas['isOriginalTitle'] = pd.to_numeric(df_akas['isOriginalTitle'], errors='coerce').fillna(0).astype('int64')
    df_akas['region'] = df_akas['region'].fillna('Unknown')
    df_akas['language'] = df_akas['language'].fillna('Unknown')

    # 8. Bridge Crew
    dirs = df_crew[['tconst', 'directors']].dropna().copy()
    dirs['directors'] = dirs['directors'].str.split(',')
    dirs = dirs.explode('directors').rename(columns={'directors': 'nconst'})
    dirs['category'] = 'director'

    wris = df_crew[['tconst', 'writers']].dropna().copy()
    wris['writers'] = wris['writers'].str.split(',')
    wris = wris.explode('writers').rename(columns={'writers': 'nconst'})
    wris['category'] = 'writer'

    bridge_crew = pd.concat([dirs, wris]).reset_index(drop=True).astype('string')

    # 9. Fact Master
    master_df = pd.merge(df_basics, df_ratings, on='tconst', how='inner')

    return {
        'dim_titles': df_basics,
        'fact_ratings': df_ratings,
        'dim_names': df_names,
        'dim_akas': df_akas,
        'bridge_professions': bridge_professions,
        'bridge_known_for': bridge_known_for,
        'bridge_crew': bridge_crew,
        'fact_master_titles': master_df
    }

@task(name="Export to Supabase", retries=2, retry_delay_seconds=30)
def export_to_supabase(data_dict):
    if not SUPABASE_URL or not SUPABASE_KEY:
        raise ValueError("SUPABASE_PROJECT_URL or SUPABASE_ANON_KEY not found in environment variables!")
    
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    print("Supabase client created successfully!")

    for table_name, df in data_dict.items():
        print(f"Processing {table_name}... ({len(df)} rows)")
        records = df.to_dict(orient="records")
        batch_size = 1000
        for i in range(0, len(records), batch_size):
            batch = records[i:i + batch_size]
            supabase.table(table_name).upsert(batch).execute()
            print(f"   {table_name}: uploaded rows {i} to {i + len(batch)}")
        print(f" Done: {table_name}")

@flow(name="IMDb Full ETL")
async def imdb_flow():
    # 1. Load
    akas = load_imdb_file("title.akas.tsv.gz")
    crew = load_imdb_file("title.crew.tsv.gz")
    ratings = load_imdb_file("title.ratings.tsv.gz")
    basics = load_imdb_file("title.basics.tsv.gz")
    names = load_imdb_file("name.basics.tsv.gz")

    # 2. Transform
    transformed = transform_data(akas, crew, ratings, basics, names)

    # 3. Export
    export_to_supabase(transformed)

if __name__ == "__main__":
    imdb_flow.serve(
        name="imdb-local-deployment",
        tags=["on-premise", "etlpython imdb_pipeline.py"]
    )