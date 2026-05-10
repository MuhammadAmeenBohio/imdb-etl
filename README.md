# IMDb ETL Pipeline

A ETL pipeline that extracts IMDb datasets, transforms them into a star schema, and loads them into Supabase (PostgreSQL), orchestrated with Prefect.

## Tech Stack
- Python, Pandas, Prefect, Supabase, python-dotenv

## Data Model
8 tables following a star schema: `dim_titles`, `dim_names`, `dim_akas`, `fact_ratings`, `fact_master_titles`, `bridge_crew`, `bridge_professions`, `bridge_known_for`

## Setup

1. Clone the repo and install dependencies
```bash
git clone https://github.com/your-username/imdb-etl-pipeline.git
pip install -r requirements.txt
```

2. Download IMDb files from https://datasets.imdbws.com and place in `imdb_raw_data/`

3. Create `.env` file
```env
SUPABASE_PROJECT_URL=https://your-project-ref.supabase.co
SUPABASE_ANON_KEY=your_anon_key
SUPABASE_SERVICE_KEY=your_service_role_key
```

4. Run the SQL in `schema.sql` in your Supabase SQL Editor to create tables

5. Run the pipeline
```bash
python imdb_pipeline.py
prefect deployment run 'IMDb Full ETL/imdb-local-deployment'
```

## Notes
- Pipeline is configured for 10,000 rows per file for Supabase free tier
- Increase `nrows` in `load_imdb_file()` for larger datasets
