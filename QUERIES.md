# Running Queries

## Method 1: Using a SQL Client (Beekeeper Studio, DBeaver, etc.)

1. Click **New Query**
2. Write your SQL in the editor
3. Press **Ctrl+Enter** to run

## Method 2: Run SQL Files via Docker

We've provided sample queries in `sql/sample_queries.sql`. Run them:
```bash
docker exec -i hospital_db psql -U admin -d hospital_db < sql/sample_queries.sql
```

Or create your own SQL file and run it:
```bash
docker exec -i hospital_db psql -U admin -d hospital_db < sql/your_queries.sql
```
