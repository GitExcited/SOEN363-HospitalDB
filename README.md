# SOEN363-HospitalDB
A relational database modeling the complete journey of hospital patients — from admission to ICU stays, diagnoses, and clinical notes — designed using the MIMIC-III dataset.

## Quick Start

**Prerequisites**: Docker and Docker Compose installed

### Start the Database

```bash
docker-compose up -d
```

This will:
- Create and start a PostgreSQL 15 container
- Automatically create all tables from the schema
- Load all CSV data into the database
- Be ready to query in ~10-30 seconds (depending on data size)

### Stop the Database

```bash
docker-compose down
```

To also remove all data:
```bash
docker-compose down -v
```

## Connection Details

Use these credentials to connect with any SQL client:

| Setting  | Value         |
|----------|---------------|
| Host     | `localhost`   |
| Port     | `5432`        |
| Database | `hospital_db` |
| Username | `admin`       |
| Password | `admin`       |

### Using Beekeeper Studio

1. Open Beekeeper Studio
2. Click **New Connection**
3. Select **PostgreSQL**
4. Enter the connection details above
5. Click **Test** then **Connect**

### Using DBeaver

1. Open DBeaver
2. Click **New Database Connection** (plug icon)
3. Select **PostgreSQL**
4. Enter the connection details above
5. Click **Test Connection** then **Finish**

### Using psql CLI

```bash
psql -h localhost -p 5432 -U admin -d hospital_db
```

When prompted, enter password: `admin`

## Database Schema

The database contains 6 main tables:

- **patients** - Patient demographics and death information
- **admissions** - Hospital admission records
- **icustays** - ICU stay records
- **noteevents** - Clinical notes and documentation
- **diagnoses_icd** - Patient diagnosis codes
- **d_icd_diagnoses** - ICD-9 diagnosis code definitions

## Project Structure

```
SOEN363-HospitalDB/
├── database/
│   ├── init/
│   │   ├── 01-schema.sql      # Table definitions
│   │   └── 02-load-data.sql   # Data loading
│   └── data/                  # CSV files
├── docker-compose.yml
└── README.md
```
