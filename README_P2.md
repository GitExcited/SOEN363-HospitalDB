# MongoDB Setup - Phase 2

## Quick Start
```bash
# Start MongoDB
docker compose -f docker-compose-mongo.yml up -d

# Test connection
python connection_script.py

# Stop MongoDB
docker compose -f docker-compose-mongo.yml down
```

## Connection Details

- **Host:** localhost
- **Port:** 27018
- **Username:** admin
- **Password:** admin
- **Database:** hospital_nosql_db

## Requirements
```bash
pip install pymongo
```

## Files

- `docker-compose-mongo.yml` - MongoDB container configuration
- `connection_script.py` - Connection test script
