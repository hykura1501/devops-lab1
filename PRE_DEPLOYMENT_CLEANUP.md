# Pre-Deployment Cleanup Guide

## Files and Directories to Delete Before Moving to a New Computer

### 1. Generated/Cache Directories
```
📁 Delete these directories:
- airflow/logs/*           # Airflow execution logs
- airflow/__pycache__/    # Python cache files
- airflow/dags/__pycache__/
- dbt/logs/*              # DBT log files
- dbt/target/*            # DBT compiled files
- dbt/.user.yml           # DBT user-specific settings
- sqlserver/data/*        # SQL Server runtime data (if exists)
- sqlserver/logs/*        # SQL Server logs (if exists)
```

### 2. Environment and Local Config Files
```
📄 Delete or recreate these files:
- .env                    # Environment variables (recreate with new values)
- dbt/profiles.yml       # DBT connection profiles (may need new credentials)
```

### 3. Docker-Related Files
```
🐳 Delete these Docker-generated files:
- .docker/               # Docker cache directory
- docker.env            # Docker environment file
- docker-compose.override.yml  # Local Docker overrides
```

### 4. Database Files
```
📊 Delete these if present:
- postgres_data/        # Local PostgreSQL data
- mssql_data/          # Local SQL Server data
```

### 5. Temporary Files
```
🗑️ Delete these temporary files:
- *.pyc                 # Python compiled files
- .DS_Store            # Mac system files
- Thumbs.db            # Windows system files
- *.log                # Any log files
- *.tmp                # Temporary files
```

## Files to Keep

### 1. Essential Project Files
```
✅ DO NOT delete these files:
- docker-compose.yml    # Main Docker configuration
- Dockerfile           # Container build instructions
- requirements.txt     # Python dependencies
- dbt_project.yml     # DBT project configuration
- packages.yml        # DBT package dependencies
- restore-db.sh       # Database initialization script
- sqlserver/init/*    # Initial database setup scripts
- sqlserver/backup/*  # Database backup files (if needed for setup)
```

### 2. Source Code
```
✅ Keep these directories:
- airflow/dags/*.py    # Your Airflow DAG definitions
- dbt/models/          # Your DBT models
- dbt/macros/         # Your DBT macros
- dbt/tests/          # Your DBT tests
```

### 3. Documentation
```
✅ Keep these files:
- README.md
- SETUP.md
- TROUBLESHOOTING.md
- docs/
```

## Pre-Deployment Checklist

1. **Stop All Services**
```bash
docker-compose down
```

2. **Clean Docker Environment**
```bash
# Remove all containers and volumes
docker-compose down -v

# Clean Docker system
docker system prune -a

# Remove all volumes
docker volume prune
```

3. **Clean Python Cache**
```bash
# Remove all .pyc files and __pycache__ directories
find . -type f -name "*.pyc" -delete
find . -type d -name "__pycache__" -exec rm -r {} +
```

4. **Clean DBT Files**
```bash
# Remove DBT generated files
rm -rf dbt/target/
rm -rf dbt/logs/
rm -f dbt/.user.yml
```

5. **Clean Environment Files**
```bash
# Remove environment files (backup if needed)
rm -f .env
rm -f docker.env
```

## After Moving to New Computer

1. **Create Fresh Environment Files**
```bash
# Create new .env file
cp .env.example .env
# Edit with new values
```

2. **Set Up Directory Structure**
```bash
mkdir -p airflow/logs
mkdir -p airflow/plugins
mkdir -p dbt/logs
```

3. **Set Correct Permissions**
```bash
# For Windows (PowerShell as Administrator):
icacls * /reset
icacls * /grant Everyone:F /t

# For Linux/Mac:
chmod -R 755 ./dbt
chmod -R 755 ./airflow
chmod 644 docker-compose.yml
```

4. **Start Fresh Build**
```bash
docker-compose build --no-cache
docker-compose up -d
```

## Special Considerations for Database Files

### SQL Server Setup Files
1. **What to Keep**:
   - `restore-db.sh` (Essential for database initialization)
   - Initial backup files or .bak files
   - Database creation scripts
   - Sample data scripts

2. **What to Delete**:
   - Runtime data files
   - Transaction logs
   - Temporary database files
   - Generated backup files

3. **sqlserver Directory Structure**:
```
sqlserver/
├── init/           # KEEP: Initial setup scripts
├── backup/         # KEEP: Original backup files
├── data/          # DELETE: Runtime data
└── logs/          # DELETE: Runtime logs
``` 