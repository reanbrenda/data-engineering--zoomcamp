
# Railway Movement Analytics Platform

A comprehensive data engineering solution that captures and analyzes real-time train movement data from the UK rail network, providing insights through an advanced analytics pipeline.

## Project Overview

This platform establishes a robust data infrastructure that connects to Network Rail's live data feed to capture comprehensive information about:
- Real-time train movements and schedules
- Platform assignments and operational delays
- Service variations and route modifications
- Performance metrics and operational analytics

## System Architecture

```
Network Rail Data Feed → Stream Processor → Data Lake → Analytics Warehouse → Business Intelligence
     (Real-time)        (Kafka)         (MinIO)    (BigQuery)        (Reports & Dashboards)
```

## Technology Stack

- **Stream Processing**: Apache Kafka for real-time message handling
- **Data Storage**: MinIO as local data lake, BigQuery as cloud data warehouse  
- **Workflow Orchestration**: Apache Airflow for ETL and data pipeline management
- **Data Transformation**: dbt for data modeling and business logic implementation
- **Infrastructure Management**: Terraform for cloud resource provisioning
- **Development Environment**: Docker for containerized local development

## Platform Components

### Core Data Processing
- **Data Ingestion Engine**: `railway_movement_ingestion.py` - Connects to Network Rail and streams to Kafka
- **Data Processing Engine**: `process_railway_movements_to_lake.py` - Processes Kafka messages into MinIO

### Infrastructure Services
- **Local Development Stack**: Kafka, Zookeeper, Schema Registry, Airflow, MinIO
- **Cloud Analytics Platform**: BigQuery dataset for data warehousing and analytics
- **Data Pipeline Orchestration**: Airflow workflows for data loading and transformation

### Data Analytics
- **Raw Data Storage**: MinIO buckets with time-partitioned data architecture
- **Data Transformation**: dbt models for data cleaning, aggregation, and business metrics
- **Analytics Ready Data**: BigQuery tables optimized for business intelligence and reporting

## Getting Started

### Prerequisites
- Docker Desktop running
- Python 3.11+ with pip
- Google Cloud Platform account
- Network Rail data feed access

### Initial Setup

1. **Clone and navigate to project**
   ```bash
   cd railway-analytics-platform
   ```

2. **Set up credentials**
   ```bash
   # Copy and edit secrets file
   cp secrets.json.example secrets.json
   # Add your Network Rail credentials
   ```

3. **Configure Google Cloud**
   ```bash
   # Update terraform variables
   cd terraform
   # Edit terraform.tfvars with your GCP details
   ```

### Running the Platform

1. **Start infrastructure services**
   ```bash
   # Create required directories
   mkdir -p mnt/dags mnt/logs mnt/plugins tmp
   
   # Launch all services
   docker compose up -d
   ```

2. **Set up cloud resources**
   ```bash
   cd terraform
   terraform init
   terraform plan
   terraform apply
   ```

3. **Configure connections**
   - MinIO: http://localhost:9001 (minio/minio123)
   - Airflow: http://localhost:8080 (airflow/airflow)
   - Kafka: http://localhost:9021

4. **Trigger ETL workflow**
   - Go to Airflow UI
   - Enable and trigger the BigQuery loading DAG

## Container Management

### Using Helper Scripts

#### Linux/Mac (Shell Script)
```bash
# Make script executable
chmod +x run_movement_consumer.sh

# Build and run container
./run_movement_consumer.sh run

# View logs
./run_movement_consumer.sh logs

# Check status
./run_movement_consumer.sh status

# Stop container
./run_movement_consumer.sh stop

# Restart container
./run_movement_consumer.sh restart

# Clean up resources
./run_movement_consumer.sh cleanup

# Show help
./run_movement_consumer.sh help
```

#### Windows (Batch File)
```cmd
# Build and run container
run_movement_consumer.bat run

# View logs
run_movement_consumer.bat logs

# Check status
run_movement_consumer.bat status

# Stop container
run_movement_consumer.bat stop

# Restart container
run_movement_consumer.bat restart

# Clean up resources
run_movement_consumer.bat cleanup

# Show help
run_movement_consumer.bat help
```

### Direct Docker Commands

```bash
# Build and run container
docker compose up -d

# View logs (follow mode)
docker compose logs -f

# View last 100 lines of logs
docker compose logs --tail=100

# Check container status
docker compose ps

# Show resource usage
docker stats

# Stop container
docker compose down

# Restart container
docker compose restart

# Clean up (remove volumes and images)
docker compose down --volumes --remove-orphans
docker system prune -f

# Build image without cache
docker compose build --no-cache

# Execute command in container
docker compose exec movement-consumer python -c "print('Hello from container')"

# Open shell in container
docker compose exec movement-consumer /bin/bash
```

### Container Health and Monitoring

```bash
# Check container health
docker compose ps

# View real-time resource usage
docker stats

# Inspect container configuration
docker compose config

# View error logs only
docker compose logs | grep -i error

# Check container logs with timestamps
docker compose logs -t
```

## Data Flow Architecture

1. **Data Ingestion**: Network Rail ActiveMQ → Kafka topics
2. **Stream Processing**: Kafka → MinIO with time-based partitioning (year/month/day)
3. **Data Pipeline**: MinIO → BigQuery via Airflow orchestration
4. **Data Transformation**: dbt models for analytics-ready data preparation
5. **Business Intelligence**: BigQuery for reporting, analysis, and dashboard creation

## Project Structure

```
├── mnt/dags/                    # Airflow workflow definitions
├── railway_analytics/           # dbt data transformation models
├── terraform/                   # Cloud infrastructure configuration
├── playground/                  # Development and testing scripts
├── requirements.txt             # Python package dependencies
├── docker-compose.yml          # Local service orchestration
├── railway_movement_ingestion.py # Data ingestion script
└── process_railway_movements_to_lake.py # Data processing script
```

## Monitoring and Operations

### Service Health Monitoring
- **Kafka**: http://localhost:9021 - Topic monitoring and message flow
- **Airflow**: http://localhost:8080 - Workflow execution and logs
- **MinIO**: http://localhost:9001 - Data lake storage and buckets

### Common Operational Issues
- **Port conflicts**: Ensure 8080, 9021, 9001, 9092 are available
- **Memory requirements**: Minimum 4GB RAM for local development
- **Dependencies**: Install librdkafka for Kafka Python client
- **Credentials**: Verify Network Rail account and GCP service account

## Development Workflow

1. **Local Development**: Use Docker services for testing and development
2. **Data Validation**: Check MinIO buckets for data flow and quality
3. **Pipeline Testing**: Trigger Airflow DAGs manually for validation
4. **Schema Evolution**: Update dbt models as data structure evolves
5. **Production Deployment**: Use Terraform for cloud resource management

## Next Steps

After successful setup:
- Monitor real-time data flow in Kafka
- Schedule automated ETL jobs in Airflow
- Build custom dbt models for business metrics
- Create dashboards using BigQuery data
- Scale infrastructure for production workloads

## Contributing

This is a personal project for learning data engineering concepts. The architecture demonstrates modern data stack patterns including streaming, data lakes, and cloud data warehousing.
