# 🚂 Network Rail Movement Data Engineering Project


**Project:** Real-time Network Rail movement data ingestion, processing, and analytics pipeline

## 🌟 Project Description

This project creates a real-time data pipeline that captures live train movement data from the UK rail network and transforms it into actionable business intelligence. The system ingests data via STOMP protocol, processes it through a data lake, and loads it into a cloud data warehouse for analytics and visualization.

## 🔄 How It Works

### Data Flow Architecture
```
Network Rail STOMP Feed → Python Consumer → MinIO (Data Lake) → Airflow (ETL) → BigQuery (Data Warehouse) → Looker Studio (Visualization)
```

1. **Data Ingestion**: Python consumer connects to Network Rail's live STOMP feed and receives real-time train movement updates
2. **Data Storage**: Raw data is stored in MinIO (S3-compatible storage) with time-based partitioning
3. **Data Processing**: Apache Airflow orchestrates the ETL process, transforming raw data into structured format
4. **Data Warehouse**: Processed data is loaded into Google BigQuery for analytics and reporting
5. **Visualization**: Data is visualized through Looker Studio dashboards for business insights

### Key Components
- **movement-consumer**: Python application that ingests Network Rail data
- **MinIO**: Local data lake for storing raw movement data
- **Apache Airflow**: Workflow orchestration and ETL processing
- **Google BigQuery**: Cloud data warehouse for analytics
- **Looker Studio**: Business intelligence and dashboard creation

## 🚀 Quick Start

### Prerequisites
- Docker & Docker Compose (V2)
- Google Cloud Project with BigQuery enabled
- Network Rail API credentials

### Setup Steps

#### Step 1: Clone and Configure
```bash
# Clone the project
git clone <your-repo-url>
cd dtc-data-engineering-zoomcamp-project

# Configure Network Rail credentials
cp secrets.json.example secrets.json
# Edit secrets.json with your Network Rail username and password
```

#### Step 2: Setup BigQuery
1. **Go to**: [Google Cloud Console](https://console.cloud.google.com/)
2. **Select your project** or create a new one
3. **Enable BigQuery API**
4. **Create dataset**: `networkrail`
5. **Download service account key** and save as `raildata-469117-c69f6f420e79.json` in project root

#### Step 3: Run the Pipeline
```bash
# Start all services
docker compose up -d

# Wait for containers to start (2-3 minutes)
# Check status
docker compose ps
```

#### Step 4: Configure Airflow Connections (IMPORTANT!)
**After running the containers, you MUST update Airflow connections:**

1. **Access Airflow UI**: http://localhost:8080 (admin/admin)
2. **Go to**: Admin → Variables
3. **Add Variable**: `bigquery_credential_secret`
   - **Value**: Copy entire content of `raildata-469117-c69f6f420e79.json`
4. **Add Variable**: `bigquery_project_id`
   - **Value**: `raildata-469117`

5. **Go to**: Admin → Connections
6. **Add Connection**: `minio_default`
   - **Connection Type**: Amazon Web Services
   - **Access Key ID**: `minioadmin`
   - **Secret Access Key**: `minioadmin`
   - **Extra**: `{"endpoint_url": "http://minio:9000"}`

## 📊 Dashboard Charts

The project creates four key visualizations that provide comprehensive insights into Network Rail performance:

### Chart 1: Punctuality Distribution (Donut Chart)
**Purpose**: Shows overall network health and performance at a glance
**Data**: Percentage breakdown of ON TIME, LATE, and EARLY trains
**Insight**: Quick executive summary of network reliability

### Chart 2: TOC Performance Ranking (Horizontal Bar Chart)
**Purpose**: Ranks train operating companies by punctuality performance
**Data**: TOC performance metrics with movement counts and percentages
**Insight**: Identifies best and worst performing operators for accountability

### Chart 3: Hourly Movement Patterns (Line Chart)
**Purpose**: Reveals operational patterns throughout the day
**Data**: Movement counts by hour, showing peak and quiet periods
**Insight**: Helps with capacity planning and schedule optimization

### Chart 4: Station Performance Heatmap
**Purpose**: Geographic and temporal performance analysis
**Data**: Station performance by time of day using color-coded heatmap
**Insight**: Identifies problem stations and time-based performance issues

## 🔗 Dashboard Access

**Your Looker Studio Dashboard:** [Network Rail Performance Dashboard](https://lookerstudio.google.com/reporting/19c0f282-2a1c-4f50-a7a2-7b44239d58d4)





## 🔧 Troubleshooting

### Common Issues

#### 1. Credentials Format Error
```bash
# Check secrets.json format - must be object, not array
cat secrets.json
# Should be: {"username": "...", "password": "..."}
# NOT: [{"username": "...", "password": "..."}]
```

#### 2. Airflow Connection Issues
- **Ensure you've updated Airflow connections** after starting containers
- **Check BigQuery variables** are set correctly
- **Verify MinIO connection** is configured

#### 3. BigQuery Load Failures
- **Check Airflow variables** contain valid credentials
- **Verify BigQuery project ID** is correct
- **Ensure BigQuery API** is enabled

### Debug Commands
```bash
# Check container status
docker compose ps

# View service logs
docker compose logs -f [service-name]

# Restart specific service
docker compose restart [service-name]

# Clean restart
docker compose down --volumes
docker compose up -d
```


## 🔗 Service URLs

| Service | URL | Credentials |
|---------|-----|-------------|
| **Airflow** | http://localhost:8080 | admin/admin |
| **MinIO** | http://localhost:9000 | minioadmin/minioadmin |
| **MinIO Console** | http://localhost:9001 | minioadmin/minioadmin |
| **BigQuery** | https://console.cloud.google.com/bigquery | Google account |

## 🎯 Success Checklist

- ✅ **Pipeline Running**: movement-consumer ingesting data
- ✅ **Data Storage**: MinIO receiving movement data
- ✅ **ETL Working**: Airflow DAGs processing data
- ✅ **BigQuery Populated**: Data warehouse contains movement data
- ✅ **Dashboard Created**: Looker Studio charts displaying insights
- ✅ **Connections Configured**: Airflow properly connected to MinIO and BigQuery

## 🚀 Next Steps

1. **Test the pipeline** and verify data flow
2. **Create your Looker Studio dashboard** using the four chart types
3. **Add your dashboard link** to this README
4. **Customize queries** for specific business insights
5. **Share dashboard** with stakeholders
6. **Set up automated reporting** schedules


