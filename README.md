# Pinterest Data Pipeline (Local Kafka Simulation + Spark Analysis)

## Table of Contents

- [Project Description](#project-description)
- [Objectives](#objectives)
- [What I Learned](#what-i-learned)
- [Technologies Used](#technologies-used)
- [Installation](#installation)
- [Usage](#usage)
- [API Endpoints](#api-endpoints)
- [Project Structure](#project-structure)
- [License](#license)

## Project Description

This project simulates a real-time data pipeline that streams Pinterest data using PostgreSQL, FastAPI, and Kafka, with a full analytical pipeline built using PySpark in Databricks. It combines local data simulation with large-scale distributed data processing to mimic production-grade pipelines.

The pipeline:
- Streams random data from PostgreSQL via FastAPI into Kafka topics
- Uses Kafka Connect to write topic data into local files (JSON sink)
- Imports that data into Spark for exploration and analysis in Databricks

## Objectives

- Simulate a real-time data stream using Kafka and FastAPI
- Extract, transform and analyze Pinterest user data using PySpark
- Calculate key user engagement and demographic metrics using Spark transformations
- Practice using Spark aggregations, `approxQuantile` for medians, joins, and groupBy
- Demonstrate local-first architecture that scales to cloud infrastructure

## What I Learned

- Kafka and FastAPI integration with PostgreSQL for message simulation
- FastAPI endpoints for sending/receiving Kafka messages
- Kafka Connect standalone setup with JSON sink configuration
- How to analyze large datasets using PySpark
  - Filtering, joining, and aggregating data at scale
  - Creating age groups from user ages
  - Calculating median values using `approxQuantile` (since Spark doesn’t support grouped medians natively)
  - Deriving metrics like most popular categories, top influencers per country, and temporal trends

## Technologies Used

- **Backend & Streaming**
  - Python 3
  - PostgreSQL
  - FastAPI
  - Kafka
  - Kafka Connect (JSON sink)
  - SQLAlchemy
  - Kafka-Python
  - PyYAML

- **Big Data & Analysis**
  - PySpark
  - Databricks

## Installation

1. **Clone the repository**

    ```bash
    git clone https://github.com/your-username/pinterest_data_pipeline.git
    cd pinterest_data_pipeline
    ```

2. **Set up a Python virtual environment (recommended)**

    ```bash
    python -m venv venv
    source venv/bin/activate
    ```

3. **Install dependencies**

    ```bash
    pip install -r requirements.txt
    ```

4. **Install and run Kafka locally**

    Ensure Kafka and Zookeeper are running on your machine at `localhost:9092`. You can use the official Kafka binaries or Docker.

## Usage

### 1. Configure PostgreSQL credentials

Create a file named `local_db_creds.yaml` in the root of the project:

```yaml
HOST: your_postgres_host
USER: your_postgres_user
PASSWORD: your_postgres_password
DATABASE: your_database_name
PORT: your_postgres_port
```

### 2. **Run the pipeline**

    ```bash
    python user_posting_emulation.py
    ```

    This will:
    - Start a FastAPI web server on [http://localhost:8000](http://localhost:8000)
    - Begin streaming random rows from your PostgreSQL tables into Kafka topics.

### 3. Run data analysis in Databricks

Open databricks_integration.pynb inside Databricks, which:

    - Loads the streamed data (from JSON files)
    - Cleans and transforms the data
    - Performs analysis such as:

        - Most popular post categories per age group
        - Median follower counts per year and demographic
        - Top influencers by country
        - Category popularity trends over time

## API Endpoints

- **POST** `/send_data?topic=your_topic`  
  Sends JSON data to the specified Kafka topic.

- **GET** `/get_data?topic=your_topic`  
  Returns a message from the specified topic for testing.

## Project Structure

```pinterest_data_pipeline/
├── user_posting_emulation.py       # Main script for PostgreSQL to Kafka simulation
├── databricks_integration.pynb     # Full Spark data cleaning and analysis workflow
├── local_db_creds.yaml             # Local DB credentials (not committed)
├── README.md                       # Project documentation
├── requirements.txt                # Python dependencies
└── .gitignore                      # Files to ignore (including credentials)
```
## License

This project is licensed under the MIT License. See the LICENSE file for details.
