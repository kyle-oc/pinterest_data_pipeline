# Pinterest Data Pipeline (Local Kafka Simulation)

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

This project simulates a real-time data pipeline that streams Pinterest data using a combination of PostgreSQL, FastAPI, and Kafka. It extracts random rows from local PostgreSQL tables and sends the data to Kafka topics via a FastAPI-based REST API.

## Objectives

- Simulate real-time data flow using Kafka in a local environment.
- Learn how to integrate PostgreSQL, FastAPI, and Kafka for data streaming.
- Create a simple, testable architecture for streaming messages.

## What I Learned

- How to set up and use Kafka with Python.
- Writing FastAPI endpoints for message streaming.
- Connecting to PostgreSQL using SQLAlchemy.
- Handling UUIDs and datetimes for JSON serialization.
- Managing local pipelines for testing before scaling to cloud infrastructure.

## Technologies Used

- Python 3
- PostgreSQL
- Kafka
- FastAPI
- SQLAlchemy
- Uvicorn
- kafka-python
- PyYAML
- Databricks

## Installation

1. **Clone the repository**

    ```bash
    git clone https://github.com/your-username/pinterest_data_pipeline.git
    cd pinterest_data_pipeline
    ```

2. **Set up a Python virtual environment (optional but recommended)**

    ```bash
    python -m venv venv
    source venv/bin/activate  # On Windows: venv\Scripts\activate
    ```

3. **Install dependencies**

    ```bash
    pip install -r requirements.txt
    ```

4. **Install and run Kafka locally**

    Make sure Kafka and Zookeeper are running on your local machine at `localhost:9092`. You can use the official Kafka binaries or Docker.

## Usage

1. **Configure your PostgreSQL credentials**

    Create a file named `local_db_creds.yaml` in the root of the project:

    ```yaml
    HOST: your_postgres_host
    USER: your_postgres_user
    PASSWORD: your_postgres_password
    DATABASE: your_database_name
    PORT: your_postgres_port
    ```

2. **Run the pipeline**

    ```bash
    python user_posting_emulation.py
    ```

    This will:
    - Start a FastAPI web server on [http://localhost:8000](http://localhost:8000)
    - Begin streaming random rows from your PostgreSQL tables into Kafka topics.

## API Endpoints

- **POST** `/send_data?topic=your_topic`  
  Sends JSON data to the specified Kafka topic.

- **GET** `/get_data?topic=your_topic`  
  Returns one message from the specified topic for testing.

## Project Structure

```pinterest_data_pipeline/
├── user_posting_emulation.py       # Main script: extracts, sends and serves data
├── databricks_integration.pynb     # Databricks script: imports csv files, converts to DataFrames, then cleans
├── local_db_creds.yaml             # Local DB credentials (not committed)
├── README.md                       # Project documentation
├── requirements.txt                # Python dependencies
└── .gitignore                      # Files to ignore (should include credentials)
```
## License

This project is licensed under the MIT License. See the LICENSE file for details.
