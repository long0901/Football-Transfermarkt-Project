# End-to-end data pipeline transfermarkt football project

Quick Links: [Looker Studio Data Visualization](https://lookerstudio.google.com/reporting/965238c0-aa82-4db1-91b6-af37fc7bdece)

![Players Dashboard](https://github.com/long0901/Football-Transfermarkt-Project/blob/main/img/clubs.png)

## Table of Contents

- [Introduction](#introduction)
- [Problem description and Project objective](#problem-description-and-project-objective)
- [Tech Stack](#tech-stack)
- [Dataset](#dataset)
- [Data Pipeline](#data-pipeline)
- [Step-by-Step Implementation](#step-by-step-implementation)

---

## Introduction

This project aims to build a comprehensive end-to-end data pipeline focused on analyzing football data from Transfermarkt. By leveraging cloud infrastructure, orchestration tools, and distributed computing technologies, the project demonstrates how modern data engineering practices can be applied to collect, process, and visualize large-scale sports data.

## Problem Description and Project Objective

Football stands as one of the most globally followed sports, generating a massive and ever-growing volume of data related to players, teams, tournaments, and transfers. However, unlocking the full potential of this data for analytical purposes presents several challenges:

🔍 Fragmented Data Sources: Football data is often dispersed across multiple platforms, making unified access and analysis difficult.

🧹 Data Quality Issues: Raw datasets commonly suffer from inconsistencies, duplicates, and missing values, requiring robust preprocessing.

⚙️ Scalability Limitations: Manual data processing or analysis on local systems does not scale well with growing data volumes.

📊 Lack of Actionable Insights: Without effective visualization, even clean data may fail to provide meaningful insights to stakeholders.

By implementing a structured ELT pipeline with cloud technologies, distributed computing, and automated workflows, this project facilitates efficient data processing, storage, and visualization, delivering valuable insights into football statistics.

## Dataset

You can access the dataset on Kaggle [here](https://www.kaggle.com/datasets/davidcariboo/player-scores). 60,000+ games from many seasons on all major competitions
400+ clubs from those competitions
30,000+ players from those clubs
400,000+ player market valuations historical records
1,200,000+ player appearance records from all games.

## Tech Stack

- **Cloud Platform:** Google Cloud Platform (GCP)
- **Infrastructure as Code:** Terraform
- **Orchestration:** Apache Airflow
- **Data Processing:** PySpark
- **Data Transformation:** dbt Cloud
- **Storage & Warehousing:** Google Cloud Storage (GCS), BigQuery
- **Visualization:** Looker Studio

## Data Pipeline

![Pipeline Diagram](https://github.com/long0901/Football-Transfermarkt-Project/blob/main/img/pipeline.png)
The data pipeline follows a structured ELT (Extract, Load, Transform) process:

1. **Extract**: Download raw csv football data from Kaggle and store it in Google Cloud Storage (GCS).
2. **Process and Load**:
   - A **PySpark job** is submitted to process and clean the data.
   - It reads the raw CSV files from GCS.
   - Performs necessary processing, cleaning, and filtering on the raw data.
   - The cleaned data is then loaded into **BigQuery** as tables.
3. **Transform**: Utilize dbt to transform the data in BigQuery, creating staging, intermediate, and mart models.
4. **Visualize**: Build dashboards and insights in Looker Studio to visualize the results.

## Step-by-Step Implementation

### 1. Setting up GCP

- Created a **Google Cloud Platform (GCP)** new project.

### 2. Infrastructure Setup with Terraform

- Provisioned **Google Cloud Storage (GCS) bucket** and **BigQuery datasets** using [Terraform script](https://github.com/long0901/Football-Transfermarkt-Project/tree/main/terraform).

### 3. Developing Data Processing & Transformation Scripts

- Developed a **PySpark script** to process raw data from gcs and upload it to Bigquery in tables format.
  - 📜 [PySpark script](https://github.com/long0901/Football-Transfermarkt-Project/tree/main/PySpark)
- Created a **dbt Cloud project** and built staging, intermediate, and mart models to prepare data for analysis.
  - 📂 [dbt full project](https://github.com/long0901/Football-Transfermarkt-Project/tree/main/dbt_project)
  - Included **tests and documentation** to ensure data quality and pipeline reliability.
    ![dbt lineage](https://github.com/long0901/Football-Transfermarkt-Project/blob/main/img/dbt.png)

### 4. Automating the Data Pipeline with Airflow

Once the data processing and transformation scripts were developed, **Apache Airflow** was used to automate the end-to-end pipeline

- Set up **Astronomer Airflow Docker** by following [this tutorial](https://academy.astronomer.io/path/airflow-101/local-development-environment).
  - 📂 [Airflow setup directory](https://github.com/long0901/Football-Transfermarkt-Project/tree/main/airflow_astro_docker)
- Configured necessary connections in Airflow UI (**Kaggle, GCP, and dbt Cloud**).

#### 🔹 DAG Workflow

1. **Download Dataset from Kaggle** → Fetches football player statistics via the Kaggle API.
2. **Upload Dataset to GCS** → Stores raw data in a **Google Cloud Storage (GCS) bucket**.
3. **Process Data with PySpark on Dataproc** → Runs a PySpark job on **Google Dataproc** to clean and preprocess data before loading it into **BigQuery**.
4. **Transform Data using dbt** → Runs dbt models to structure and optimize data in **BigQuery**.

🔗 **[View the full Airflow DAG](https://github.com/long0901/Football-Transfermarkt-Project/blob/main/airflow_astro_docker/dags/project_dag.py)**

### 5. Visualization with Looker Studio

- Created an interactive **Looker Studio dashboard** to display key insights from the data processed and transformed.
- The dashboard is divided into **5 pages**, each providing specific insights and enabling data exploration:
  1. **Players Dashboard**:
     - Users can filter by **Player** and **Season** to view detailed statistics for a specific player during a given season or across multiple seasons.
  2. **Clubs Dashboard**:
     - Users can filter by **Club** and **Season** to explore statistics for a specific club during a given season or across multiple seasons.
  3. **Competitions Dashboard**:
     - Allows filtering by **Competition** and **Season** to view competition-specific statistics, including performance data for each season.
  4. **Transfers Dashboard**:
     - Displays transfer-related data, allowing users to filter by **Season** to see transfers trends.
  5. **Total Goals**:
     - Provides **Total Goals** by **Country**.
- You can access the full Looker studio report [here](https://lookerstudio.google.com/reporting/965238c0-aa82-4db1-91b6-af37fc7bdece) (if my trial GCP account still active :v).
