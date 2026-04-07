
# Data Warehouse and Analytics Project

Welcome to the **Data Warehouse and Analytics Project** repository! 🚀  
This project demonstrates a comprehensive data warehousing and analytics solution, from building a data warehouse to generating actionable insights. 
---
## 🏗️ Data Architecture

The data architecture for this project follows Medallion Architecture **Bronze**, **Silver**, and **Gold** layers:


1. **Bronze Layer**: Stores raw data as-is from the source systems. Data is ingested from CSV/Excel Files into SQL Server Database.
2. **Silver Layer**: This layer includes data cleansing, standardization, and normalization processes to prepare data for analysis.
3. **Gold Layer**: Houses business-ready data modeled into a star schema required for reporting and analytics.

---
## 📖 Project Overview

This project involves:

1. **Data Architecture**: Designing a Modern Data Warehouse Using Medallion Architecture **Bronze**, **Silver**, and **Gold** layers.
2. **ETL Pipelines**: Extracting, transforming, and loading data from source systems into the warehouse.
3. **Data Modeling**: Developing fact and dimension tables optimized for analytical queries.
4. **Analytics & Reporting**: Creating SQL-based reports and dashboards for actionable insights.


---

## 🚀 Project Requirements

### Building the Data Warehouse (Data Engineering)

#### Objective
Develop a modern data warehouse using SQL Server to consolidate sales data, enabling analytical reporting and informed decision-making.

#### Specifications
- **Data Sources**: Import data from two source systems (ERP and CRM) provided as CSV files.
- **Data Quality**: Cleanse and resolve data quality issues prior to analysis.
- **Integration**: Combine both sources into a single, user-friendly data model designed for analytical queries.
- **Scope**: Focus on the latest dataset only; historization of data is not required.
- **Documentation**: Provide clear documentation of the data model to support both business stakeholders and analytics teams.

---

### BI: Analytics & Reporting (Data Analysis)

#### Objective
Develop SQL-based analytics to deliver detailed insights into:
- **Customer Behavior**
- **Product Performance**
- **Sales Trends**

These insights empower stakeholders with key business metrics, enabling strategic decision-making.  

#### Data Architecture
## The data architecture for this project follows Medallion Architecture Bronze, Silver and Gold layers.
<img width="1058" height="761" alt="Data Architecture Diagram drawio" src="https://github.com/user-attachments/assets/22df3da3-bbf5-46b7-b884-49d3d56d31c2" />




## 📂 Repository Structure
```
data-warehouse-project/
│
├── datasets/                           # Raw datasets used for the project (ERP and CRM data)
│
├── docs/                               # Project documentation and architecture details
│   ├── etl.drawio                      # Draw.io file shows all different techniquies and methods of ETL
│   ├── data_architecture.drawio        # Draw.io file shows the project's architecture
│   ├── data_catalog.md                 # Catalog of datasets, including field descriptions and metadata
│   ├── data_flow.drawio                # Draw.io file for the data flow diagram
│   ├── data_models.drawio              # Draw.io file for data models (star schema)
│   ├── naming-conventions.md           # Consistent naming guidelines for tables, columns, and files
│
├── scripts/                            # SQL scripts for ETL and transformations
│   ├── bronze/                         # Scripts for extracting and loading raw data
│   ├── silver/                         # Scripts for cleaning and transforming data
│   ├── gold/                           # Scripts for creating analytical models
│
├── tests/                              # Test scripts and quality files
│
├── README.md                           # Project overview and instructions
├── LICENSE                             # License information for the repository
├── .gitignore                          # Files and directories to be ignored by Git
└── requirements.txt                    # Dependencies and requirements for the project
```
---



## 🛡️ License

This project is licensed under the [MIT License](LICENSE). You are free to use, modify, and share this project with proper attribution.

## 🌟 About Me: 


👋 Hi, I'm Zain UL Abideen | Data Engineer

I am passionate about Building scalable data pipelines that turn raw data into actionable insights

🎓 Computer Engineering Graduate | 💡 Data Engineering Enthusiast | ☁️ Cloud-Native Advocate

About Me:
I'm a data engineer who loves transforming messy data into clean, reliable pipelines. My passion lies in architecting robust data solutions that scale—whether it's processing terabytes in Spark or optimizing complex SQL queries that make databases sing.

🔧 Tech Stack:

• Python 
• SQL
• Big Data: Apache Spark 
• Databricks
• Cloud: Microsoft Azure

Specialties: 
• ETL/ELT Pipelines 
• Data Warehousing 
• Data Design Architecture 
• System Design 
• Performance Optimization 

🚀 What I'm About:

📊 Building end-to-end data solutions from ingestion to visualization
⚡ Optimizing Spark jobs to squeeze out every bit of performance
🏗️ Designing cloud-native architectures on Azure
🧩 Turning complex data problems into elegant engineering solutions

📫 Let's Connect!
I'm always excited to collaborate on data engineering projects or discuss the latest in distributed computing. 

Email: abideenz095@gmail.com

