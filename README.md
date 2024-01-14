# FINAL PROJECT IYKRA

Online Payment Fraud Detection Monitoring &amp; Algorithm Evaluation

# Background

Tema dari final project saya adalah melakukan analisis terhadap data fraud bank dengan tujuan untuk mengevaluasi algorithm machine learning fraud yang dibuat oleh developer sebelumnya dan juga pola - pola transaksi fraud yang biasa terjadi dari data transaksi fraud selama satu bulan sehingga dapat memunculkan insight mengenai transaksi fraud apa yang sering terjadi, jenis transaksi, dan dalam nominal berapa fraud bisa terjadi. Sehingga expected output dari final project ini adalah menghasilkan sebuah dashboard yang menampilkan informasi - informasi tersebut melalui sebuah pipeline yang menggunakan arsitektur batch processing dan streaming processing.

Original dataset: [here](https://www.kaggle.com/datasets/rupakroy/online-payments-fraud-detection-dataset)

Presentation: [here](https://www.canva.com/design/DAFynbVrqqc/b0p8SDB-V1FuMwbpYvuvMA/edit?utm_content=DAFynbVrqqc&utm_campaign=designshare&utm_medium=link2&utm_source=sharebutton_)

# Objective
The objectives of this projects are described below:

Create an automated pipeline to flow both batch and stream data from data sources to data warehouses and data marts.
Create a visualization dashboard to get insights from the data, which can be used for business decisions.
Create an infrastructure as code which makes the codes reusable and scalable for another projects.

# Tools
- Orchestration: Airflow
- Compute : Virtual Machine (VM) instance
- Container : Docker
- Storage: Google Cloud Storage
- Warehouse: BigQuery
- Data Visualization: Looker

# Data Pipeline
![image](https://github.com/ridhohrnf/finalproject-df10/assets/63965187/64689c46-1724-42b0-91b9-c7e710ed45de)

# visualisasi
![image](https://github.com/ridhohrnf/finalproject-df10/assets/63965187/a560e01f-624f-484a-a050-061613950591)


![image](https://github.com/ridhohrnf/finalproject-df10/assets/63965187/8b77bb17-eb84-4aeb-b6fc-f3303c5ac4cd)

# Google Cloud Usage Billing Report
Data infrastructure we used in this project are entirely built on Google Cloud Platform with more or less 3 weeks of project duration, using this following services:

- Google Cloud Storage (pay for what you use)
- Virtual Machine (VM) instance (cost are based Vcpu & memory and storage disk)
- Google BigQuery (first terrabyte processed are free of charge)
- Google Looker Studio (cost is based from number of Looker Blocks (data models and visualizations), users, and the number of queries processed per month)

**Total cost around 6$ out of 300$ free credits that GCP provided**
