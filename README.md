# Database Evaluation

---

This repository is dedicated to investigating and comparing the differences in data manipulation performance—specifically focusing on read, write, and update operations—as well as resource utilization between NoSQL and NewSQL database systems. For this study, **MongoDB** will be used as the representative NoSQL database, while **CockroachDB** will serve as the representative NewSQL database. The goal is to gain insights into how each system handles various operations under different workloads, and to evaluate their efficiency, scalability, and overall suitability for modern data-driven applications.

---

## Setup

To set up the project, the user must first install all the required dependencies listed in the `requirements.txt` file. This can be done by running the following command in the terminal:

`pip install -r requirements.txt`

## CockroachDB

**To initiate the database:**

1. **Host the database** on port `26257`
2. **Initialize the DB**

   `$env:ZONEINFO = "C:\Program Files\Go\lib\time\zoneinfo.zip" cockroach start-single-node --insecure`
3. **Create the necessary table** by executing the CREATE TABLE SQL commands in the `table.sql` file.
4. **Insert the initial dataset** by running the `upload_data.py` script under CockroachDB_Code:

   `python upload_data.py`

Following these steps will set up and populate the database so it's ready for use.


**To run any of the benchmarks:**

Execute the corresponding Python script using the following command:

`python <benchmark_name>.py`

Replace <benchmark_name> with the specific benchmark you want to execute.

After the benchmark completes, the output image will be automatically saved in the `CockroachDB_Images` folder for review.


## MongoDB

To initiate the database:

1. Download MongoDB Compass
2. Initiaite a local connection
3. Insert Initial dataset by running the upload_data.py script under MongoDB_Code:
   `python upload_data.py`

Following these steps will set up and populate the database so it's ready for use.


**To run any of the benchmarks:**

Execute the corresponding Python script using the following command:

`python <benchmark_name>.py`

Replace <benchmark_name> with the specific benchmark you want to execute.

After the benchmark completes, the output image will be automatically saved in the `MongoDB_Images` folder for review.
