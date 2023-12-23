# Spotify Data Processing ETL Pipeline on AWS


## Project Structure

<img src="screenshots/diagram.png" alt="Alt text" width="1400">

## Overview

## Tools Used
- **Jupyter Notebooks:** Utilized for initial data exploration, analysis, and establishing a baseline for data extraction.
- **Amazon S3**: Used as a data lake to store raw and processed data from Spotify.
- **AWS Lambda**: Executes code to perform specific functions, such as triggering ETL processes and handling events.
- **AWS Glue Catalog**: Serves as a metadata repository for storing table definitions and schema information for processed data.
- **Amazon Athena**: Allows for querying data directly from Amazon S3 using SQL-like queries.
- **AWS EventBridge**: Enables event-driven architecture for triggering ETL jobs, allowing for automation based on specified events.
- **AWS CloudWatch**: Monitors and logs activities within the pipeline, providing insights and alerts.

## Getting Started
1. Clone this repository.
2. Review the Jupyter Notebooks in the `notebooks` directory for initial exploration and setup.
3. Explore the Python scripts in the `lambda_functions` directory for AWS Lambda functions.
4. <TODO: add steps for creating new AWS services>

## License
This project is licensed under MIT License - see the [LICENSE](LICENSE) file for details.
