# Weekly Top 100 Streamers Data Pipeline

This repository contains a data engineering pipeline that pulls data weekly from the [Streams Charts API](https://streams-charts.gitbook.io/streams-charts-live-streaming-api-docs/free-request). The pipeline extracts information about the top 100 streamers based on hours watched at 8 AM UTC every Monday.

## Overview

The project leverages modern data engineering tools to automate the collection, transformation, and storage of weekly streaming data. The key components include:

- **Mage AI**: Used for orchestrating the data pipeline, tracking runs, and ensuring reliability.
- **PostgreSQL**: Serves as the backend for Mage AI to store metadata and pipeline execution logs.
- **MotherDuck**: Acts as the data warehouse for storing and analysing the collected data.

## Pipeline Workflow

1. **Data Extraction**: The pipeline queries the Streams Charts API to fetch the top 100 streamers of the week.
2. **Data Transformation**: Any necessary transformations are performed before storage.
3. **Data Loading**: Transformed data is appended to the MotherDuck data warehouse for storage and further analysis.
4. **Orchestration and Scheduling**: Mage AI schedules the pipeline to run automatically every Monday at 8 AM UTC.


## Requirements

To set up and run the pipeline, ensure you have the following:

- Python 3.12+
- Docker
- Poetry
- MotherDuck setup with a Read/Write Access Token
- Streams Charts API Access


## Setup Instructions

1. **Clone the Repository**:
   ```bash
   git clone https://github.com/SRoadknight/streams-charts-t100-pipeline.git
   cd streams-charts-pipeline
   ```

2. **Create .env**:
    - Create .env from .env.example

3. **Configure API Access**:
   - Obtain an API key from [Streams Charts](https://streams-charts.gitbook.io/streams-charts-live-streaming-api-docs).
   - Add your API client id and token to the `.env` file in the following format:
     
     ```env
     STREAMS_CHARTS_CLIENT_ID=your-client-id
     STREAMS_CHARTS_TOKEN=your-token
     ```

4. **Configure Database Access**:
   - Configure MotherDuck for storing the transformed data.

5. **Create and run the Docker containers**
   - Run the devcontainer or use the Docker Compose CLI to create and run the containers

## Data Schema

More information about the data schema can be found on [Streams Charts](https://streams-charts.gitbook.io/streams-charts-live-streaming-api-docs/free-request). This is further augmented with date information for the period the data is collected.

## Future Enhancements

- Improvement of the data pipeline. Primarily data validation, testing, and monitoring.

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.

## Contributions 

Contributions and suggestions are welcome!
