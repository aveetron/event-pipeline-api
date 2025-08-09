# Data Pipeline Broker API

A FastAPI-based service for publishing and consuming topics via RabbitMQ with support for multiple service types and ClickHouse integration.

## Features

- Publish topic messages to RabbitMQ
- Consume messages from RabbitMQ and route to appropriate service handlers
- Support for multiple service types: QI, Analytics, and Export
- ClickHouse integration for data retrieval
- Comprehensive monitoring and management endpoints
- Direct service triggering for testing

## Prerequisites

- Python 3.11+
- Docker (for RabbitMQ and ClickHouse)
- RabbitMQ server (can use Docker Compose)
- ClickHouse database (configured in core/config.py)

## Don't forget to check ruff and isort for isort

- ruff check
- isort .

## Setup

1. **Clone the repository**

   ```sh
   git clone <your-repo-url>
   cd data-pipeline-broker-api
   ```

2. **Install dependencies**

   ```sh
   pip install -r requirements.txt
   ```

3. **Start RabbitMQ using Docker Compose**

   ```sh
   docker-compose up -d
   ```

4. **Configure environment variables**

   The application uses environment variables for configuration. A `.env` file is provided with default values.
   
   - Edit `.env` to customize settings for your environment
   - Key environment variables:
     ```
     # RabbitMQ Settings
     RABBITMQ_URL=amqp://pipeline:pipeline@localhost:5672/
     EXCHANGE_NAME=
     QUEUE_NAME=pipeline_queue
     ROUTING_KEY=pipeline
     
     # ClickHouse Cloud Settings
     CLICKHOUSE_HOST=0.0.0.0
     CLICKHOUSE_PORT=8443
     CLICKHOUSE_DATABASE=pipeline
     CLICKHOUSE_USERNAME=default
     CLICKHOUSE_PASSWORD=your_password
     
     # Additional connection settings are available in the .env file
     ```

## Running the API

### Option 1: Run locally

```sh
uvicorn main:app --reload
```

### Option 2: Run with Docker Compose

This will start both the RabbitMQ service and the API service using the environment variables from the .env file.

```sh
docker-compose up -d
```

The API will be available at http://localhost:8000

## API Endpoints

### Core Endpoints
- `GET /health` — Service health check with detailed service status
- `POST /publish-topic` — Publish a topic to RabbitMQ

### Monitoring Endpoints
- `GET /message-count` — Get number of processed messages
- `GET /service-stats` — Get statistics about processed messages by service type
- `GET /consumer-status` — Get detailed consumer status

### Management Endpoints
- `POST /restart-consumer` — Restart the consumer service

### Direct Service Endpoints (for testing)
- `POST /service/qi` — Manually trigger the QI service
- `POST /service/analytics` — Manually trigger the Analytics service
- `POST /service/export` — Manually trigger the Export service

## Example: Publish a Topic

```sh
curl -X POST http://localhost:8000/publish-topic \
  -H "Content-Type: application/json" \
  -d '{
    "integration_id": "123456",
    "service_type": "qi"
  }'
```

## Example: Trigger a Service Directly

```sh
curl -X POST http://localhost:8000/service/analytics \
  -H "Content-Type: application/json" \
  -d '{
    "integration_id": "123456",
    "service_type": "analytics"
  }'
```

## Stopping Services

```sh
docker-compose down
```

## Project Structure

- `main.py` — FastAPI app, consumer logic, and service handlers
- `config/rabbitmq.py` — RabbitMQ client for message publishing and consuming
- `config/clickhouse.py` — ClickHouse client for data retrieval
- `core/config.py` — Configuration settings
- `model/topic.py` — Data models for requests and responses
- `docker-compose.yml` — Docker Compose configuration for services

## Service Types

### QI (Query Inside)
Retrieves log data from ClickHouse based on integration ID.

### Analytics
Processes analytics data for the specified integration ID.

### Export
Exports data for the specified integration ID in various formats.

## License