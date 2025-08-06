# Data Pipeline Broker API

A FastAPI-based service for publishing and consuming topics via RabbitMQ.

## Features

- Publish topic messages to RabbitMQ
- Consume messages from RabbitMQ and print them
- Health and status endpoints

## Prerequisites

- Python 3.11+
- Docker (for RabbitMQ)
- RabbitMQ server (can use Docker Compose)

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

   - Edit `.env` if needed (default RabbitMQ URL is `amqp://pipeline:pipeline@localhost:5672/`).

## Running the API

```sh
uvicorn main:app --reload
```

## API Endpoints

- `GET /health` — Service health check
- `POST /publish-topic` — Publish a topic to RabbitMQ
- `GET /message-count` — Get number of processed messages
- `GET /consumer-status` — Get consumer status

## Example: Publish a Topic

```sh
curl -X POST http://localhost:8000/publish-topic \
  -H "Content-Type: application/json" \
  -d '{
    "service": "example_service",
    "date_from": "2024-01-01",
    "date_to": "2024-01-31",
    "parameters": {"foo": "bar"}
  }'
```

## Stopping Services

```sh
docker-compose down
```

## Project Structure

- `main.py` — FastAPI app and consumer logic
- `config/rabbitmq.py` — RabbitMQ client
- `core/config.py` — Configuration
- `model/topic.py` — Topic models

## License