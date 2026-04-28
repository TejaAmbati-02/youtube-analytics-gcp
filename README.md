# 📊 YouTube Analytics with Kafka & ksqlDB

A real-time data engineering pipeline that extracts YouTube video analytics (views, likes, comments, etc.) using the **YouTube Data API v3**, streams the data into **Apache Kafka**, and enables real-time SQL-based querying via **ksqlDB** — all orchestrated with **Docker Compose** using the **Confluent Platform**.

---

## 🏗️ Architecture Overview

```
YouTube Data API v3
        │
        ▼
  youtube_analytics.py
  (Python Producer)
        │
        ▼
  Apache Kafka Topic
  (youtube_videos)
        │
        ▼
  ksqlDB Stream
  (Real-time SQL Queries)
        │
        ▼
  Confluent Control Center
  (Monitoring & Management UI)
```

### Data Flow

1. **Python Producer** (`youtube_analytics.py`) calls the YouTube Data API to fetch all videos in a playlist along with their statistics (views, likes, comments, favorites, thumbnail).
2. Each video's data is serialized as **JSON** and published to a **Kafka topic** called `youtube_videos`.
3. **ksqlDB** registers a stream on top of that Kafka topic, enabling real-time SQL queries over the streaming data.
4. **Confluent Control Center** provides a browser-based UI to monitor brokers, topics, connectors, and ksqlDB.

---

## 📁 Project Structure

```
youtube-analytics-gcp/
│
├── config/
│   └── config.local          # API credentials (gitignored)
│
├── connectors/               # Custom Kafka Connect connectors (mounted into container)
│
├── constants.py              # Loads API key and playlist ID from config
├── youtube_analytics.py      # Main producer script: fetches YouTube data → Kafka
│
├── docker-compose.yaml       # Full Confluent Platform stack definition
├── creating_ksql_db.txt      # ksqlDB stream creation SQL and explanation
├── commands.txt              # Quick reference for pipenv commands
│
├── Pipfile                   # Python dependency definitions
├── Pipfile.lock              # Locked dependency versions
└── .gitignore                # Excludes secrets and config from version control
```

---

## ⚙️ Tech Stack

| Tool / Technology          | Role                                                         |
|---------------------------|--------------------------------------------------------------|
| **Python 3.11**           | Producer script language                                     |
| **YouTube Data API v3**   | Source of video statistics                                   |
| **kafka-python**          | Python Kafka client for producing messages                   |
| **Apache Kafka (CP 7.4)** | Distributed message broker / event streaming platform        |
| **Zookeeper**             | Kafka cluster coordination                                   |
| **Confluent Schema Registry** | Centralized schema management (Avro support)            |
| **Kafka Connect**         | Framework for streaming data between Kafka and other systems |
| **ksqlDB**                | Stream processing using SQL on top of Kafka topics           |
| **Confluent Control Center** | Web UI for monitoring and managing the Kafka ecosystem   |
| **Docker Compose**        | Container orchestration for all services                     |
| **Pipenv**                | Python virtual environment and dependency management         |

---

## 🚀 Getting Started

### Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) installed and running
- [Python 3.11](https://www.python.org/downloads/) installed
- [Pipenv](https://pipenv.pypa.io/en/latest/) installed (`pip install pipenv`)
- A valid **YouTube Data API v3** key ([Get one here](https://console.cloud.google.com/))

---

### 1. Clone the Repository

```bash
git clone https://github.com/<your-username>/youtube-analytics-gcp.git
cd youtube-analytics-gcp
```

---

### 2. Configure API Credentials

Create the config file (it is gitignored to protect your secrets):

```bash
mkdir -p config
```

Create `config/config.local` with the following content:

```ini
[youtube]
yt_api_key = YOUR_YOUTUBE_API_KEY_HERE
playlist_id = YOUR_YOUTUBE_PLAYLIST_ID_HERE
```

> **⚠️ Important:** Never commit `config/config.local` to version control. It is already listed in `.gitignore`.

---

### 3. Set Up the Python Environment

```bash
# Create a Python 3.11 virtual environment
pipenv --python 3.11

# Activate the virtual environment
pipenv shell

# Install dependencies
pipenv install
```

**Python dependencies (`Pipfile`):**

| Package         | Purpose                                            |
|----------------|----------------------------------------------------|
| `requests`     | HTTP calls to the YouTube Data API                 |
| `configparser` | Reads credentials from `config/config.local`       |
| `kafka-python` | Kafka producer client to publish messages          |

---

### 4. Start the Confluent Platform (Docker)

Launch all services with Docker Compose:

```bash
docker-compose up -d
```

This spins up the following containers:

| Container           | Port(s)       | Description                                      |
|--------------------|---------------|--------------------------------------------------|
| `zookeeper`        | `2181`        | Coordinates Kafka brokers                        |
| `broker`           | `9092`, `9101`| Kafka broker (client: 9092, JMX: 9101)          |
| `schema-registry`  | `8081`        | Avro schema registry                             |
| `control-center`   | `9021`        | Confluent web management UI                      |
| `connect`          | `8083`        | Kafka Connect framework                          |
| `ksqldb-server`    | `8088`        | ksqlDB REST API and query engine                 |
| `ksqldb-cli`       | —             | Interactive CLI for ksqlDB queries               |

Wait for all containers to become healthy before proceeding (check with `docker-compose ps`).

---

### 5. Run the YouTube Producer

With your virtual environment active and Docker containers running:

```bash
python youtube_analytics.py
```

The script will:
1. Iterate over every video in the configured YouTube playlist
2. Fetch per-video statistics from the YouTube API
3. Format and publish each video's data as a JSON message to the `youtube_videos` Kafka topic

**Sample output:**
```
Sent  Python Full Course for Beginners
Sent  Data Engineering with Kafka
...
```

---

### 6. Create the ksqlDB Stream

Open the ksqlDB CLI to interact with the server:

```bash
docker exec -it ksqldb-cli ksql http://ksqldb-server:8088
```

Create the stream on top of the `youtube_videos` Kafka topic:

```sql
CREATE STREAM youtube_videos (
  video_id  VARCHAR KEY,
  title     VARCHAR,
  likes     INTEGER,
  comments  INTEGER,
  views     INTEGER,
  favorites INTEGER,
  thumbnail VARCHAR
) WITH (
  KAFKA_TOPIC  = 'youtube_videos',
  PARTITIONS   = 1,
  VALUE_FORMAT = 'json'
);
```

Once created, you can query in real time:

```sql
-- Stream all records as they arrive
SELECT * FROM youtube_videos EMIT CHANGES;

-- Top 10 most viewed videos
SELECT title, views FROM youtube_videos
ORDER BY views DESC
LIMIT 10
EMIT CHANGES;
```

---

## 🔍 Key File Explanations

### `constants.py`

Reads API credentials from the config file using Python's `configparser` and exposes them as module-level constants:

```python
import configparser, os

parser = configparser.ConfigParser()
parser.read(os.path.join(os.path.dirname(__file__), "config/config.local"))

YOUTUBE_API_KEY = parser.get("youtube", "yt_api_key")
PLAYLIST_ID     = parser.get("youtube", "playlist_id")
```

These constants are imported by `youtube_analytics.py` to authenticate API calls.

---

### `youtube_analytics.py`

The core producer script. Key functions:

| Function              | Description                                                                   |
|----------------------|-------------------------------------------------------------------------------|
| `fetch_page()`       | Makes a single HTTP GET request to a YouTube API endpoint with given params   |
| `fetch_page_lists()` | Handles **pagination** — loops through all pages using `nextPageToken`        |
| `format_response()`  | Extracts and normalizes relevant fields (title, likes, comments, views, etc.) |
| `__main__`           | Orchestrates: creates Kafka producer → fetches playlist → publishes each video |

**Message schema published to Kafka:**
```json
{
  "title":     "Video Title",
  "likes":     12345,
  "comments":  678,
  "views":     999999,
  "favorites": 0,
  "thumbnail": "https://i.ytimg.com/vi/<video_id>/default.jpg"
}
```
The Kafka message **key** is the `video_id` string, which ensures all updates for a given video land in the same partition.

---

### `docker-compose.yaml`

Defines the full **Confluent Platform** stack. Key design details:

- All services share a custom Docker network called `confluent` for internal communication.
- Services use **health checks** with `depends_on` conditions to ensure correct startup order:
  `zookeeper` → `broker` → `schema-registry` → `connect` / `ksqldb-server` → `control-center`
- Kafka Connect mounts the `./connectors/` directory at `/usr/share/custom-connectors` inside the container, allowing you to drop in custom connector JARs.
- The broker is configured with `REPLICATION_FACTOR: 1` throughout — suitable for local single-node development.

---

### `creating_ksql_db.txt`

Contains the ksqlDB stream DDL and a detailed explanation of what a ksqlDB **Stream** is:

- A stream is an **unbounded, immutable sequence of events** — it doesn't store state.
- The `CREATE STREAM` statement **registers a schema** over an existing Kafka topic so ksqlDB can interpret raw bytes as structured rows.
- `video_id VARCHAR KEY` designates the Kafka message key as the stream's key column.

---

## 🌐 Monitoring with Confluent Control Center

Open your browser and navigate to:

```
http://localhost:9021
```

From here you can:
- View and browse Kafka topics and their messages
- Monitor consumer group lag
- Manage Kafka Connect connectors
- Run ksqlDB queries interactively
- View Schema Registry schemas

---

## 🔒 Security Notes

- **API Key:** Your YouTube API key is stored in `config/config.local`, which is excluded from Git via `.gitignore`. **Never hardcode credentials** in Python files.
- **Kafka:** This setup uses `PLAINTEXT` (no TLS/auth) — appropriate only for local development.

---

## 🛑 Stopping the Stack

```bash
# Stop all containers
docker-compose down

# Stop and remove all volumes (clears all Kafka data)
docker-compose down -v
```

---

## 🧩 Extending the Project

Here are some natural next steps to expand this pipeline:

- **Add a Kafka Connect BigQuery/GCS Sink** — stream processed data into Google BigQuery or Cloud Storage for long-term storage and BI dashboards.
- **Add Avro serialization** — use the Schema Registry's Avro converter instead of plain JSON for schema evolution support.
- **Scheduled ingestion** — run `youtube_analytics.py` on a cron schedule or using Apache Airflow to continuously refresh video stats.
- **ksqlDB aggregations** — create materialized tables that compute running totals of views/likes per video.
- **Grafana dashboards** — connect Grafana to ksqlDB or BigQuery to visualize trends over time.

---

## 📌 Quick Reference

```bash
# Start virtual environment
pipenv shell

# Install dependencies
pipenv install

# Start all Docker services
docker-compose up -d

# Run the YouTube producer
python youtube_analytics.py

# Open ksqlDB CLI
docker exec -it ksqldb-cli ksql http://ksqldb-server:8088

# Stop all services
docker-compose down
```

---

## 📄 License

This project is for educational and portfolio purposes.
