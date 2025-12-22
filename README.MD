# HELIOS
**High-throughput Event-Level Infrastructure for Optimized Scheduling**

A distributed event processing framework for scientific computing workloads.

## Overview

HELIOS is a containerized batch processing system designed for parallel analysis of event-based datasets. Inspired by computing workflows at large research facilities, the system provides a control plane for job orchestration and an execution plane for stateless worker processes.

The framework emphasizes reproducibility, fault isolation, and infrastructure-driven scalability rather than domain-specific analytics or real-time processing.

## Motivation

High-energy physics experiments and large-scale scientific facilities generate massive volumes of event data that require parallel processing for efficient analysis. HELIOS models the essential components of such workflows:

- Independent batch job submission and tracking
- Parallel execution across multiple compute units
- Deterministic result aggregation
- Container-based reproducibility
- Clear separation of control and compute logic

## Architecture

### Control Plane
**API Service** - FastAPI-based REST interface for job management
- Job submission and lifecycle tracking
- Progress monitoring and result aggregation
- System health and statistics endpoints

**Job Manager** - In-memory orchestrator managing job state transitions
- `submitted → running → completed | failed`
- Chunk assignment and progress tracking
- Resource allocation coordination

### Execution Plane
**Worker Processes** - Stateless compute units processing event subsets
- Independent chunk processing
- Partial statistics computation
- Deterministic output generation

### Data Flow

```
Input Dataset → Job Submission → Chunk Distribution → Parallel Workers → Result Aggregation
```

## Scientific Computing Analogy

| Concept | HELIOS Component |
|---------|------------------|
| Detector event data | CSV event records |
| Batch job | REST API job submission |
| Computing node | Worker process |
| Job scheduler | Job Manager |
| Partial results | Per-chunk statistics |
| Final reconstruction | Aggregated metrics |

## Features

- Batch-oriented job execution model
- Data-parallel event processing with configurable parallelism
- Two execution backends:
  - **Multiprocessing**: Shared-memory parallelism on single node
  - **Subprocess**: Process-isolated execution with intermediate artifacts
- Asynchronous job execution with real-time progress tracking
- RESTful API for programmatic job control
- Docker-based deployment for reproducibility

## Data Model

Event records contain:
- `event_id` - Unique event identifier
- `timestamp` - Event occurrence time
- `sensor_id` - Detector/sensor identifier
- `energy` - Measured energy value
- `status` - Event classification

Computed statistics:
- Total event count
- Status distribution
- Per-sensor event counts
- Average energy
- High-energy event frequency

## API Reference

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/` | System health check |
| GET | `/health` | Detailed service status |
| POST | `/jobs/submit` | Submit processing job |
| GET | `/jobs/{job_id}/status` | Query job state |
| GET | `/jobs/{job_id}/result` | Retrieve results |
| GET | `/jobs` | List all jobs |
| DELETE | `/jobs/{job_id}` | Cancel running job |
| GET | `/stats` | System statistics |

## Installation

### Prerequisites
- Python 3.11+
- Docker and Docker Compose

### Local Development

```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
uvicorn main:app --reload --port 8000
```

### Container Deployment

```bash
# Build containers
docker-compose build

# Start API service
docker-compose up api

# Run in background
docker-compose up -d api

# View logs
docker-compose logs -f api
```

## Usage

### Generate Test Dataset

```bash
python generate_data.py
```

Creates synthetic event data at `data/raw/events.csv`.

### Submit Processing Job

```bash
curl -X POST http://localhost:8000/jobs/submit \
  -H "Content-Type: application/json" \
  -d '{
    "input_file": "data/raw/events.csv",
    "num_workers": 4,
    "method": "multiprocessing"
  }'
```

Response:
```json
{
  "job_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
  "status": "submitted",
  "message": "Job submitted successfully"
}
```

### Monitor Job Execution

```bash
curl http://localhost:8000/jobs/{job_id}/status
```

### Retrieve Results

```bash
curl http://localhost:8000/jobs/{job_id}/result
```

Example output:
```json
{
  "job_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
  "status": "completed",
  "results": {
    "total": 1000000,
    "status_counts": {
      "active": 750000,
      "inactive": 250000
    },
    "sensor_counts": {
      "S001": 333333,
      "S002": 333334,
      "S003": 333333
    },
    "avg_energy": 75.42,
    "high_energy_events": 234100,
    "chunks_processed": 4,
    "method": "multiprocessing",
    "num_workers": 4
  }
}
```

## Execution Models

### Multiprocessing (Default)
Utilizes Python's `multiprocessing.Pool` for shared-memory parallelism.

**Characteristics:**
- Low overhead
- Optimal for CPU-bound tasks
- Single-node execution

**Configuration:**
```json
{
  "method": "multiprocessing",
  "num_workers": 4
}
```

### Subprocess Isolation
Launches independent worker processes with stronger isolation.

**Characteristics:**
- Process-level fault isolation
- Intermediate result artifacts
- Models distributed execution semantics

**Configuration:**
```json
{
  "method": "subprocess",
  "num_workers": 4
}
```

## Project Structure

```
helios/
├── scripts/
│   ├── data/
│   │   ├── raw/              # Input datasets
│   │   └── processed/        # Processing artifacts
│   ├── main.py               # FastAPI application
│   ├── job_manager.py        # Job lifecycle management
│   ├── compute.py            # Parallel processing engine
│   ├── worker.py             # Worker process implementation
│   ├── generate_data.py      # Dataset generator
│   ├── test_api.py           # Integration tests
│   ├── Dockerfile            # API container image
│   ├── Dockerfile.worker     # Worker container image
│   ├── docker-compose.yml    # Local orchestration
│   └── requirements.txt      # Python dependencies
└── README.md
```

## Performance Characteristics

- Worker count should match available CPU cores for optimal throughput
- Data chunking limits memory footprint per worker
- Subprocess execution trades I/O overhead for stronger isolation
- Throughput scales linearly with worker count for CPU-bound workloads
- Network-bound workloads may exhibit sub-linear scaling

## Testing

### API Integration Tests
```bash
python test_api.py
```

### Worker Validation
```bash
docker-compose --profile worker-test up worker
```

### Manual Worker Execution
```bash
python worker.py data/raw/events.csv 0 1000 0 data/processed/chunks/test.json
```

## Configuration

Job submission parameters:
- `input_file` - Path to input CSV dataset
- `num_workers` - Parallel worker count (default: 4)
- `method` - Execution backend: `multiprocessing` or `subprocess`

Environment variables:
- `HELIOS_DATA_DIR` - Data directory path
- `HELIOS_LOG_LEVEL` - Logging verbosity
- `HELIOS_MAX_WORKERS` - Maximum concurrent workers

## Current Scope

- Single-node execution with multiprocessing and subprocess backends
- In-memory job state management
- Local filesystem for data and intermediate results
- RESTful API for job control and monitoring
- Docker-based containerization for reproducibility

## Acknowledgments

This project draws inspiration from batch processing systems used at:
- CERN Computing Infrastructure
- Large Hadron Collider experiments
- Distributed computing workflows in high-energy physics

## License

MIT