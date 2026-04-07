# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Quack-Cluster is a serverless distributed SQL query engine built with **Ray**, **DuckDB**, and **FastAPI**. It executes SQL queries across a Ray cluster where each worker runs an embedded DuckDB instance for parallel data processing. Supports Parquet, CSV, and JSON files from local storage or object storage (S3, GCS).

## Common Commands

```bash
# Build and start cluster with N workers
make up scale=N

# Stop cluster (safe)
make down

# Run tests
make test

# Lint and format
make lint

# Generate sample data
make data

# Clean (delete volumes)
make clean

# Local development (outside Docker)
pdm run start        # Start FastAPI server
pdm run test         # Run pytest
pdm run lint         # Run ruff
```

## Architecture

The system uses a **Planning → Execution** separation:

```
coordinator.py (FastAPI Gateway)
    ↓ parses SQL, resolves CTEs/subqueries
planner.py (Strategy Selection)
    ↓ creates declarative ExecutionPlan
executor.py (Ray Orchestration)
    ↓ distributes work
worker.py (Ray Actor + DuckDB)
```

### Query Flow

1. **Coordinator** receives SQL via `/query` endpoint, parses with SQLGlot
2. **Planner** analyzes AST and selects execution strategy:
   - `LocalExecutionPlan` - no table access needed
   - `DistributedScanPlan` - single table scan across workers
   - `DistributedShuffleJoinPlan` - partitioned join (map-shuffle-reduce)
   - `DistributedBroadcastJoinPlan` - small table broadcast to all workers
3. **Executor** orchestrates Ray workers and aggregates results
4. **Workers** (`DuckDBWorker` Ray actors) execute queries on data partitions

### Key Files

- `quack_cluster/coordinator.py` - FastAPI endpoints, CTE/subquery resolution, caching
- `quack_cluster/planner.py` - AST analysis, strategy selection, file discovery
- `quack_cluster/executor.py` - Ray task orchestration, result aggregation
- `quack_cluster/worker.py` - Ray actor with DuckDB for data processing
- `quack_cluster/execution_plan.py` - Pydantic models defining plan contracts
- `quack_cluster/settings.py` - Configuration (shuffle partitions, broadcast threshold, retries)

### Adding New Features

To add a new join strategy:
1. Define a new plan class in `execution_plan.py` (e.g., `DistributedBroadcastJoinPlan`)
2. Add strategy selection logic in `planner.py` `create_plan()`
3. Implement execution in `executor.py` with new `_execute_*` method
4. Add worker method in `worker.py` if needed

## Data File Discovery

Files are discovered in `sample_data/` using glob patterns:
- `table_name.*.parquet` - priority 1
- `table_name.*.csv` - priority 2
- `table_name.*.json` - priority 3

## API Endpoints

- `POST /query` - Execute SQL (body: `{"sql": "SELECT..."}`)
- `POST /explain` - Returns Graphviz plan visualization as PNG
