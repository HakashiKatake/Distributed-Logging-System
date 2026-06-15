#!/bin/bash

# Distributed Logging System Run Script
# Launches all services locally using the virtual environment

cd "$(dirname "$0")/.."

export PYTHONPATH="$(pwd)"

echo "============================================="
echo "Starting Distributed Logging System Stack..."
echo "============================================="

# Ensure virtual environment exists
if [ ! -d .venv ]; then
  echo "Error: .venv virtual environment not found. Please run ./scripts/setup.sh first."
  exit 1
fi

# Ensure log directory exists
mkdir -p logs

# Clean previous background logs
rm -f logs/api.log logs/worker.log logs/retention.log logs/generator.log logs/agent.log

echo "Starting FastAPI Ingestion & Search API..."
.venv/bin/uvicorn source.main:app --host 0.0.0.0 --port 8000 > logs/api.log 2>&1 &
API_PID=$!

# Wait briefly for API to spin up
sleep 2

echo "Starting Processing Worker..."
.venv/bin/python source/worker.py > logs/worker.log 2>&1 &
WORKER_PID=$!

echo "Starting Retention Manager..."
.venv/bin/python source/retention.py > logs/retention.log 2>&1 &
RETENTION_PID=$!

echo "Starting Simulated Log Generator..."
.venv/bin/python source/log_generator.py > logs/generator.log 2>&1 &
GENERATOR_PID=$!

echo "Starting Custom Log Agent..."
.venv/bin/python source/agent.py > logs/agent.log 2>&1 &
AGENT_PID=$!

echo "All services running in the background!"
echo "----------------------------------------"
echo "  Ingestion & Search API: PID $API_PID (Logs: logs/api.log)"
echo "  Processing Worker:      PID $WORKER_PID (Logs: logs/worker.log)"
echo "  Retention Manager:      PID $RETENTION_PID (Logs: logs/retention.log)"
echo "  Log Generator:          PID $GENERATOR_PID (Logs: logs/generator.log)"
echo "  Log Agent:              PID $AGENT_PID (Logs: logs/agent.log)"
echo "----------------------------------------"
echo "Services active. Press Ctrl+C to stop all services."

cleanup() {
  echo ""
  echo "Stopping all services..."
  kill $API_PID $WORKER_PID $RETENTION_PID $GENERATOR_PID $AGENT_PID 2>/dev/null
  echo "Cleaned up background processes."
  exit 0
}

# Trap Ctrl+C
trap cleanup SIGINT

# Keep script alive
while true; do
  sleep 1
done
