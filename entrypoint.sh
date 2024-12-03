#!/bin/bash

if [ "$RUN_MODE" = "pipeline" ]; then
    echo "Running pipeline..."
    python run_pipeline.py
else
    echo "Starting Mage AI server..."
    mage start . --host 0.0.0.0 --port ${PORT:-6789}
fi