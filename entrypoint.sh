#!/bin/bash
set -e

echo "[$(date)] Starting quiz scheduler..."
exec python -u main.py
