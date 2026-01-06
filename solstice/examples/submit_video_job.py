#!/usr/bin/env python3

# Copyright 2025 nurion team
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Submit video processing job to a running Ray cluster.

This script submits the video slice workflow to a Ray cluster using the Ray Job API.
It keeps the job running long enough to inspect the WebUI.

Usage:
    1. Start Ray cluster:
       ray start --head --port=6379 --dashboard-port=8265 --include-dashboard=true

    2. Submit job:
       python examples/submit_video_job.py
"""

import os
import sys
import time
from pathlib import Path

from ray.job_submission import JobSubmissionClient


def main():
    # Get current directory
    solstice_dir = Path(__file__).parent.parent.absolute()
    
    # Connect to Ray
    client = JobSubmissionClient("http://127.0.0.1:8265")
    
    print(f"Submitting job to Ray cluster at http://127.0.0.1:8265")
    
    # Define runtime environment
    runtime_env = {
        "working_dir": str(solstice_dir),
        "excludes": [
            "**/.venv/**", 
            ".venv/**", 
            "**/__pycache__/**", 
            "**/.git/**",
            "**/java/**",
            "**/raydp/jars/**",
            "**/tests/testdata/resources/videos/**",  # Exclude large video files
            "**/*.mp4",
            "**/*.mkv",
            "**/*.lance",
        ],
        "pip": [
            "tansu-py", "pyarrow>=18.1.0", "pandas>=2.0.0", "click>=8.1.7",
            "fsspec[s3]>=2024.6.0", "pylance>=0.38.0", "sqlalchemy>=2.0.0",
            "py-spy>=0.4.1", "fastapi>=0.115.0", "uvicorn>=0.34.0",
            "jinja2>=3.1.0", "sse-starlette>=1.8.0", "slatedb>=0.8.1",
            "prometheus-client>=0.20.0"
        ],
    }
    
    # Entrypoint command
    # Run the video slice demo with extended timeout to keep WebUI active
    entrypoint = (
        "python examples/video_slice_demo.py "
        "--job-id long_running_video_job "
        "--wait-time 3600"  # Custom arg we'll add to demo script
    )
    
    # Submit job
    job_id = client.submit_job(
        entrypoint=entrypoint,
        runtime_env=runtime_env,
    )
    
    print(f"Job submitted successfully! Job ID: {job_id}")
    print(f"Stream logs with: ray job logs {job_id} -f")
    print("\nWebUI will be available at:")
    print(f"  Portal:     http://localhost:8000/solstice/")
    print(f"  Job Detail: http://localhost:8000/solstice/jobs/long_running_video_job/")
    
    # Wait and stream logs using CLI command (simpler than async in sync script)
    print("Streaming logs (Ctrl+C to stop logs, job continues)...")
    os.system(f"ray job logs {job_id} -f")


if __name__ == "__main__":
    main()

