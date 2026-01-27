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

"""Prometheus metrics exporter for WebUI.

This module exports metrics to Prometheus for real-time monitoring.
Metrics are read from storage (SlateDB).
"""

import asyncio
from typing import TYPE_CHECKING, Any, Dict

from solstice.webui.storage.prometheus_exporter import PrometheusMetricsExporter
from solstice.utils.logging import create_ray_logger

if TYPE_CHECKING:
    from solstice.webui.storage import JobStorage


class PrometheusCollector:
    """Export metrics to Prometheus from storage.

    Reads metrics from SlateDB and exports to Prometheus.
    """

    def __init__(
        self,
        storage: "JobStorage",
        job_id: str,
    ):
        """Initialize Prometheus collector.

        Args:
            storage: JobStorage instance to read metrics from
            job_id: Job identifier
        """
        self.storage = storage
        self.job_id = job_id
        self.logger = create_ray_logger(f"PrometheusCollector-{job_id}")

        self.prometheus = PrometheusMetricsExporter(job_id)

        self._running = False
        self._last_metrics: Dict[str, Dict[str, Any]] = {}

    async def run_loop(self) -> None:
        """Main export loop."""
        self._running = True
        self.logger.info("Prometheus collector started")

        try:
            while self._running:
                self._export_metrics()
                await asyncio.sleep(1)

        except Exception as e:
            self.logger.error(f"Prometheus collector error: {e}")
        finally:
            self._running = False
            self.logger.info("Prometheus collector stopped")

    def stop(self) -> None:
        """Stop the collector."""
        self._running = False

    def _export_metrics(self) -> None:
        """Export metrics from storage to Prometheus."""
        try:
            # Get job info from storage
            job_info = self.storage.get_job_archive(self.job_id)
            if not job_info:
                return

            stages = job_info.get("stages", [])

            for stage_data in stages:
                stage_id = stage_data.get("stage_id", "")
                if not stage_id:
                    continue

                # Get runtime metrics from storage
                workers = self.storage.list_workers(self.job_id, stage_id=stage_id, limit=1000)
                worker_count = len(workers)

                # Get latest stage metrics (aggregated from splits)
                latest_metrics = self.storage.get_latest_stage_metrics(stage_id) or {}

                metrics_dict = {
                    "stage_id": stage_id,
                    "worker_count": worker_count,
                    "input_records": latest_metrics.get("input_records", 0),
                    "output_records": latest_metrics.get("output_records", 0),
                    "output_queue_size": 0,  # Not tracked in new model
                    "is_running": stage_data.get("status") == "RUNNING",
                    "is_finished": stage_data.get("status") == "COMPLETED",
                }

                # Calculate throughput from previous data
                if stage_id in self._last_metrics:
                    last = self._last_metrics[stage_id]
                    input_delta = metrics_dict["input_records"] - last.get("input_records", 0)
                    output_delta = metrics_dict["output_records"] - last.get("output_records", 0)
                    metrics_dict["input_throughput"] = max(0, input_delta)
                    metrics_dict["output_throughput"] = max(0, output_delta)

                self.prometheus.update_stage_metrics(stage_id, metrics_dict)
                self._last_metrics[stage_id] = metrics_dict

        except Exception as e:
            self.logger.warning(f"Failed to export metrics: {e}")
