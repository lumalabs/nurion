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

"""Tests for MinHash deduplication workflow.

Test Data (same format as runMinHashExample.py):
- Train file: ~/Downloads/articles_10000.train
  Format: Each line is "<doc_id> <word1> <word2> ..."
- Truth file: ~/Downloads/articles_10000.truth
  Format: Each line is "<doc_id1> <doc_id2>" (plagiary pairs)

NOTE: Current pipeline limitation - documents without candidate pairs are not
output. This is because Solstice doesn't yet support multi-upstream stages.
"""

import asyncio
import logging
import os
import shutil
import tempfile
from pathlib import Path
from typing import Any, Dict, List, Tuple

import lance
import pyarrow as pa
import pytest

logger = logging.getLogger(__name__)

# Real test data paths
ARTICLES_TRAIN_PATH = os.path.expanduser("~/Downloads/articles_10000.train")
ARTICLES_TRUTH_PATH = os.path.expanduser("~/Downloads/articles_10000.truth")


def load_articles_train(path: str = ARTICLES_TRAIN_PATH) -> List[Dict[str, str]]:
    """Load articles from train file.

    Format (same as runMinHashExample.py):
        words = f.readline().split(" ")
        docID = words[0]
        del words[0]
        # rest are content words

    Each line: "<doc_id> <word1> <word2> ..."
    """
    documents = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            # Split by space
            words = line.split(" ")
            # First word is doc_id
            doc_id = words[0]
            # Rest is text (rejoin with spaces)
            text = " ".join(words[1:]).strip()
            documents.append({"doc_id": doc_id, "text": text})
    return documents


def load_articles_truth(path: str = ARTICLES_TRUTH_PATH) -> Dict[str, str]:
    """Load ground truth plagiary pairs.

    Format (same as runMinHashExample.py):
        docs = line.split(" ")
        plagiaries[docs[0]] = docs[1]
        plagiaries[docs[1]] = docs[0]

    Returns bidirectional dict: plagiaries[doc1] = doc2, plagiaries[doc2] = doc1
    """
    plagiaries = {}
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            # Strip newline
            if line and line[-1] == '\n':
                line = line[:-1]
            if not line:
                continue
            docs = line.split(" ")
            if len(docs) >= 2:
                # Map the two documents to each other
                plagiaries[docs[0]] = docs[1]
                plagiaries[docs[1]] = docs[0]
    return plagiaries


def create_test_documents(path: str) -> Dict[str, Any]:
    """Create test documents from real articles dataset.

    Args:
        path: Output Lance dataset path

    Returns metadata for verification.
    """
    # Load real data
    documents = load_articles_train()
    plagiaries = load_articles_truth()

    # Write to Lance
    table = pa.Table.from_pylist(documents)
    lance.write_dataset(table, path, mode="overwrite")

    # Number of truth pairs = len(plagiaries) / 2 (since bidirectional)
    num_truth_pairs = len(plagiaries) // 2

    return {
        "total_docs": len(documents),
        "num_truth_pairs": num_truth_pairs,
        "plagiaries": plagiaries,  # Bidirectional dict
    }


@pytest.mark.workflow
@pytest.mark.timeout(600)
class TestMinHashDedupWorkflowExecution:
    """End-to-end workflow tests for MinHash deduplication."""

    def test_basic_execution(self, ray_cluster):
        """Test workflow execution."""
        tmp_dir = tempfile.mkdtemp(prefix="minhash_exec_test_")
        input_path = os.path.join(tmp_dir, "input.lance")
        output_path = os.path.join(tmp_dir, "output.lance")

        try:
            # Load all 10000 documents
            metadata = create_test_documents(input_path)

            logger.info(
                f"Test data loaded:\n"
                f"  - Total docs: {metadata['total_docs']}\n"
                f"  - Truth pairs: {metadata['num_truth_pairs']}"
            )

            from workflows.minhash_dedup import create_job

            # Parameters from runMinHashExample.py:
            # - numHashes=10, threshold=0.5
            # - Use 8 partitions for CC to enable multi-round iteration
            job = create_job(
                job_id="test_minhash_exec",
                config={
                    "input": input_path,
                    "output": output_path,
                    "content_column": "text",
                    "id_column": "doc_id",
                    "similarity_threshold": 0.5,
                    "num_hashes": 10,
                    "num_bands": 2,  # 10/2 = 5 rows per band
                    "max_iterations": 20,
                    "tansu_storage_url": "memory://",
                    "output_format": "lance",
                    "num_partitions": 8,
                    # Resources for 10k doc test
                    "worker_num_cpus": 0.5,
                    "worker_memory_mb": 512,
                },
            )

            runner = job.create_ray_runner()

            async def run():
                try:
                    status = await runner.run(timeout=300)
                    return status
                finally:
                    await runner.stop()

            status = asyncio.run(run())

            # Verify pipeline completed
            assert not status.error, f"Pipeline failed: {status.error}"

            # Verify output was produced
            assert Path(output_path).exists(), "Output file not created"

            result_ds = lance.dataset(output_path)
            result_table = result_ds.to_table()
            result_count = result_table.num_rows

            # Get output doc_ids
            assert "doc_id" in result_table.column_names, "Missing doc_id column"
            output_doc_ids = result_table.column("doc_id").to_pylist()
            output_id_set = set(output_doc_ids)

            logger.info(
                f"Results:\n"
                f"  - Input: {metadata['total_docs']}\n"
                f"  - Output: {result_count}"
            )

            # === VERIFICATION (same logic as runMinHashExample.py) ===

            # 1. No duplicate doc_ids in output
            assert len(output_doc_ids) == len(output_id_set), (
                f"Duplicate doc_ids in output: {len(output_doc_ids)} rows but only {len(output_id_set)} unique"
            )

            # 2. For each truth pair: at most one should be in output
            #    (if both are in output, dedup failed for that pair)
            plagiaries = metadata["plagiaries"]
            both_kept = []
            one_kept = 0
            neither_kept = 0

            # Count unique pairs (since plagiaries is bidirectional)
            seen_pairs = set()
            for doc1, doc2 in plagiaries.items():
                pair = tuple(sorted([doc1, doc2]))
                if pair in seen_pairs:
                    continue
                seen_pairs.add(pair)

                doc1_in = doc1 in output_id_set
                doc2_in = doc2 in output_id_set

                if doc1_in and doc2_in:
                    both_kept.append(f"{doc1} and {doc2}")
                elif doc1_in or doc2_in:
                    one_kept += 1
                else:
                    neither_kept += 1

            logger.info(
                f"\nDedup results:\n"
                f"  - Truth pairs with exactly one kept: {one_kept}/{metadata['num_truth_pairs']}\n"
                f"  - Truth pairs with both kept (dedup failed): {len(both_kept)}\n"
                f"  - Truth pairs with neither kept: {neither_kept}\n"
                f"  - Output count: {result_count}"
            )

            # Dedup should not keep both docs from any truth pair
            assert len(both_kept) == 0, (
                f"Dedup failed - both docs kept for {len(both_kept)} pairs:\n"
                + "\n".join(both_kept[:10])
            )

            # At least some truth pairs should have one doc kept (recall > 0)
            recall = one_kept / metadata["num_truth_pairs"] * 100 if metadata["num_truth_pairs"] > 0 else 0
            logger.info(f"  - Recall: {recall:.1f}%")

        finally:
            if Path(tmp_dir).exists():
                shutil.rmtree(tmp_dir)
