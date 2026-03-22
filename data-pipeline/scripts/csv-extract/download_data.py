#!/usr/bin/env python3
"""Unified downloader CLI for all datasets."""

import argparse
import sys
from pathlib import Path
SCRIPT_DIR = Path(__file__).resolve().parent
SCRIPTS_ROOT = SCRIPT_DIR.parent
if str(SCRIPTS_ROOT) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_ROOT))
from csv_utils import print_results_summary  # noqa: E402
from pipeline_core import DATASETS, download_all, download_dataset  # noqa: E402

OLD_LEGACY_DATASET = "old_portal"
OLD_COTACOES_DATASET = "cotacoes_old"
NEW_COTACOES_DATASET = "cotacoes_new"

SPECIAL_DATASETS = {
    "old_legacy_2022_2023": [("old_legacy_2022_2023", OLD_LEGACY_DATASET)],
    "old_cotacoes_2023_2024": [("old_cotacoes_2023_2024", OLD_COTACOES_DATASET)],
    "old_2022_2024": [
        ("old_legacy_2022_2023", OLD_LEGACY_DATASET),
        ("old_cotacoes_2023_2024", OLD_COTACOES_DATASET),
    ],
    "new_cotacoes_2025_plus": [("new_cotacoes_2025_plus", NEW_COTACOES_DATASET)],
}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Download raw CSV files for inflation datasets")
    parser.add_argument(
        "--dataset",
        default="all",
        choices=["all", *DATASETS.keys(), *SPECIAL_DATASETS.keys()],
        help="Dataset to download (default: all)",
    )
    return parser


def _run_special_dataset(group_key: str) -> dict[str, bool]:
    return {
        label: download_dataset(dataset_key)
        for label, dataset_key in SPECIAL_DATASETS[group_key]
    }


def main() -> int:
    args = build_parser().parse_args()

    if args.dataset == "all":
        return print_results_summary("DOWNLOAD SUMMARY", download_all())

    if args.dataset in SPECIAL_DATASETS:
        return print_results_summary("DOWNLOAD SUMMARY", _run_special_dataset(args.dataset))

    return print_results_summary("DOWNLOAD SUMMARY", {args.dataset: download_dataset(args.dataset)})


if __name__ == "__main__":
    sys.exit(main())
