#!/usr/bin/env python3
"""Unified data pipeline CLI for download and processing."""

import argparse
import sys

from csv_utils import print_results_summary, print_section
from pipeline_core import CLEANED_DIR, DATASETS, RAW_DIR, download_all, download_dataset, process_all, process_dataset


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Inflação Brasil data pipeline")
    parser.add_argument(
        "action",
        nargs="?",
        default="process",
        choices=["process", "download", "all"],
        help="Action to run (default: process)",
    )
    parser.add_argument(
        "--dataset",
        default="all",
        choices=["all", *DATASETS.keys()],
        help="Dataset to target (default: all)",
    )
    return parser


def run_process(dataset: str) -> int:
    if dataset == "all":
        return print_results_summary("PROCESS SUMMARY", process_all())
    result = process_dataset(dataset)
    return print_results_summary("PROCESS SUMMARY", {dataset: result})


def run_download(dataset: str) -> int:
    if dataset == "all":
        return print_results_summary("DOWNLOAD SUMMARY", download_all())
    result = download_dataset(dataset)
    return print_results_summary("DOWNLOAD SUMMARY", {dataset: result})


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    RAW_DIR.mkdir(parents=True, exist_ok=True)
    CLEANED_DIR.mkdir(parents=True, exist_ok=True)

    print_section("INFLAÇÃO BRASIL - DATA PIPELINE")
    print(f"Raw data dir:     {RAW_DIR}")
    print(f"Cleaned data dir: {CLEANED_DIR}")

    if args.action == "process":
        return run_process(args.dataset)
    if args.action == "download":
        return run_download(args.dataset)

    download_code = run_download(args.dataset)
    process_code = run_process(args.dataset)
    return 0 if download_code == 0 and process_code == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
