#!/usr/bin/env python3
"""Unified pipeline core with shared download and cleaning abstractions."""

from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Callable, Dict, Optional

import pandas as pd
import requests
from dateutil.relativedelta import relativedelta
from csv_utils import (
    extract_date_from_filename,
    print_section,
)
from csv_cleaner.base.legacy_format_cleaner import clean_old_format_csv
from csv_cleaner.base.new_format_cleaner import clean_new_format_csv


SCRIPT_DIR = Path(__file__).parent
DATA_DIR = SCRIPT_DIR.parent / "data"
RAW_DIR = DATA_DIR / "raw"
CLEANED_DIR = DATA_DIR / "cleaned"


def parse_date_iso(date_str: str) -> Optional[datetime]:
    try:
        return datetime.strptime(date_str, "%Y-%m-%d")
    except (ValueError, TypeError):
        return None


def _generate_monthly_dates(start: datetime, end: datetime, include_end_always: bool) -> list[datetime]:
    dates = []
    current = start
    while current <= end:
        dates.append(current)
        if current.month == end.month and current.year == end.year:
            break
        current += relativedelta(months=1)
    if include_end_always and dates and dates[-1] != end:
        dates.append(end)
    return dates


def _download_with_fallback(
    base_url: str,
    output_dir: Path,
    date: datetime,
    filename_formatter: Callable[[datetime], str],
    max_days_back: int,
) -> bool:
    for day_offset in range(max_days_back):
        current_date = date - timedelta(days=day_offset)
        filename = filename_formatter(current_date)
        url = base_url + filename
        output_file = output_dir / filename

        if output_file.exists():
            if day_offset == 0:
                print(f"⊘ Skipped (already exists): {filename}")
            return True

        try:
            print(f"Downloading: {url}")
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            output_file.write_bytes(response.content)
            print(f"✓ Saved: {output_file.name} ({len(response.content)} bytes)")
            return True
        except requests.exceptions.HTTPError as exc:
            if exc.response is not None and exc.response.status_code == 404:
                if day_offset < max_days_back - 1:
                    next_day = (current_date - timedelta(days=1)).day
                    print(f"✗ Day {current_date.day} not found, trying day {next_day}...")
                continue
            status = exc.response.status_code if exc.response is not None else "unknown"
            print(f"✗ HTTP Error {status} for {url}")
            return False
        except requests.exceptions.RequestException as exc:
            print(f"✗ Failed to download {url}: {exc}")
            return False
        except Exception as exc:
            print(f"✗ Error saving file: {exc}")
            return False

    print(f"✗ No valid file found within {max_days_back} days from {date.date()}")
    return False


@dataclass(frozen=True)
class DatasetConfig:
    key: str
    title: str
    raw_subdir: str
    cleaned_subdir: str
    cleaner: Callable[[Path, Path, str], bool]
    download_base_url: str
    download_start: datetime
    download_end_provider: Callable[[], datetime]
    download_filename: Callable[[datetime], str]
    fallback_days: int
    include_end_always: bool = False
    file_matcher: Callable[[str], bool] = lambda _: True
    dedupe_cleaned_files: bool = False


def _cleaned_file_signature(file_path: Path) -> Optional[tuple[str, int]]:
    try:
        frame = pd.read_csv(file_path, usecols=["data_pesquisa"], dtype=str, on_bad_lines="skip", engine="c")
    except Exception as exc:
        print(f"⚠ Could not read file signature for {file_path.name}: {exc}")
        return None

    if "data_pesquisa" not in frame.columns:
        return None

    row_count = len(frame)
    unique_dates = frame["data_pesquisa"].fillna("").astype(str).str.strip().unique()
    if len(unique_dates) == 0:
        return None

    return unique_dates[0], row_count


def _remove_duplicate_cleaned_files(output_dir: Path) -> int:
    cleaned_files = sorted(output_dir.glob("cleaned_*.csv"))
    if not cleaned_files:
        return 0

    signatures: Dict[tuple[str, int], Path] = {}
    removed = 0

    for file_path in cleaned_files:
        signature = _cleaned_file_signature(file_path)
        if signature is None:
            continue

        existing = signatures.get(signature)
        if existing is None:
            signatures[signature] = file_path
            continue

        file_path.unlink(missing_ok=True)
        removed += 1
        print(
            "🗑 Removed duplicate cleaned file: "
            f"{file_path.name} (same data_pesquisa and row count as {existing.name})"
        )

    if removed:
        print(f"✓ Removed {removed} duplicate cleaned file(s) in {output_dir.name}")

    return removed


def _filename_old_portal(date: datetime) -> str:
    return f"{date.strftime('%Y-%m-%d')}_Clique_Economia_-_Base_de_Dados.csv"


def _filename_cotacoes(date: datetime) -> str:
    return f"{date.strftime('%Y-%m-%d')}_Clique_Economia_-_Cotacoes_-_Base_de_Dados.csv"


def _match_legacy_old_filename(filename: str) -> bool:
    return "_Clique_Economia_-_Base_de_Dados.csv" in filename and "_Cotacoes_-_" not in filename


def _match_cotacoes_filename(filename: str) -> bool:
    return "_Clique_Economia_-_Cotacoes_-_Base_de_Dados.csv" in filename


def _extract_cotacoes_filename_date(filename: str) -> Optional[datetime]:
    if not _match_cotacoes_filename(filename):
        return None
    if len(filename) < 10:
        return None
    return parse_date_iso(filename[:10])


def _match_cotacoes_old_filename(filename: str) -> bool:
    file_date = _extract_cotacoes_filename_date(filename)
    if file_date is None:
        return False
    return datetime(2023, 7, 1) <= file_date <= datetime(2024, 12, 31)


def _match_cotacoes_new_filename(filename: str) -> bool:
    file_date = _extract_cotacoes_filename_date(filename)
    if file_date is None:
        return False
    return file_date >= datetime(2025, 1, 1)


def _current_month_day_20() -> datetime:
    now = datetime.now()
    return now.replace(day=20)


DATASETS: Dict[str, DatasetConfig] = {
    "old_portal": DatasetConfig(
        key="old_portal",
        title="Old Portal Legacy (2022-2023)",
        raw_subdir="downloaded_files_old_portal",
        cleaned_subdir="old_format",
        cleaner=clean_old_format_csv,
        download_base_url="https://dadosabertos.c3sl.ufpr.br/curitiba/CliqueEconomia/",
        download_start=datetime(2022, 7, 20),
        download_end_provider=lambda: datetime(2023, 6, 20),
        download_filename=_filename_old_portal,
        fallback_days=11,
        file_matcher=_match_legacy_old_filename,
        dedupe_cleaned_files=True,
    ),
    "cotacoes_old": DatasetConfig(
        key="cotacoes_old",
        title="Cotacoes Old Portal (2023-2024)",
        raw_subdir="downloaded_files_cotacoes",
        cleaned_subdir="new_format",
        cleaner=clean_new_format_csv,
        download_base_url="https://dadosabertos.c3sl.ufpr.br/curitiba/CliqueEconomia/",
        download_start=datetime(2023, 7, 20),
        download_end_provider=lambda: datetime(2024, 12, 20),
        download_filename=_filename_cotacoes,
        fallback_days=11,
        include_end_always=True,
        file_matcher=_match_cotacoes_old_filename,
    ),
    "cotacoes_new": DatasetConfig(
        key="cotacoes_new",
        title="Cotacoes New Portal (2025+)",
        raw_subdir="downloaded_files_cotacoes",
        cleaned_subdir="new_format",
        cleaner=clean_new_format_csv,
        download_base_url="https://mid-dadosabertos.curitiba.pr.gov.br/CliqueEconomia/",
        download_start=datetime(2025, 1, 20),
        download_end_provider=_current_month_day_20,
        download_filename=_filename_cotacoes,
        fallback_days=11,
        file_matcher=_match_cotacoes_new_filename,
    ),
}


def _get_dataset_config(dataset_key: str) -> DatasetConfig:
    config = DATASETS.get(dataset_key)
    if config is None:
        valid = ", ".join(sorted(DATASETS.keys()))
        raise ValueError(f"invalid dataset '{dataset_key}'. expected one of: {valid}")
    return config


def _run_for_all(operation: Callable[[str], bool]) -> dict[str, bool]:
    return {key: operation(key) for key in DATASETS.keys()}


def get_paths(config: DatasetConfig) -> tuple[Path, Path]:
    input_dir = RAW_DIR / config.raw_subdir
    output_dir = CLEANED_DIR / config.cleaned_subdir
    return input_dir, output_dir


def process_dataset(dataset_key: str) -> bool:
    config = _get_dataset_config(dataset_key)
    input_dir, output_dir = get_paths(config)

    print_section(f"Processing {config.title}")

    if not input_dir.exists():
        print(f"⚠ Input directory not found: {input_dir}")
        return False

    output_dir.mkdir(parents=True, exist_ok=True)
    csv_files = sorted(file for file in input_dir.glob("*.csv") if config.file_matcher(file.name))
    if not csv_files:
        print(f"✗ No CSV files found in {input_dir}")
        return False

    failed = 0
    for csv_file in csv_files:
        target_date = extract_date_from_filename(csv_file.name)
        if target_date is None:
            failed += 1
            continue
        output_file = output_dir / f"cleaned_{csv_file.name}"
        if not config.cleaner(csv_file, output_file, target_date):
            failed += 1

    if config.dedupe_cleaned_files:
        _remove_duplicate_cleaned_files(output_dir)

    return failed == 0


def process_all() -> dict[str, bool]:
    return _run_for_all(process_dataset)


def download_dataset(dataset_key: str) -> bool:
    config = _get_dataset_config(dataset_key)
    output_dir = RAW_DIR / config.raw_subdir
    output_dir.mkdir(parents=True, exist_ok=True)

    start = config.download_start
    end = config.download_end_provider()
    dates = _generate_monthly_dates(start, end, include_end_always=config.include_end_always)

    print_section(f"Downloading {config.title}")
    print(f"Output directory: {output_dir}")
    print(f"Downloading {len(dates)} files from {start.date()} to {end.date()}\n")

    failed = 0
    for date in dates:
        if not _download_with_fallback(
            base_url=config.download_base_url,
            output_dir=output_dir,
            date=date,
            filename_formatter=config.download_filename,
            max_days_back=config.fallback_days,
        ):
            failed += 1
    return failed == 0


def download_all() -> dict[str, bool]:
    return _run_for_all(download_dataset)