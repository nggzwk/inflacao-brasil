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
    resolve_target_date,
)
from legacy_old_portal_cleaner import clean_legacy_old_portal_csv


SCRIPT_DIR = Path(__file__).parent
DATA_DIR = SCRIPT_DIR.parent / "data"
RAW_DIR = DATA_DIR / "raw"
CLEANED_DIR = DATA_DIR / "cleaned"


def parse_date_br(date_str: str) -> Optional[datetime]:
    try:
        return datetime.strptime(date_str, "%d/%m/%Y")
    except (ValueError, TypeError):
        return None


def parse_date_iso(date_str: str) -> Optional[datetime]:
    try:
        return datetime.strptime(date_str, "%Y-%m-%d")
    except (ValueError, TypeError):
        return None


def _read_csv_with_encodings(input_file: Path, delimiter: str, encodings: list[str]) -> Optional[pd.DataFrame]:
    for encoding in encodings:
        try:
            df = pd.read_csv(
                input_file,
                delimiter=delimiter,
                dtype={"data_pesquisa": str},
                on_bad_lines="skip",
                engine="c",
                encoding=encoding,
            )
            print(f"✓ File read with encoding: {encoding}")
            return df
        except (UnicodeDecodeError, LookupError):
            continue
        except Exception as exc:
            print(f"✗ Error reading CSV with {encoding}: {exc}")
            return None
    return None


def _extract_quantity(desc: str) -> str:
    if pd.isna(desc) or not desc:
        return ""
    parts = str(desc).rsplit("-", 1)
    return parts[1].strip() if len(parts) == 2 else ""


def _clean_description(desc: str) -> str:
    if pd.isna(desc) or not desc:
        return desc
    parts = str(desc).rsplit("-", 1)
    return parts[0].strip() if len(parts) == 2 else desc


def clean_old_portal_csv(input_file: Path, output_file: Path, target_date: str) -> bool:
    print(f"\nCleaning: {input_file.name}")
    print("Delimiter: ','")

    try:
        df = pd.read_csv(
            input_file,
            delimiter=",",
            dtype={"data_pesquisa": str},
            on_bad_lines="skip",
            engine="c",
        )
    except Exception as exc:
        print(f"✗ Error reading CSV: {exc}")
        return False

    print(f"Rows before filtering: {len(df)}")

    closest_date = resolve_target_date(
        df,
        target_date,
        target_parser=parse_date_br,
        df_parser=parse_date_br,
        df_date_format_str="%d/%m/%Y",
        max_days_offset=7,
    )
    if closest_date is None:
        return False

    df_filtered = df[df["data_pesquisa"] == closest_date].copy()
    print(f"Rows after filtering by date {closest_date}: {len(df_filtered)}")

    essential_columns = [
        "data_pesquisa",
        "id_empresa",
        "razao_social",
        "id_produto_classificacao",
        "produto_classificacao",
        "id_produto",
        "produto",
        "preco_encontrado",
        "qtd_embalagem",
    ]
    available_cols = [col for col in essential_columns if col in df_filtered.columns]
    df_clean = df_filtered[available_cols].copy()

    before = len(df_clean)
    df_clean = df_clean.drop_duplicates(subset=["id_empresa", "id_produto", "data_pesquisa"], keep="first")
    print(f"Duplicates removed: {before - len(df_clean)}")

    df_clean = df_clean.fillna("")
    df_clean["data_pesquisa"] = pd.to_datetime(df_clean["data_pesquisa"], format="%d/%m/%Y").dt.strftime("%Y-%m-%d")

    try:
        df_clean.to_csv(output_file, index=False, sep=",")
        print(f"✓ Cleaned CSV saved: {output_file.name}")
        print(f"Final rows: {len(df_clean)}")
        return True
    except Exception as exc:
        print(f"✗ Error saving CSV: {exc}")
        return False


def clean_cotacoes_csv(input_file: Path, output_file: Path, target_date: str) -> bool:
    print(f"\nCleaning: {input_file.name}")
    print("Delimiter: ';'")

    df = _read_csv_with_encodings(input_file, ";", ["utf-8", "latin-1", "iso-8859-1", "cp1252"])
    if df is None:
        print("✗ Error reading CSV: Could not decode file with any supported encoding")
        return False

    print(f"Rows before filtering: {len(df)}")

    closest_date = resolve_target_date(
        df,
        target_date,
        target_parser=parse_date_br,
        df_parser=parse_date_iso,
        df_date_format_str="%Y-%m-%d",
        max_days_offset=7,
    )
    if closest_date is None:
        return False

    df_filtered = df[df["data_pesquisa"] == closest_date].copy()
    print(f"Rows after filtering by date {closest_date}: {len(df_filtered)}")

    essential_columns = [
        "data_pesquisa",
        "id_empresa",
        "rede",
        "codigo_categoria",
        "id_produto",
        "descricao",
        "preco_regular",
    ]
    available_cols = [col for col in essential_columns if col in df_filtered.columns]
    df_clean = df_filtered[available_cols].copy()

    df_clean["qtd_embalagem"] = df_clean["descricao"].apply(_extract_quantity)
    df_clean["descricao"] = df_clean["descricao"].apply(_clean_description)
    df_clean = df_clean.rename(columns={"rede": "razao_social"})

    before = len(df_clean)
    df_clean = df_clean.drop_duplicates(subset=["id_empresa", "id_produto", "data_pesquisa"], keep="first")
    print(f"Duplicates removed: {before - len(df_clean)}")

    df_clean = df_clean.fillna("")
    output_columns = [
        "data_pesquisa",
        "id_empresa",
        "razao_social",
        "codigo_categoria",
        "id_produto",
        "descricao",
        "preco_regular",
        "qtd_embalagem",
    ]
    df_clean = df_clean[output_columns]

    try:
        df_clean.to_csv(output_file, index=False, sep=";")
        print(f"✓ Cleaned CSV saved: {output_file.name}")
        print(f"Final rows: {len(df_clean)}")
        return True
    except Exception as exc:
        print(f"✗ Error saving CSV: {exc}")
        return False


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
        cleaned_subdir="old_portal",
        cleaner=clean_legacy_old_portal_csv,
        download_base_url="https://dadosabertos.c3sl.ufpr.br/curitiba/CliqueEconomia/",
        download_start=datetime(2022, 7, 20),
        download_end_provider=lambda: datetime(2023, 6, 20),
        download_filename=_filename_old_portal,
        fallback_days=11,
        file_matcher=_match_legacy_old_filename,
    ),
    "cotacoes_old": DatasetConfig(
        key="cotacoes_old",
        title="Cotacoes Old Portal (2023-2024)",
        raw_subdir="downloaded_files_cotacoes",
        cleaned_subdir="cotacoes_old_portal",
        cleaner=clean_cotacoes_csv,
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
        cleaned_subdir="cotacoes_new_portal",
        cleaner=clean_cotacoes_csv,
        download_base_url="https://mid-dadosabertos.curitiba.pr.gov.br/CliqueEconomia/",
        download_start=datetime(2025, 1, 20),
        download_end_provider=_current_month_day_20,
        download_filename=_filename_cotacoes,
        fallback_days=11,
        file_matcher=_match_cotacoes_new_filename,
    ),
}


def get_paths(config: DatasetConfig) -> tuple[Path, Path]:
    input_dir = RAW_DIR / config.raw_subdir
    output_dir = CLEANED_DIR / config.cleaned_subdir
    return input_dir, output_dir


def process_dataset(dataset_key: str) -> bool:
    config = DATASETS[dataset_key]
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

    return failed == 0


def process_all() -> dict[str, bool]:
    return {key: process_dataset(key) for key in DATASETS.keys()}


def download_dataset(dataset_key: str) -> bool:
    config = DATASETS[dataset_key]
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
    return {key: download_dataset(key) for key in DATASETS.keys()}