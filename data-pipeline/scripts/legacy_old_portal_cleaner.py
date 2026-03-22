#!/usr/bin/env python3
"""Dedicated cleaner for legacy old portal CSV files (07/2022-06/2023)."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

import pandas as pd

from csv_utils import resolve_target_date


REQUIRED_COLUMNS = [
    "data_pesquisa",
    "id_empresa",
    "razao_social",
    "id_categoria",
    "categoria",
    "id_produto",
    "descricao",
    "preco",
    "qtd",
    "unidade"
]

COLUMN_ALIASES = {
    "data_pesquisa": ["data_pesquisa"],
    "id_empresa": ["id_empresa"],
    "razao_social": ["razao_social", "rede"],
    "id_categoria": ["id_categoria", "id_produto_classificacao"],
    "categoria": ["categoria", "produto_classificacao"],
    "id_produto": ["id_produto"],
    "descricao": ["descricao", "produto"],
    "preco": ["preco", "preco_encontrado"],
    "qtd": ["qtd", "qtd_embalagem"],
    "unidade": ["unidade_sigla"]
}


def _parse_date_br(value: str):
    try:
        return datetime.strptime(value, "%d/%m/%Y")
    except (ValueError, TypeError):
        return None


def _pick_column(frame: pd.DataFrame, aliases: list[str]) -> pd.Series:
    for alias in aliases:
        if alias in frame.columns:
            return frame[alias]
    return pd.Series([""] * len(frame), index=frame.index)


def clean_legacy_old_portal_csv(input_file: Path, output_file: Path, target_date: str) -> bool:
    print(f"\nCleaning legacy old portal: {input_file.name}")
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

    if "data_pesquisa" not in df.columns:
        print("✗ Required column not found: data_pesquisa")
        return False

    print(f"Rows before filtering: {len(df)}")

    selected_date = resolve_target_date(
        df,
        target_date,
        target_parser=_parse_date_br,
        df_parser=_parse_date_br,
        df_date_format_str="%d/%m/%Y",
        max_days_offset=7,
        fallback_to_latest=True,
    )
    if selected_date is None:
        return False

    filtered = df[df["data_pesquisa"] == selected_date].copy()
    print(f"Rows after filtering by date {selected_date}: {len(filtered)}")

    cleaned = pd.DataFrame(index=filtered.index)
    for output_column in REQUIRED_COLUMNS:
        cleaned[output_column] = _pick_column(filtered, COLUMN_ALIASES[output_column])

    before = len(cleaned)
    cleaned = cleaned.drop_duplicates(subset=["id_empresa", "id_produto", "data_pesquisa"], keep="first")
    print(f"Duplicates removed: {before - len(cleaned)}")

    cleaned = cleaned.fillna("")
    cleaned["data_pesquisa"] = pd.to_datetime(
        cleaned["data_pesquisa"],
        format="%d/%m/%Y",
        errors="coerce",
    ).dt.strftime("%Y-%m-%d")
    cleaned["data_pesquisa"] = cleaned["data_pesquisa"].fillna("")

    try:
        cleaned.to_csv(output_file, index=False, sep=",")
        print(f"✓ Cleaned CSV saved: {output_file.name}")
        print(f"Final rows: {len(cleaned)}")
        return True
    except Exception as exc:
        print(f"✗ Error saving CSV: {exc}")
        return False
