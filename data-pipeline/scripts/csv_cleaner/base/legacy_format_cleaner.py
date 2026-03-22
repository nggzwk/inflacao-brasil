#!/usr/bin/env python3
"""Dedicated cleaner for legacy old portal CSV files (07/2022-06/2023)."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
import re
from typing import Optional

import pandas as pd

from csv_utils import resolve_target_date


REQUIRED_COLUMNS = [
    "data_pesquisa",
    "rede",
    "codigo_categoria",
    "id_produto",
    "produto",
    "preco",
    "qtd_embalagem",
    "unidade_sigla",
]

DEDUP_COLUMNS = ["produto", "qtd_embalagem", "unidade_sigla"]
TEMP_PRECO_COLUMN = "__preco_num__"
TEMP_GROUP_SIZE_COLUMN = "__group_size__"
TEMP_GROUP_RANK_COLUMN = "__group_rank__"
TEMP_CODIGO_CATEGORIA_COLUMN = "__codigo_categoria_num__"

COLUMN_ALIASES = {
    "data_pesquisa": ["data_pesquisa"],
    "rede": ["rede"],
    "codigo_categoria": ["id_produto_classificacao"],
    "id_produto": ["id_produto"],
    "produto": ["produto"],
    "preco": ["preco_encontrado"],
    "qtd_embalagem": ["qtd_embalagem"],
    "unidade_sigla": ["unidade_sigla"],
}


def _parse_date_br(value: str) -> Optional[datetime]:
    try:
        return datetime.strptime(value, "%d/%m/%Y")
    except (ValueError, TypeError):
        return None


def _pick_column(frame: pd.DataFrame, aliases: list[str]) -> pd.Series:
    for alias in aliases:
        if alias in frame.columns:
            return frame[alias]
    return pd.Series([""] * len(frame), index=frame.index, dtype="string")


def _normalize_produto(value: str) -> str:
    if pd.isna(value):
        return ""
    text = str(value)
    text = re.sub(r"\(\s*\+\s*\)\s*BARATO|\(\s*\+\s*BARATO\s*\)", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\bLATA\b", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def _parse_preco(value: str) -> float:
    if pd.isna(value):
        return float("inf")
    text = str(value).strip().replace(",", ".")
    try:
        return float(text)
    except ValueError:
        return float("inf")


def _normalize_qtd_embalagem(value: str) -> str:
    if pd.isna(value):
        return ""
    text = str(value).strip().replace(",", ".")
    if text == "":
        return ""
    try:
        number = float(text)
        if number.is_integer():
            return str(int(number))
        return f"{number:g}"
    except ValueError:
        return text


def _select_output_rows(frame: pd.DataFrame) -> pd.DataFrame:
    frame = frame.sort_values(by=TEMP_PRECO_COLUMN, ascending=True, kind="stable")
    frame[TEMP_GROUP_SIZE_COLUMN] = frame.groupby(DEDUP_COLUMNS)["produto"].transform("size")
    frame[TEMP_GROUP_RANK_COLUMN] = frame.groupby(DEDUP_COLUMNS).cumcount()
    return frame[
        ((frame[TEMP_GROUP_SIZE_COLUMN] > 1) & (frame[TEMP_GROUP_RANK_COLUMN] == 1))
        | ((frame[TEMP_GROUP_SIZE_COLUMN] == 1) & (frame[TEMP_GROUP_RANK_COLUMN] == 0))
    ]


def clean_old_format_csv(input_file: Path, output_file: Path, target_date: str) -> bool:
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

    missing_input_cols = [
        alias
        for output_column in REQUIRED_COLUMNS
        for alias in COLUMN_ALIASES[output_column]
        if alias not in filtered.columns
    ]
    if missing_input_cols:
        missing = ", ".join(sorted(set(missing_input_cols)))
        print(f"⚠ Missing source columns (filled as empty): {missing}")

    cleaned = pd.DataFrame(index=filtered.index)
    for output_column in REQUIRED_COLUMNS:
        cleaned[output_column] = _pick_column(filtered, COLUMN_ALIASES[output_column])

    cleaned = cleaned.fillna("")
    cleaned["produto"] = cleaned["produto"].apply(_normalize_produto)
    cleaned["qtd_embalagem"] = cleaned["qtd_embalagem"].apply(_normalize_qtd_embalagem)
    cleaned[TEMP_PRECO_COLUMN] = cleaned["preco"].apply(_parse_preco)

    before = len(cleaned)
    cleaned = _select_output_rows(cleaned)
    print(f"Duplicates removed: {before - len(cleaned)}")

    cleaned["data_pesquisa"] = pd.to_datetime(
        cleaned["data_pesquisa"],
        format="%d/%m/%Y",
        errors="coerce",
    ).dt.strftime("%Y-%m-%d")
    cleaned["data_pesquisa"] = cleaned["data_pesquisa"].fillna("")

    cleaned[TEMP_CODIGO_CATEGORIA_COLUMN] = pd.to_numeric(cleaned["codigo_categoria"], errors="coerce")
    cleaned = cleaned.sort_values(
        by=[TEMP_CODIGO_CATEGORIA_COLUMN, "codigo_categoria"],
        ascending=[True, True],
        kind="stable",
        na_position="last",
    )
    cleaned = cleaned.drop(
        columns=[
            TEMP_PRECO_COLUMN,
            TEMP_GROUP_SIZE_COLUMN,
            TEMP_GROUP_RANK_COLUMN,
            TEMP_CODIGO_CATEGORIA_COLUMN,
        ]
    )
    cleaned = cleaned[REQUIRED_COLUMNS]

    try:
        output_file.parent.mkdir(parents=True, exist_ok=True)
        cleaned.to_csv(output_file, index=False, sep=",")
        print(f"✓ Cleaned CSV saved: {output_file.name}")
        print(f"Final rows: {len(cleaned)}")
        return True
    except Exception as exc:
        print(f"✗ Error saving CSV: {exc}")
        return False
