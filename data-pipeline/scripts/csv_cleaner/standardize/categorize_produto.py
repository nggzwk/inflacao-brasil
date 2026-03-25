#!/usr/bin/env python3
"""Categorize CSV `produto` values using rules JSON.

Usage (directory -> output directory):
    python categorize_produto_ai.py \
        --input ../../data/cleaned/new_format \
        --output ../../data/standardized \
        --rules rules_v1.json
    # saves each file as: standardized_YYYY-MM-DD.csv

"""

from __future__ import annotations

import argparse
import csv
import json
import re
import unicodedata
from dataclasses import dataclass
from difflib import SequenceMatcher
from pathlib import Path

import pandas as pd

from standardize_files import add_marca_column_from_cerveja


@dataclass(frozen=True)
class Category:
    id: int
    name: str
    items: list[str]


UNITS_TO_REMOVE_IN_PRODUTO = ("KG", "ML", "LITRO")


def _normalize(text: str) -> str:
    text = str(text or "").strip().upper()
    text = "".join(ch for ch in unicodedata.normalize("NFKD", text) if not unicodedata.combining(ch))
    text = re.sub(r"\([^)]*\)", " ", text)
    text = re.sub(r"[^A-Z0-9/ ]+", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _token_set(text: str) -> set[str]:
    return {token for token in _normalize(text).split(" ") if len(token) > 1}


def _clean_produto_units(text: str) -> str:
    value = str(text or "")
    unit_pattern = "|".join(re.escape(unit) for unit in UNITS_TO_REMOVE_IN_PRODUTO)
    value = re.sub(rf"\b\d+(?:[\.,]\d+)?\s*(?:{unit_pattern})\b", " ", value, flags=re.IGNORECASE)
    value = re.sub(rf"\b(?:{unit_pattern})\b", " ", value, flags=re.IGNORECASE)
    value = re.sub(r"\s+", " ", value).strip()
    return value


def _load_categories(path: Path) -> list[Category]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    categories: list[Category] = []
    for row in payload.get("categories", []):
        categories.append(
            Category(
                id=int(row["id"]),
                name=str(row["name"]),
                items=[str(item) for item in row.get("items", [])],
            )
        )
    return categories


def _build_item_index(categories: list[Category]) -> dict[str, tuple[Category, str]]:
    index: dict[str, tuple[Category, str]] = {}
    for category in categories:
        for item in category.items:
            key = _normalize(item)
            index[key] = (category, item)
    return index


def _load_subcategory_indexes(
    path: Path,
) -> tuple[dict[int, dict[str, tuple[Category, str]]], dict[int, int]]:
    if not path.exists():
        return {}, {}

    payload = json.loads(path.read_text(encoding="utf-8"))
    by_category: dict[int, list[Category]] = {}
    fallback_by_category: dict[int, int] = {}

    for row in payload.get("subcategories", []):
        subcategory = Category(
            id=int(row["id"]),
            name=str(row["name"]),
            items=[str(item) for item in row.get("items", [])],
        )
        category_id = int(row["category_id"])
        by_category.setdefault(category_id, []).append(subcategory)

        if not subcategory.items or _normalize(subcategory.name).startswith("OUTROS"):
            fallback_by_category[category_id] = subcategory.id

    indexes: dict[int, dict[str, tuple[Category, str]]] = {}
    for category_id, subcategories in by_category.items():
        indexes[category_id] = _build_item_index(subcategories)

    return indexes, fallback_by_category


def _best_rule_match(produto: str, item_index: dict[str, tuple[Category, str]]) -> tuple[Category | None, str, float]:
    normalized = _normalize(produto)
    if not normalized:
        return None, "empty", 0.0

    exact = item_index.get(normalized)
    if exact is not None:
        return exact[0], "rule_exact", 1.0

    best_category: Category | None = None
    best_score = 0.0

    for item_norm, (category, _) in item_index.items():
        score = SequenceMatcher(None, normalized, item_norm).ratio()
        if score > best_score:
            best_score = score
            best_category = category

    if best_category is not None and best_score >= 0.90:
        return best_category, "rule_fuzzy", float(best_score)

    produto_tokens = _token_set(produto)
    if not produto_tokens:
        return None, "no_tokens", 0.0

    by_overlap: dict[int, float] = {}
    by_category: dict[int, Category] = {}
    for item_norm, (category, _) in item_index.items():
        item_tokens = set(item_norm.split(" "))
        if not item_tokens:
            continue
        overlap = len(produto_tokens.intersection(item_tokens)) / max(len(produto_tokens), 1)
        if overlap > by_overlap.get(category.id, 0.0):
            by_overlap[category.id] = overlap
            by_category[category.id] = category

    if by_overlap:
        category_id, score = max(by_overlap.items(), key=lambda pair: pair[1])
        if score >= 0.55:
            return by_category[category_id], "rule_keyword", float(score)

    return None, "unmatched", 0.0


def categorize_file(
    input_path: Path,
    output_path: Path,
    rules_path: Path,
    subcategories_rules_path: Path,
    produto_column: str,
) -> None:
    input_path_text = str(input_path).replace("\\", "/").lower()
    use_old_format_rules = "cleaned/old_format" in input_path_text

    categories = _load_categories(rules_path)
    item_index = _build_item_index(categories)
    subcategory_indexes, fallback_subcategory_by_category = _load_subcategory_indexes(subcategories_rules_path)

    sep = _detect_csv_separator(input_path)
    df = pd.read_csv(input_path, sep=sep, dtype=str).fillna("")
    if produto_column not in df.columns:
        raise ValueError(f"Column not found: {produto_column}")

    df = add_marca_column_from_cerveja(
        df,
        produto_column=produto_column,
        preco_column="preco",
        marca_column="marca",
        use_old_format_rules=use_old_format_rules,
    )

    df[produto_column] = df[produto_column].map(_clean_produto_units)

    unique_produtos = sorted(set(df[produto_column].astype(str).str.strip()))
    mapping: dict[str, tuple[int | None, str, float]] = {}

    for produto in unique_produtos:
        category, method, score = _best_rule_match(produto, item_index)
        if category is not None:
            mapping[produto] = (category.id, method, score)
            continue

        mapping[produto] = (None, method, score)

    subcategory_mapping: dict[str, int | None] = {}
    for produto in unique_produtos:
        category_id = mapping.get(produto, (None, "", 0.0))[0]
        if category_id is None:
            subcategory_mapping[produto] = None
            continue

        index_for_category = subcategory_indexes.get(int(category_id), {})
        fallback_subcategory = fallback_subcategory_by_category.get(int(category_id))
        if not index_for_category:
            subcategory_mapping[produto] = fallback_subcategory
            continue

        matched_subcategory, _, _ = _best_rule_match(produto, index_for_category)
        if matched_subcategory is not None:
            subcategory_mapping[produto] = matched_subcategory.id
        else:
            subcategory_mapping[produto] = fallback_subcategory

    df["categoria_score"] = df[produto_column].map(lambda p: mapping.get(str(p).strip(), (None, "", 0.0))[2])
    df["produto_categoria"] = df[produto_column].map(lambda p: mapping.get(str(p).strip(), (None, "", 0.0))[0])
    df["produto_categoria"] = pd.to_numeric(df["produto_categoria"], errors="coerce").astype("Int64")
    df["produto_subcategoria"] = df[produto_column].map(lambda p: subcategory_mapping.get(str(p).strip()))
    df["produto_subcategoria"] = pd.to_numeric(df["produto_subcategoria"], errors="coerce").astype("Int64")

    legacy_columns = [col for col in ["codigo_categoria", "id_produto"] if col in df.columns]
    if legacy_columns:
        df = df.drop(columns=legacy_columns)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False, sep=sep)

    total = len(df)
    matched = int(df["produto_categoria"].notna().sum())
    print(f"Rows: {total}")
    print(f"Categorized: {matched}")
    print(f"Uncategorized: {total - matched}")


def _detect_csv_separator(path: Path) -> str:
    try:
        sample = path.read_text(encoding="latin-1", errors="ignore")[:4096]
        dialect = csv.Sniffer().sniff(sample, delimiters=",;")
        return dialect.delimiter
    except Exception:
        return ";" if ";" in path.name else ","


def _standardized_filename_from_input(input_path: Path) -> str:
    match = re.search(r"(\d{4}-\d{2}-\d{2})", input_path.name)
    if match:
        return f"standardized_{match.group(1)}.csv"
    return f"standardized_{input_path.stem}.csv"


def _build_jobs(input_path: Path, output_path: Path) -> list[tuple[Path, Path]]:
    if input_path.is_file():
        if output_path.exists() and output_path.is_dir():
            return [(input_path, output_path / _standardized_filename_from_input(input_path))]
        return [(input_path, output_path)]

    if not input_path.is_dir():
        raise ValueError(f"Input path not found: {input_path}")

    if output_path.exists() and output_path.is_file():
        raise ValueError("When input is a directory, output must be a directory path")

    jobs: list[tuple[Path, Path]] = []
    output_path.mkdir(parents=True, exist_ok=True)
    for csv_file in sorted(input_path.glob("*.csv")):
        jobs.append((csv_file, output_path / _standardized_filename_from_input(csv_file)))
    return jobs


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Categorize CSV produtos using rules_v1.json")
    parser.add_argument("--input", required=True, type=Path, help="Input CSV file or directory path")
    parser.add_argument("--output", required=True, type=Path, help="Output CSV file or directory path")
    parser.add_argument(
        "--rules",
        default=Path(__file__).with_name("rules_v1.json"),
        type=Path,
        help="Rules JSON path",
    )
    parser.add_argument(
        "--subcategories-rules",
        default=Path(__file__).with_name("rules_subcategories_v1.json"),
        type=Path,
        help="Subcategories rules JSON path",
    )
    parser.add_argument("--produto-column", default="produto", help="Produto column name")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    jobs = _build_jobs(args.input, args.output)
    for input_path, output_path in jobs:
        print(f"\nProcessing: {input_path.name}")
        categorize_file(
            input_path=input_path,
            output_path=output_path,
            rules_path=args.rules,
            subcategories_rules_path=args.subcategories_rules,
            produto_column=args.produto_column,
        )
        print(f"Saved: {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
