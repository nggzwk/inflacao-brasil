#!/usr/bin/env python3
"""Load standardized CSV files into Postgres and build monthly item series.

Usage:
  DATABASE_URL=dbname \
  python build_monthly_item_series.py

Optional:
  python build_monthly_item_series.py --standardized-dir data/standardized
  python build_monthly_item_series.py --refresh-only
"""

from __future__ import annotations

import argparse
import csv
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
import importlib
import os
from pathlib import Path
from typing import Any, Protocol


@dataclass(frozen=True)
class PriceObservation:
    reference_date: date
    month_ref: date
    rede: str
    endereco: str
    produto: str
    marca: str
    preco: Decimal
    qtd_embalagem: str
    unidade_sigla: str
    categoria_score: Decimal | None
    produto_categoria: int | None
    produto_subcategoria: int | None
    source_file: str

    def to_db_row(self) -> tuple:
        return (
            self.reference_date,
            self.month_ref,
            self.rede,
            self.endereco,
            self.produto,
            self.marca,
            self.preco,
            self.qtd_embalagem,
            self.unidade_sigla,
            self.categoria_score,
            self.produto_categoria,
            self.produto_subcategoria,
            self.source_file,
        )


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build monthly item series from standardized CSV files"
    )
    parser.add_argument(
        "--standardized-dir",
        type=Path,
        default=Path(__file__).resolve().parent.parent / "data" / "standardized",
        help="Directory containing standardized_*.csv files",
    )
    parser.add_argument(
        "--refresh-only",
        action="store_true",
        help="Skip file load and only refresh monthly aggregates",
    )
    parser.add_argument(
        "--month-ref-source",
        choices=("reference_date", "source_file_name"),
        default="reference_date",
        help=(
            "How month_ref is derived. "
            "reference_date uses data_pesquisa (default); "
            "source_file_name uses standardized_YYYY-MM-DD filename."
        ),
    )
    return parser.parse_args()


def _parse_date(value: str) -> date:
    text = (value or "").strip()
    for fmt in ("%Y-%m-%d", "%d/%m/%Y"):
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    raise ValueError(f"invalid date: {value}")


def _parse_decimal(value: str) -> Decimal:
    text = (value or "").strip().replace(",", ".")
    if not text:
        raise ValueError("empty decimal")
    try:
        return Decimal(text)
    except InvalidOperation as exc:
        raise ValueError(f"invalid decimal: {value}") from exc


def _parse_optional_int(value: str) -> int | None:
    text = (value or "").strip()
    if not text:
        return None
    try:
        return int(float(text))
    except ValueError:
        return None


def _parse_optional_decimal(value: str) -> Decimal | None:
    text = (value or "").strip().replace(",", ".")
    if not text:
        return None
    try:
        return Decimal(text)
    except InvalidOperation:
        return None


class ObservationParser:
    def __init__(self, month_ref_resolver: "MonthRefResolver") -> None:
        self._month_ref_resolver = month_ref_resolver

    def parse(self, raw: dict[str, str], source_file: str) -> PriceObservation | None:
        try:
            reference_date = _parse_date(raw.get("data_pesquisa", ""))
            preco = _parse_decimal(raw.get("preco", ""))
        except ValueError:
            return None

        if preco <= 0:
            return None

        return PriceObservation(
            reference_date=reference_date,
            month_ref=self._month_ref_resolver.resolve(reference_date, source_file),
            rede=(raw.get("rede") or "").strip(),
            endereco=(raw.get("endereco") or "").strip(),
            produto=(raw.get("produto") or "").strip(),
            marca=(raw.get("marca") or "").strip(),
            preco=preco,
            qtd_embalagem=(raw.get("qtd_embalagem") or "").strip(),
            unidade_sigla=(raw.get("unidade_sigla") or "").strip(),
            categoria_score=_parse_optional_decimal(raw.get("categoria_score", "")),
            produto_categoria=_parse_optional_int(raw.get("produto_categoria", "")),
            produto_subcategoria=_parse_optional_int(raw.get("produto_subcategoria", "")),
            source_file=source_file,
        )


class MonthRefResolver(Protocol):
    def resolve(self, reference_date: date, source_file: str) -> date:
        ...


class ReferenceDateMonthRefResolver:
    def resolve(self, reference_date: date, source_file: str) -> date:
        return reference_date.replace(day=1)


class SourceFileMonthRefResolver:
    def resolve(self, reference_date: date, source_file: str) -> date:
        # Expected source file format: standardized_YYYY-MM-DD.csv
        stem = Path(source_file).stem
        prefix = "standardized_"
        if not stem.startswith(prefix):
            return reference_date.replace(day=1)

        date_part = stem[len(prefix):]
        try:
            parsed = datetime.strptime(date_part, "%Y-%m-%d").date()
        except ValueError:
            return reference_date.replace(day=1)
        return parsed.replace(day=1)


class RowParser(Protocol):
    def parse(self, raw: dict[str, str], source_file: str) -> PriceObservation | None:
        ...


class CsvObservationLoader:
    def __init__(self, parser: RowParser) -> None:
        self._parser = parser

    def iter_rows(self, file_path: Path) -> list[tuple]:
        separator = _detect_separator(file_path)
        rows: list[tuple] = []

        with file_path.open("r", encoding="utf-8", newline="") as handler:
            reader = csv.DictReader(handler, delimiter=separator)
            for raw in reader:
                observation = self._parser.parse(raw, file_path.name)
                if observation is None:
                    continue
                rows.append(observation.to_db_row())

        return rows


class MonthlySeriesRepository:
    INSERT_SQL = """
        INSERT INTO inflacao_brasil.price_observation (
            reference_date,
            month_ref,
            rede,
            endereco,
            produto,
            marca,
            preco,
            qtd_embalagem,
            unidade_sigla,
            categoria_score,
            produto_categoria,
            produto_subcategoria,
            source_file
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
    """

    def __init__(self, conn: Any) -> None:
        self._conn = conn

    def replace_source_file_rows(self, source_file: str, rows: list[tuple]) -> None:
        with self._conn.cursor() as cur:
            cur.execute(
                "DELETE FROM inflacao_brasil.price_observation WHERE source_file = %s",
                (source_file,),
            )
            if rows:
                cur.executemany(self.INSERT_SQL, rows)

    def refresh_item_monthly_price(self) -> None:
        with self._conn.cursor() as cur:
            cur.execute("SELECT inflacao_brasil.refresh_item_monthly_price()")


class ObservationLoader(Protocol):
    def iter_rows(self, file_path: Path) -> list[tuple]:
        ...


class MonthlyRepository(Protocol):
    def replace_source_file_rows(self, source_file: str, rows: list[tuple]) -> None:
        ...

    def refresh_item_monthly_price(self) -> None:
        ...


class MonthlySeriesService:
    def __init__(self, loader: ObservationLoader, repository: MonthlyRepository) -> None:
        self._loader = loader
        self._repository = repository

    def load_standardized_files(self, standardized_dir: Path) -> None:
        files = sorted(standardized_dir.glob("standardized_*.csv"))
        if not files:
            raise FileNotFoundError(
                f"no standardized files found in {standardized_dir}"
            )

        for file_path in files:
            rows = self._loader.iter_rows(file_path)
            self._repository.replace_source_file_rows(file_path.name, rows)
            print(f"loaded {file_path.name}: {len(rows)} rows")

    def refresh_aggregates(self) -> None:
        self._repository.refresh_item_monthly_price()
        print("refreshed inflacao_brasil.item_monthly_price")


def _detect_separator(file_path: Path) -> str:
    sample = file_path.read_text(encoding="utf-8", errors="ignore")[:4096]
    if sample.count(";") > sample.count(","):
        return ";"
    return ","


def main() -> int:
    args = _parse_args()
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        print("DATABASE_URL is required")
        return 1

    try:
        psycopg = importlib.import_module("psycopg")
    except ImportError:
        print("Missing dependency: psycopg. Install with: pip install psycopg[binary]")
        return 1

    try:
        with psycopg.connect(database_url) as conn:
            if args.month_ref_source == "source_file_name":
                month_ref_resolver: MonthRefResolver = SourceFileMonthRefResolver()
            else:
                month_ref_resolver = ReferenceDateMonthRefResolver()

            service = MonthlySeriesService(
                loader=CsvObservationLoader(ObservationParser(month_ref_resolver)),
                repository=MonthlySeriesRepository(conn),
            )
            if not args.refresh_only:
                service.load_standardized_files(args.standardized_dir)
            service.refresh_aggregates()
            conn.commit()
    except Exception as exc:
        print(f"error: {exc}")
        return 1

    print("done")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
