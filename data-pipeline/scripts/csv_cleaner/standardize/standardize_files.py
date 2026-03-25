import re
import pandas as pd


UNITS_TO_REMOVE_IN_PRODUTO = ("KG", "ML", "LITRO")


def clean_produto_units(produto: str) -> str:
    if pd.isna(produto):
        return ""

    text = str(produto)
    units_pattern = "|".join(re.escape(unit) for unit in UNITS_TO_REMOVE_IN_PRODUTO)
    text = re.sub(rf"\b\d+(?:[\.,]\d+)?\s*(?:{units_pattern})\b", " ", text, flags=re.IGNORECASE)
    text = re.sub(rf"\b(?:{units_pattern})\b", " ", text, flags=re.IGNORECASE)
    text = re.sub(r"\s+", " ", text).strip()
    return text

def split_produto_marca(produto: str) -> tuple[str, str]:
    if pd.isna(produto):
        return "", ""

    text = re.sub(r"\s+", " ", clean_produto_units(produto)).strip()
    if not text:
        return "", ""

    # caso padrão: "PRODUTO -MARCA" ou "PRODUTO - MARCA"
    parts = re.split(r"\s*-\s*", text, maxsplit=1)
    if len(parts) == 2 and parts[1].strip():
        return parts[0].strip(), parts[1].strip()

    # casos com hífen sobrando no fim: "FERMENTO BIOLÓGICO -"
    text = re.sub(r"\s*-\s*$", "", text).strip()
    return text, ""


def split_produto_marca_old_format(produto: str) -> tuple[str, str]:
    if pd.isna(produto):
        return "", ""

    text = re.sub(r"\s+", " ", clean_produto_units(produto)).strip()
    if not text:
        return "", ""

    match = re.search(r"^(.*?\b(?:CERVEJA|REFRIGERANTE)\b)(?:\s+(.+))?$", text, flags=re.IGNORECASE)
    if not match:
        return text, ""

    produto_base = re.sub(r"\s+", " ", match.group(1)).strip(" -")
    marca = re.sub(r"\s+", " ", match.group(2) or "").strip(" -")
    return produto_base, marca


def add_marca_column_from_cerveja(
    df: pd.DataFrame,
    produto_column: str = "produto",
    preco_column: str = "preco",
    marca_column: str = "marca",
    use_old_format_rules: bool = True,
) -> pd.DataFrame:
    if produto_column not in df.columns:
        return df

    updated = df.copy()
    if use_old_format_rules:
        produto_marca_pairs = updated[produto_column].map(split_produto_marca_old_format)
    else:
        produto_marca_pairs = updated[produto_column].map(split_produto_marca)

    updated[produto_column] = produto_marca_pairs.map(lambda pair: pair[0])
    marca_series = produto_marca_pairs.map(lambda pair: pair[1])

    if marca_column in updated.columns:
        updated = updated.drop(columns=[marca_column])

    if preco_column in updated.columns:
        insert_idx = updated.columns.get_loc(preco_column)
    else:
        insert_idx = updated.columns.get_loc(produto_column) + 1

    updated.insert(insert_idx, marca_column, marca_series)
    return updated
