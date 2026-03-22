#!/usr/bin/env python3
"""Shared utilities for CSV cleaning operations and CLI output formatting."""

from datetime import datetime, timedelta
from typing import Callable, Optional, Tuple


SECTION_LINE = "=" * 70


def print_section(title: str) -> None:
    print(f"\n{SECTION_LINE}")
    print(title)
    print(SECTION_LINE)


def print_results_summary(title: str, results: dict[str, bool]) -> int:
    print_section(title)
    for name, success in results.items():
        status = "✓ SUCCESS" if success else "✗ PARTIAL or FAIL"
        print(f"{name:20} {status}")
    return 0 if all(results.values()) else 1


def find_latest_valid_date(
    df,
    date_parser: Callable[[str], Optional[datetime]],
    date_column: str = "data_pesquisa",
) -> Optional[str]:
    valid_dates = [value for value in df[date_column].unique() if date_parser(value) is not None]
    if not valid_dates:
        return None
    return sorted(valid_dates, key=date_parser)[-1]


def resolve_target_date(
    df,
    target_date_str: str,
    target_parser: Callable[[str], Optional[datetime]],
    df_parser: Callable[[str], Optional[datetime]],
    df_date_format_str: str,
    max_days_offset: int = 7,
    fallback_to_latest: bool = False,
    date_column: str = "data_pesquisa",
) -> Optional[str]:
    if target_date_str.lower() == "latest":
        latest = find_latest_valid_date(df, df_parser, date_column=date_column)
        if latest is None:
            print("✗ No valid dates found")
            return None
        print(f"Using latest date: {latest}")
        return latest

    result = find_date_in_dataframe(
        df,
        target_date_str,
        target_parser,
        df_parser,
        df_date_format_str,
        max_days_offset=max_days_offset,
    )
    if result is not None:
        return result[0]

    if not fallback_to_latest:
        return None

    latest = find_latest_valid_date(df, df_parser, date_column=date_column)
    if latest is None:
        print("✗ No valid dates found")
        return None

    print(f"⚠ Target date not found near {target_date_str}; using latest available date: {latest}")
    return latest


def extract_date_from_filename(filename: str, target_day: int = 20) -> Optional[str]:
    """
    Extract date from filename in format: YYYY-MM-DD_*.csv
    Returns the specified day of the same month in DD/MM/YYYY format.
    """
    try:
        date_part = filename.split('_')[0]
        file_date = datetime.strptime(date_part, "%Y-%m-%d")
        target_date = file_date.replace(day=target_day)
        return target_date.strftime("%d/%m/%Y")
    except Exception as exc:
        print(f"✗ Could not extract date from filename {filename}: {exc}")
        return None


def find_date_in_dataframe(
    df,
    target_date_str: str,
    target_parser: Callable[[str], Optional[datetime]],
    df_parser: Callable[[str], Optional[datetime]],
    df_date_format_str: str = "%Y-%m-%d",
    max_days_offset: int = 7,
) -> Optional[Tuple[str, datetime]]:
    """
    Find target date in dataframe, iterating forward then backward if not found.
    
    Args:
        df: DataFrame with 'data_pesquisa' column
        target_date_str: Target date string
        target_parser: Function to parse the target date (e.g., parse_date for DD/MM/YYYY)
        df_parser: Function to parse dates from dataframe (e.g., parse_date_iso for YYYY-MM-DD)
        df_date_format_str: Format string for dates in dataframe
        max_days_offset: Maximum days to iterate forward/backward
    
    Returns:
        Tuple of (date_str, datetime_obj) or None if not found
    """
    target_date = target_parser(target_date_str)
    if target_date is None:
        print(f"✗ Invalid target date format: {target_date_str}")
        return None
    
    available_dates = df["data_pesquisa"].unique()
    available_dates = sorted([d for d in available_dates if df_parser(d) is not None])
    
    if not available_dates:
        print("✗ No valid dates found in the CSV")
        return None
    
    print(f"Target date: {target_date_str}")
    
    for day_offset in range(max_days_offset + 1):
        search_date = target_date + timedelta(days=day_offset)
        search_date_str = search_date.strftime(df_date_format_str)
        
        if search_date_str in available_dates:
            if day_offset == 0:
                print(f"✓ Found exact target date: {search_date_str}")
            else:
                print(f"✓ Found date after +{day_offset} day(s): {search_date_str}")
            return search_date_str, search_date
    
    print(f"✗ No date found within {max_days_offset} days forward, trying backward...")
    for day_offset in range(1, max_days_offset + 1):
        search_date = target_date - timedelta(days=day_offset)
        search_date_str = search_date.strftime(df_date_format_str)
        
        if search_date_str in available_dates:
            print(f"✓ Found date after -{day_offset} day(s) backward: {search_date_str}")
            return search_date_str, search_date
    
    print(f"✗ No valid date found within ±{max_days_offset} days from {target_date_str}")
    return None


