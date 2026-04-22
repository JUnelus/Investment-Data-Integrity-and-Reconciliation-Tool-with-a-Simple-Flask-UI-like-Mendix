from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd


REQUIRED_COLUMNS = [
    "CUSIP",
    "Security Name",
    "Asset Class",
    "Issue Date",
    "Maturity Date",
    "Coupon Rate",
    "Outstanding Balance",
    "Market Price",
    "Currency",
    "Country",
    "Exchange",
]

CANONICAL_COLUMNS = {
    "CUSIP": "cusip",
    "Security Name": "security_name",
    "Asset Class": "asset_class",
    "Issue Date": "issue_date",
    "Maturity Date": "maturity_date",
    "Coupon Rate": "coupon_rate",
    "Outstanding Balance": "outstanding_balance",
    "Market Price": "market_price",
    "Currency": "currency",
    "Country": "country",
    "Exchange": "exchange",
}

DISPLAY_NAMES = {value: key for key, value in CANONICAL_COLUMNS.items()}

COMPARISON_FIELDS = [
    "security_name",
    "asset_class",
    "issue_date",
    "maturity_date",
    "coupon_rate",
    "outstanding_balance",
    "market_price",
    "currency",
    "country",
    "exchange",
]

NUMERIC_FIELDS = {"coupon_rate", "outstanding_balance", "market_price"}
NUMERIC_TOLERANCES = {
    "coupon_rate": 0.0001,
    "outstanding_balance": 1.0,
    "market_price": 0.01,
}


class ReconciliationError(ValueError):
    """Raised when an uploaded dataset cannot be reconciled."""


def reconcile_data(
    source_a_path: str | Path,
    source_b_path: str | Path,
    report_path: str | Path,
    source_a_label: str = "Source A",
    source_b_label: str = "Source B",
) -> dict[str, Any]:
    source_a = _load_security_master(source_a_path, source_a_label)
    source_b = _load_security_master(source_b_path, source_b_label)

    source_a_duplicates = _build_duplicate_records(source_a, source_a_label)
    source_b_duplicates = _build_duplicate_records(source_b, source_b_label)

    source_a_compare = source_a.drop_duplicates(subset="cusip", keep="first").copy()
    source_b_compare = source_b.drop_duplicates(subset="cusip", keep="first").copy()

    merged = source_a_compare.merge(
        source_b_compare,
        on="cusip",
        how="outer",
        suffixes=("_a", "_b"),
        indicator=True,
    )

    discrepancies: list[dict[str, Any]] = []
    source_a_only_rows: list[dict[str, Any]] = []
    source_b_only_rows: list[dict[str, Any]] = []

    for duplicate_record in source_a_duplicates + source_b_duplicates:
        discrepancies.append(duplicate_record)

    matched_records = 0
    field_mismatch_count = 0

    for _, row in merged.iterrows():
        merge_state = row["_merge"]
        cusip = _stringify(row["cusip"])
        security_name = _pick_value(row.get("security_name_a"), row.get("security_name_b"))
        asset_class = _pick_value(row.get("asset_class_a"), row.get("asset_class_b"))
        currency = _pick_value(row.get("currency_a"), row.get("currency_b"))

        if merge_state == "left_only":
            source_a_only_rows.append(_build_unmatched_row(row, "a"))
            discrepancies.append(
                _build_discrepancy_record(
                    cusip=cusip,
                    security_name=security_name,
                    issue_type="Missing in Source B",
                    field_name="Record Coverage",
                    source_a_value="Present",
                    source_b_value="Missing",
                    severity="Critical",
                    asset_class=asset_class,
                    currency=currency,
                    notes=f"{source_a_label} contains a security not found in {source_b_label}.",
                )
            )
            continue

        if merge_state == "right_only":
            source_b_only_rows.append(_build_unmatched_row(row, "b"))
            discrepancies.append(
                _build_discrepancy_record(
                    cusip=cusip,
                    security_name=security_name,
                    issue_type="Missing in Source A",
                    field_name="Record Coverage",
                    source_a_value="Missing",
                    source_b_value="Present",
                    severity="Critical",
                    asset_class=asset_class,
                    currency=currency,
                    notes=f"{source_b_label} contains a security not found in {source_a_label}.",
                )
            )
            continue

        matched_records += 1

        for field in COMPARISON_FIELDS:
            source_a_value = row.get(f"{field}_a")
            source_b_value = row.get(f"{field}_b")

            if _values_match(field, source_a_value, source_b_value):
                continue

            field_mismatch_count += 1
            discrepancies.append(
                _build_discrepancy_record(
                    cusip=cusip,
                    security_name=security_name,
                    issue_type="Field Mismatch",
                    field_name=DISPLAY_NAMES[field],
                    source_a_value=_format_value(field, source_a_value),
                    source_b_value=_format_value(field, source_b_value),
                    severity=_severity_for_field(field),
                    asset_class=asset_class,
                    currency=currency,
                    notes=_build_mismatch_note(field, source_a_label, source_b_label),
                )
            )

    summary = _build_summary(
        source_a_label=source_a_label,
        source_b_label=source_b_label,
        source_a_rows=len(source_a),
        source_b_rows=len(source_b),
        source_a_unique=source_a_compare["cusip"].nunique(),
        source_b_unique=source_b_compare["cusip"].nunique(),
        matched_records=matched_records,
        discrepancy_count=len(discrepancies),
        field_mismatch_count=field_mismatch_count,
        source_a_only_count=len(source_a_only_rows),
        source_b_only_count=len(source_b_only_rows),
        source_a_duplicate_count=len(source_a_duplicates),
        source_b_duplicate_count=len(source_b_duplicates),
        discrepancies=discrepancies,
    )

    _write_report(
        report_path=report_path,
        summary=summary,
        discrepancies=discrepancies,
        source_a_only_rows=source_a_only_rows,
        source_b_only_rows=source_b_only_rows,
        source_a_label=source_a_label,
        source_b_label=source_b_label,
    )

    return {
        "summary": summary,
        "discrepancies": discrepancies,
        "source_a_only": source_a_only_rows,
        "source_b_only": source_b_only_rows,
    }


def _load_security_master(file_path: str | Path, source_label: str) -> pd.DataFrame:
    try:
        data_frame = pd.read_csv(str(file_path), dtype=str).fillna("")
    except Exception as exc:  # pragma: no cover - defensive guard for IO/parser issues
        raise ReconciliationError(f"Unable to read {source_label} CSV file: {exc}") from exc

    missing_columns = [column for column in REQUIRED_COLUMNS if column not in data_frame.columns]
    if missing_columns:
        raise ReconciliationError(
            f"{source_label} is missing required columns: {', '.join(missing_columns)}"
        )

    data_frame = data_frame[REQUIRED_COLUMNS].copy()
    data_frame = data_frame.rename(columns=CANONICAL_COLUMNS)

    for column in data_frame.columns:
        data_frame[column] = data_frame[column].map(_clean_scalar)

    data_frame["cusip"] = data_frame["cusip"].astype(str).str.strip()
    data_frame = data_frame[data_frame["cusip"] != ""].copy()

    if data_frame.empty:
        raise ReconciliationError(f"{source_label} contains no valid CUSIP records after cleaning.")

    for field in NUMERIC_FIELDS:
        data_frame[field] = data_frame[field].map(_parse_numeric)

    return data_frame


def _clean_scalar(value: Any) -> Any:
    if pd.isna(value):
        return ""
    if isinstance(value, str):
        cleaned = value.strip()
        if "  #" in cleaned:
            cleaned = cleaned.split("  #", 1)[0].strip()
        return cleaned
    return value


def _parse_numeric(value: Any) -> float | None:
    if value in (None, ""):
        return None

    cleaned = str(value).replace(",", "").replace("%", "").strip()
    if cleaned.upper() in {"N/A", "NA", "NONE", "NULL"}:
        return None

    try:
        return float(cleaned)
    except ValueError:
        return None


def _build_duplicate_records(data_frame: pd.DataFrame, source_label: str) -> list[dict[str, Any]]:
    duplicates = data_frame[data_frame.duplicated(subset="cusip", keep=False)]
    records: list[dict[str, Any]] = []

    for _, row in duplicates.iterrows():
        records.append(
            _build_discrepancy_record(
                cusip=_stringify(row.get("cusip")),
                security_name=_stringify(row.get("security_name")),
                issue_type="Duplicate CUSIP",
                field_name="CUSIP",
                source_a_value=source_label,
                source_b_value="Duplicate record",
                severity="High",
                asset_class=_stringify(row.get("asset_class")),
                currency=_stringify(row.get("currency")),
                notes=f"{source_label} contains duplicate CUSIP rows that should be reviewed.",
            )
        )

    return records


def _build_unmatched_row(row: pd.Series, suffix: str) -> dict[str, Any]:
    return {
        "cusip": _stringify(row.get("cusip")),
        "security_name": _stringify(row.get(f"security_name_{suffix}")),
        "asset_class": _stringify(row.get(f"asset_class_{suffix}")),
        "market_price": _format_value("market_price", row.get(f"market_price_{suffix}")),
        "outstanding_balance": _format_value(
            "outstanding_balance", row.get(f"outstanding_balance_{suffix}")
        ),
        "currency": _stringify(row.get(f"currency_{suffix}")),
        "country": _stringify(row.get(f"country_{suffix}")),
        "exchange": _stringify(row.get(f"exchange_{suffix}")),
    }


def _build_discrepancy_record(
    *,
    cusip: str,
    security_name: str,
    issue_type: str,
    field_name: str,
    source_a_value: Any,
    source_b_value: Any,
    severity: str,
    asset_class: str,
    currency: str,
    notes: str,
) -> dict[str, Any]:
    return {
        "cusip": cusip,
        "security_name": security_name,
        "issue_type": issue_type,
        "field_name": field_name,
        "source_a_value": _stringify(source_a_value),
        "source_b_value": _stringify(source_b_value),
        "severity": severity,
        "asset_class": asset_class,
        "currency": currency,
        "notes": notes,
    }


def _build_summary(
    *,
    source_a_label: str,
    source_b_label: str,
    source_a_rows: int,
    source_b_rows: int,
    source_a_unique: int,
    source_b_unique: int,
    matched_records: int,
    discrepancy_count: int,
    field_mismatch_count: int,
    source_a_only_count: int,
    source_b_only_count: int,
    source_a_duplicate_count: int,
    source_b_duplicate_count: int,
    discrepancies: list[dict[str, Any]],
) -> dict[str, Any]:
    issue_breakdown = _count_by_key(discrepancies, "issue_type")
    severity_breakdown = _count_by_key(discrepancies, "severity")
    field_breakdown = _count_by_key(discrepancies, "field_name")

    total_unique = max(source_a_unique, source_b_unique, 1)
    match_rate = round((matched_records / total_unique) * 100, 2)

    return {
        "source_a_label": source_a_label,
        "source_b_label": source_b_label,
        "source_a_rows": source_a_rows,
        "source_b_rows": source_b_rows,
        "source_a_unique": source_a_unique,
        "source_b_unique": source_b_unique,
        "matched_records": matched_records,
        "discrepancy_count": discrepancy_count,
        "field_mismatch_count": field_mismatch_count,
        "source_a_only_count": source_a_only_count,
        "source_b_only_count": source_b_only_count,
        "source_a_duplicate_count": source_a_duplicate_count,
        "source_b_duplicate_count": source_b_duplicate_count,
        "match_rate": match_rate,
        "issue_breakdown": issue_breakdown,
        "severity_breakdown": severity_breakdown,
        "field_breakdown": field_breakdown,
    }


def _write_report(
    *,
    report_path: str | Path,
    summary: dict[str, Any],
    discrepancies: list[dict[str, Any]],
    source_a_only_rows: list[dict[str, Any]],
    source_b_only_rows: list[dict[str, Any]],
    source_a_label: str,
    source_b_label: str,
) -> None:
    report_path = Path(report_path)
    report_path.parent.mkdir(parents=True, exist_ok=True)

    summary_frame = pd.DataFrame(
        [
            {"Metric": "Source A", "Value": source_a_label},
            {"Metric": "Source B", "Value": source_b_label},
            {"Metric": "Source A Rows", "Value": summary["source_a_rows"]},
            {"Metric": "Source B Rows", "Value": summary["source_b_rows"]},
            {"Metric": "Source A Unique CUSIPs", "Value": summary["source_a_unique"]},
            {"Metric": "Source B Unique CUSIPs", "Value": summary["source_b_unique"]},
            {"Metric": "Matched Records", "Value": summary["matched_records"]},
            {"Metric": "Total Discrepancies", "Value": summary["discrepancy_count"]},
            {"Metric": "Field Mismatches", "Value": summary["field_mismatch_count"]},
            {"Metric": "Only in Source A", "Value": summary["source_a_only_count"]},
            {"Metric": "Only in Source B", "Value": summary["source_b_only_count"]},
            {"Metric": "Source A Duplicate Rows", "Value": summary["source_a_duplicate_count"]},
            {"Metric": "Source B Duplicate Rows", "Value": summary["source_b_duplicate_count"]},
            {"Metric": "Match Rate (%)", "Value": summary["match_rate"]},
        ]
    )

    issue_breakdown = pd.DataFrame(
        [
            {"Issue Type": key, "Count": value}
            for key, value in summary["issue_breakdown"].items()
        ]
    )
    severity_breakdown = pd.DataFrame(
        [
            {"Severity": key, "Count": value}
            for key, value in summary["severity_breakdown"].items()
        ]
    )
    discrepancies_frame = pd.DataFrame(discrepancies)
    source_a_only_frame = pd.DataFrame(source_a_only_rows)
    source_b_only_frame = pd.DataFrame(source_b_only_rows)

    with pd.ExcelWriter(report_path, engine="openpyxl") as writer:
        summary_frame.to_excel(writer, index=False, sheet_name="Summary")
        issue_breakdown.to_excel(writer, index=False, sheet_name="Issue Breakdown")
        severity_breakdown.to_excel(writer, index=False, sheet_name="Severity Breakdown")
        discrepancies_frame.to_excel(writer, index=False, sheet_name="Discrepancies")
        source_a_only_frame.to_excel(writer, index=False, sheet_name="Only in Source A")
        source_b_only_frame.to_excel(writer, index=False, sheet_name="Only in Source B")


def _count_by_key(records: list[dict[str, Any]], key: str) -> dict[str, int]:
    breakdown: dict[str, int] = {}
    for record in records:
        bucket = record.get(key) or "Unknown"
        breakdown[bucket] = breakdown.get(bucket, 0) + 1
    return breakdown


def _values_match(field: str, left: Any, right: Any) -> bool:
    if field in NUMERIC_FIELDS:
        if _is_missing(left) and _is_missing(right):
            return True
        if _is_missing(left) or _is_missing(right):
            return False
        return abs(float(left) - float(right)) <= NUMERIC_TOLERANCES[field]

    return _normalize_text(left) == _normalize_text(right)


def _normalize_text(value: Any) -> str:
    if value in (None, ""):
        return ""
    return str(value).strip().lower()


def _pick_value(*values: Any) -> str:
    for value in values:
        stringified = _stringify(value)
        if stringified:
            return stringified
    return ""


def _format_value(field: str, value: Any) -> str:
    if _is_missing(value):
        return ""
    if field == "coupon_rate":
        return f"{float(value):.2f}%"
    if field in {"outstanding_balance", "market_price"}:
        return f"{float(value):,.2f}"
    return _stringify(value)


def _severity_for_field(field: str) -> str:
    if field in {"outstanding_balance", "asset_class", "currency"}:
        return "High"
    if field in {"market_price", "exchange", "country"}:
        return "Medium"
    return "Low"


def _build_mismatch_note(field: str, source_a_label: str, source_b_label: str) -> str:
    return f"{DISPLAY_NAMES[field]} differs between {source_a_label} and {source_b_label}."


def _stringify(value: Any) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""
    return str(value)


def _is_missing(value: Any) -> bool:
    return value is None or value == "" or pd.isna(value)
