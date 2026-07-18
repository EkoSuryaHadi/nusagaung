#!/usr/bin/env python3
from __future__ import annotations
"""
Gaung ETL Worker — processes pipeline steps sequentially.

Usage: python3 etl_runner.py /tmp/gaung_pipeline_<runId>.json

Pipeline config format:
{
  "pipelineId": 1,
  "runId": 1,
  "source": { "filePath": "sales.csv", "fileSize": 1234 },
  "steps": [
    { "type": "SOURCE",  "config": {}, "order": 0 },
    { "type": "CLEAN",   "config": { "stripWhitespace": true, "deduplicate": true }, "order": 1 },
    { "type": "OUTPUT",  "config": {}, "order": 2, "outputLayer": "SILVER", "outputTable": "sales_clean" }
  ]
}
"""

import json
import re as _re
import sys
import os
import datetime
from pathlib import Path
from datetime import datetime, timezone

import pandas as pd
from sqlalchemy import create_engine, text

# ============================================================
# Helpers
# ============================================================

# Allowed schema names for lakehouse layers
_VALID_LAYERS = {"bronze", "silver", "gold"}


def sanitize_identifier(name: str) -> str:
    """Sanitize a SQL identifier (table/schema name) to prevent SQL injection.
    
    Only allows alphanumeric characters, underscores, hyphens, and dots.
    Double-quotes internal double-quotes for safe quoting.
    """
    if not name:
        raise ValueError("Identifier cannot be empty")
    # Remove or escape dangerous characters
    sanitized = _re.sub(r'[^\w\-.]', '_', name)
    # Escape double quotes for PostgreSQL quoted identifiers
    sanitized = sanitized.replace('"', '""')
    return sanitized


def sanitize_layer(layer: str) -> str:
    """Validate and return a safe layer/schema name."""
    layer = layer.lower().strip()
    if layer not in _VALID_LAYERS:
        raise ValueError(f"Invalid layer '{layer}'. Must be one of: {_VALID_LAYERS}")
    return layer


def clean_db_url(url: str) -> str:
    """Strip Prisma-specific query parameters like ?schema=public that psycopg2 rejects."""
    if not url:
        return url
    from urllib.parse import urlparse, parse_qs, urlencode, urlunparse
    try:
        parsed = urlparse(url)
        if parsed.query:
            qs = parse_qs(parsed.query)
            qs.pop("schema", None)
            new_query = urlencode(qs, doseq=True)
            return urlunparse((parsed.scheme, parsed.netloc, parsed.path, parsed.params, new_query, parsed.fragment))
    except Exception:
        pass
    return url


def get_engine():
    """Create SQLAlchemy engine from DATABASE_URL env var."""
    db_url = clean_db_url(os.environ.get("DATABASE_URL", ""))
    if not db_url:
        raise RuntimeError("DATABASE_URL not set")
    return create_engine(db_url)


def to_numeric_clean(series: pd.Series) -> pd.Series:
    """Clean currency symbols, common units/suffixes, and handle both English and Indonesian number formats.
    
    Fully vectorized C-level execution for high performance on large datasets.
    """
    if pd.api.types.is_numeric_dtype(series):
        return series

    s = series.astype(str).str.strip()
    
    # Handle accounting negative format: (123.45) -> -123.45
    is_negative = s.str.startswith('(') & s.str.endswith(')')
    s = s.where(~is_negative, s.str.slice(1, -1).str.strip())

    # Remove currency symbols and unit suffixes vectorized
    s = s.str.replace(r'^(?:IDR|Rp|USD|EUR|SGD|\$)\s*', '', regex=True, flags=_re.IGNORECASE)
    s = s.str.replace(r'\s*(?:IDR|Rp|USD|EUR|SGD|\$)$', '', regex=True, flags=_re.IGNORECASE)
    s = s.str.replace(r'\s*(?:%|kg|gram|g|pcs|box|unit|hpa|mbar|°c|c|°e|e|°w|w|°n|n|°s|s|ton|meter|m|cm|mm|inch)\.?$', '', regex=True, flags=_re.IGNORECASE)

    # Clean separators: Indonesian 1.234,56 -> 1234.56 vs English 1,234.56 -> 1234.56
    has_both = s.str.contains(r'\.', regex=True) & s.str.contains(r',', regex=True)
    indo_mask = has_both & (s.str.rfind('.') < s.str.rfind(','))
    
    s_indo = s.where(~indo_mask, s.str.replace('.', '', regex=False).str.replace(',', '.', regex=False))
    s_eng = s_indo.where(indo_mask, s_indo.str.replace(',', '', regex=False))

    # Also handle single-dot thousands separators (e.g. "250.000" -> "250000")
    dot_thousands = ~has_both & s_eng.str.match(r'^-?\d{1,3}(?:\.\d{3})+$', na=False)
    s_eng = s_eng.where(~dot_thousands, s_eng.str.replace('.', '', regex=False))

    # Convert to numeric safely
    num_series = pd.to_numeric(s_eng, errors='coerce')
    num_series = num_series.where(~is_negative, -num_series)
    return num_series


def infer_and_clean_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Automatically infer types for object/string columns and clean/cast them.
    
    Tolerates diverse source formats (like dates, numbers with units) and heals them.
    """
    import re
    result = df
    
    # We only process 'object' and 'string' columns
    obj_cols = result.select_dtypes(include=["object", "string"]).columns
    
    for col in obj_cols:
        # Exclude metadata columns
        if col.startswith("_"):
            continue
            
        non_null_series = result[col].dropna()
        # Strip string values for better checking
        non_null_series = non_null_series.astype(str).str.strip()
        # Filter out empty and null-like strings ("null", "none", "nan", "")
        non_null_series = non_null_series[~non_null_series.str.lower().isin(["", "null", "none", "nan"])]
        
        n_total = len(non_null_series)
        if n_total == 0:
            # Schema Hardening: if the column is entirely null, try to infer its type from the name
            col_lower = str(col).lower()
            if any(x in col_lower for x in ["debit", "debet", "credit", "kredit", "balance", "saldo", "amount", "total", "nominal", "gaji", "price", "harga"]):
                result[col] = pd.to_numeric(result[col], errors="coerce")
                print(f"[SCHEMA-HARDEN] Hardened empty column '{col}' to NUMERIC")
            elif any(x in col_lower for x in ["date", "time", "timestamp", "tanggal"]):
                result[col] = pd.to_datetime(result[col], errors="coerce")
                print(f"[SCHEMA-HARDEN] Hardened empty column '{col}' to DATE/TIMESTAMP")
            continue
            
        # 1. Try Numeric detection & cleaning
        cleaned_numeric = to_numeric_clean(non_null_series)
        numeric_converted = pd.to_numeric(cleaned_numeric, errors="coerce")
        n_valid_numeric = numeric_converted.notna().sum()
        
        if n_total > 0 and (n_valid_numeric / n_total) >= 0.85:
            # More than 85% of values can be numeric. Clean and cast the entire column!
            col_lower = col.lower()
            is_debit_credit = any(k in col_lower for k in ["debit", "debet", "credit", "kredit"])
            
            raw_cleaned = to_numeric_clean(result[col])
            if is_debit_credit:
                result[col] = pd.to_numeric(raw_cleaned, errors="coerce").fillna(0.0)
            else:
                result[col] = pd.to_numeric(raw_cleaned, errors="coerce")
                
            print(f"[AUTO-INFER] Converted column '{col}' to NUMERIC")
            continue
            
        # 2. Try Date detection & cleaning
        date_pattern = re.compile(r'[-/.]|[a-zA-Z]{3,}', re.IGNORECASE)
        
        # Check a sample of values to see if they fit the pattern
        sample_vals = non_null_series.head(50)
        pattern_matches = sample_vals.apply(lambda x: bool(date_pattern.search(x) and len(x) >= 5 and not x.isdigit()))
        
        if len(sample_vals) > 0 and (pattern_matches.sum() / len(sample_vals)) >= 0.80:
            # Try parsing the sample element-by-element to handle mixed date formats correctly
            parsed_sample = sample_vals.apply(lambda x: pd.to_datetime(x, errors="coerce", dayfirst=True))
            n_valid_dates = parsed_sample.notna().sum()
            
            if n_valid_dates / len(sample_vals) >= 0.85:
                # More than 85% of sample values can be parsed as dates. Cast the entire column!
                def parse_date_element(x):
                    if pd.isna(x) or str(x).strip() == "" or str(x).strip().lower() == "null":
                        return pd.NaT
                    try:
                        dt = pd.to_datetime(x, dayfirst=True)
                        if dt.tzinfo is not None:
                            local_tz = datetime.datetime.now().astimezone().tzinfo
                            dt = dt.tz_convert(local_tz).tz_localize(None)
                        return dt
                    except Exception:
                        return pd.NaT
                        
                result[col] = result[col].apply(parse_date_element)
                print(f"[AUTO-INFER] Converted column '{col}' to DATE/TIMESTAMP")
                continue
                
    return result


def translate_format(fmt: str) -> str | None:
    """Translate user-friendly date format to Python strftime format (case-insensitive for tokens)."""
    if not fmt:
        return None
    fmt = fmt.strip().strip('"').strip("'")
    
    t = fmt
    import re
    # Year: YYYY -> %Y, YY -> %y
    t = re.sub(r'yyyy', '%Y', t, flags=re.IGNORECASE)
    t = re.sub(r'\byy\b', '%y', t, flags=re.IGNORECASE)
    t = t.replace('YY', '%y').replace('yy', '%y')
    
    # Day: DD -> %d, dd -> %d
    t = re.sub(r'dd', '%d', t, flags=re.IGNORECASE)
    
    # Month vs Minute:
    # If there is no HH/hh in the format, 'mm' is treated as month.
    has_time = 'hh' in t.lower() or 'ss' in t.lower()
    if not has_time:
        t = re.sub(r'mm', '%m', t, flags=re.IGNORECASE)
    else:
        # Case-sensitive replace: MM for month, mm for minute
        t = t.replace('MM', '%m')
        t = t.replace('mm', '%M')
        
    # Hour: HH -> %H, hh -> %H
    t = re.sub(r'hh', '%H', t, flags=re.IGNORECASE)
    return t


def find_column_robust(df: pd.DataFrame, col_name: str) -> str | None:
    """Find column name in DataFrame with case-insensitive and underscore/space-insensitive matching."""
    if not col_name:
        return None
    if col_name in df.columns:
        return col_name
    
    # Normalize input: lowercase, replace underscores with spaces, strip spaces
    norm_input = col_name.lower().replace("_", " ").replace(" ", "").strip()
    
    for col in df.columns:
        norm_col = str(col).lower().replace("_", " ").replace(" ", "").strip()
        if norm_col == norm_input:
            return col
            
    # Substring fallback (e.g. "Debit" matches "Debit (IDR)")
    for col in df.columns:
        col_str = str(col).lower()
        if norm_input in col_str or col_str in norm_input:
            return col
            
    return None



def _detect_csv_params(file_path) -> dict:
    """Detect CSV delimiter and encoding by reading a small sample with Python engine."""
    import csv
    for encoding in ('utf-8', 'latin1'):
        try:
            with open(file_path, 'r', encoding=encoding, errors='strict') as f:
                sample = f.read(8192)
            dialect = csv.Sniffer().sniff(sample, delimiters=',;\t|')
            return {"sep": dialect.delimiter, "encoding": encoding}
        except Exception:
            continue
    # Fallback: comma + utf-8
    return {"sep": ",", "encoding": "utf-8"}


LOAD_CHUNK_SIZE = 50_000


def load_source(source: dict) -> pd.DataFrame:
    """Load CSV or Excel file from uploads directory.
    
    Uses chunked reading with C engine for performance on large files.
    Delimiter and encoding are auto-detected from a small sample first.
    """
    if not source.get("filePath"):
        raise ValueError("No filePath in source config")

    file_path = Path(os.getcwd()) / "uploads" / source["filePath"]
    if not file_path.exists():
        raise FileNotFoundError(f"Source file not found: {file_path}")

    ext = file_path.suffix.lower()
    file_size = source.get('fileSize', os.path.getsize(file_path))
    print(f"[SOURCE] Loading {file_path.name} ({file_size} bytes)")

    if ext in (".xlsx", ".xls"):
        df = pd.read_excel(file_path)
    else:
        # Detect delimiter and encoding from a small sample
        csv_params = _detect_csv_params(file_path)
        sep = csv_params["sep"]
        encoding = csv_params["encoding"]
        print(f"[SOURCE] Detected: sep={repr(sep)}, encoding={encoding}")

        try:
            # Use fast C engine with chunked reading for large files
            chunks = []
            reader = pd.read_csv(
                file_path,
                sep=sep,
                engine='c',
                encoding=encoding,
                chunksize=LOAD_CHUNK_SIZE,
                on_bad_lines='skip',
                low_memory=True,
            )
            for chunk in reader:
                chunks.append(chunk)
                print(f"[SOURCE] ... loaded chunk: {len(chunk)} rows")
            df = pd.concat(chunks, ignore_index=True) if chunks else pd.DataFrame()
        except Exception as e:
            # Fallback to Python engine (slower but more forgiving)
            print(f"[SOURCE] C engine failed ({e}), falling back to Python engine")
            try:
                df = pd.read_csv(file_path, sep=None, engine='python', encoding=encoding)
            except Exception:
                df = pd.read_csv(file_path, encoding='utf-8', errors='ignore')

    print(f"[SOURCE] Loaded {len(df)} rows, {len(df.columns)} columns")
    return df



def load_lakehouse_source(source: dict) -> pd.DataFrame:
    """Load data from an existing lakehouse table (bronze/silver/gold).
    
    Uses chunked reading via pandas read_sql to avoid loading entire tables
    into memory at once (prevents OOM for large tables).
    """
    table_name = source.get("sourceTable", "")
    source_layer = sanitize_layer(source.get("sourceLayer", "BRONZE"))

    if not table_name:
        raise ValueError("No sourceTable in lakehouse source config")

    safe_table = sanitize_identifier(table_name)
    engine = get_engine()
    full_table = f'{source_layer}."{safe_table}"'

    print(f"[SOURCE] Loading lakehouse table: {full_table}")

    # Use chunked reading to prevent OOM on large tables
    CHUNK_SIZE = 50_000
    chunks = []
    query = text(f'SELECT * FROM {full_table}')
    
    with engine.connect() as conn:
        for chunk in pd.read_sql(query, conn, chunksize=CHUNK_SIZE):
            chunks.append(chunk)
            print(f"[SOURCE] ... loaded chunk: {len(chunk)} rows")

    if not chunks:
        # Empty table — get columns from metadata
        with engine.connect() as conn:
            result = conn.execute(query)
            columns = list(result.keys())
        df = pd.DataFrame(columns=columns)
    else:
        df = pd.concat(chunks, ignore_index=True)

    print(f"[SOURCE] Loaded {len(df)} rows, {len(df.columns)} columns from {source_layer}")
    return df


def step_clean(df: pd.DataFrame, config: dict) -> pd.DataFrame:
    """Clean data: strip whitespace, deduplicate, fill nulls."""
    result = df
    rows_before = len(result)

    if config.get("stripWhitespace"):
        for col in result.select_dtypes(include=["object", "string"]).columns:
            result[col] = result[col].str.strip()
        print("[CLEAN] Stripped whitespace")

    # Run automatic type inference & auto-healing by default
    if config.get("autoTypeInference", True):
        result = infer_and_clean_columns(result)

    if config.get("deduplicate"):
        result = result.drop_duplicates()
        removed = rows_before - len(result)
        if removed > 0:
            print(f"[CLEAN] Removed {removed} duplicate rows")

    fill_nulls = config.get("fillNulls")
    # Parse string-encoded JSON (from frontend, stored as JSON-in-JSON)
    if isinstance(fill_nulls, str) and fill_nulls.strip().startswith('{'):
        try:
            fill_nulls = json.loads(fill_nulls)
        except (json.JSONDecodeError, ValueError):
            pass
    if fill_nulls:
        # Support two formats:
        # 1. Boolean + fillNullsValue (frontend): fill ALL object columns
        # 2. Dict (legacy): {"column": "value", ...} — fill specific columns
        # Template support: values can reference other columns like "REVIEW_{SAP_DocNo}"
        import re
        def resolve_template(val: str, row: pd.Series) -> str:
            """Replace {col_name} with actual row value."""
            def replacer(m):
                col = m.group(1)
                return str(row[col]) if col in row.index else m.group(0)
            return re.sub(r'\{(\w+)\}', replacer, val)
        
        if isinstance(fill_nulls, bool):
            fill_val = str(config.get("fillNullsValue", ""))
            has_template = '{' in fill_val
            for col in result.select_dtypes(include=["object", "string"]).columns:
                null_mask = result[col].isna()
                null_count = null_mask.sum()
                if null_count > 0:
                    if has_template:
                        # Per-row template resolution
                        result.loc[null_mask, col] = result.loc[null_mask].apply(
                            lambda row: resolve_template(fill_val, row), axis=1
                        )
                    else:
                        result[col] = result[col].fillna(fill_val)
                    print(f"[CLEAN] Filled {null_count} nulls in '{col}' with '{fill_val}'")
        elif isinstance(fill_nulls, dict):
            for col, val in fill_nulls.items():
                if col in result.columns:
                    val_str = str(val)
                    null_mask = result[col].isna()
                    null_count = null_mask.sum()
                    if null_count > 0:
                        if '{' in val_str:
                            result.loc[null_mask, col] = result.loc[null_mask].apply(
                                lambda row: resolve_template(val_str, row), axis=1
                            )
                        else:
                            result[col] = result[col].fillna(val_str)
                        print(f"[CLEAN] Filled {null_count} nulls in '{col}' with '{val_str}'")

    return result


def step_validate(df: pd.DataFrame, config: dict) -> pd.DataFrame:
    """Validate data: NOT_NULL, COMPARE, NUMBER range, DATE format, UNIQUE, REGEX, ENUM.
    
    Rules format (from frontend string or JSON array):
      NOT_NULL:Bank_Ref
      COMPARE:SAP_Amount,Bank_Amount,0
      NUMBER:Amount,min=0
      DATE:Transaction_Date,format=YYYY-MM-DD
      UNIQUE:Transaction_ID
      REGEX:Code,pattern=^[A-Z]{3}-\\d+$
      ENUM:Status,values=ACTIVE,INACTIVE,PENDING
    
    Mode: "flag" (default) — adds _validation_issues column
          "drop" — removes failing rows
    
    Supports legacy dict format: [{"column":"x","type":"number","min":0}]
    """
    result = df
    rules_raw = config.get("validationRules") or config.get("rules", [])
    
    # Parse rules string (frontend format: "NOT_NULL:col\\nCOMPARE:col1,col2,tolerance")
    parsed_rules = []
    if isinstance(rules_raw, str) and rules_raw.strip():
        for line in rules_raw.strip().split("\n"):
            line = line.strip()
            if not line or ":" not in line:
                continue
            rule_type, params = line.split(":", 1)
            rule_parts = [p.strip() for p in params.split(",")]
            
            if rule_type.upper() == "NOT_NULL" and rule_parts:
                parsed_rules.append({"type": "NOT_NULL", "column": rule_parts[0]})
            elif rule_type.upper() == "COMPARE" and len(rule_parts) >= 2:
                parsed_rules.append({"type": "COMPARE", "col1": rule_parts[0], "col2": rule_parts[1], "tolerance": float(rule_parts[2]) if len(rule_parts) > 2 else 0})
            elif rule_type.upper() == "NUMBER" and rule_parts:
                r = {"type": "NUMBER", "column": rule_parts[0]}
                for p in rule_parts[1:]:
                    if "=" in p:
                        k, v = p.split("=")
                        r[k.strip()] = float(v.strip())
                parsed_rules.append(r)
            elif rule_type.upper() == "DATE" and rule_parts:
                r = {"type": "DATE", "column": rule_parts[0], "format": None, "locale": None}
                for p in rule_parts[1:]:
                    if p.startswith("format="):
                        r["format"] = p.split("format=", 1)[1]
                    elif p.startswith("locale="):
                        r["locale"] = p.split("locale=", 1)[1]
                    elif "=" not in p:
                        r["format"] = p
                parsed_rules.append(r)
            elif rule_type.upper() == "UNIQUE" and params.strip():
                parsed_rules.append({"type": "UNIQUE", "column": params.strip()})
            elif rule_type.upper() == "REGEX" and "," in params:
                col, rest = params.split(",", 1)
                pattern = ""
                if "pattern=" in rest:
                    pattern = rest.split("pattern=", 1)[1].strip()
                parsed_rules.append({"type": "REGEX", "column": col.strip(), "pattern": pattern})
            elif rule_type.upper() == "ENUM" and "," in params:
                col, rest = params.split(",", 1)
                values_str = ""
                if "values=" in rest:
                    values_str = rest.split("values=", 1)[1].strip()
                values = [v.strip() for v in values_str.split(",") if v.strip()]
                parsed_rules.append({"type": "ENUM", "column": col.strip(), "values": values})
            elif rule_type.upper() == "OUTLIER" and rule_parts:
                r = {"type": "OUTLIER", "column": rule_parts[0], "method": "iqr", "threshold": 3.0}
                for p in rule_parts[1:]:
                    if "=" in p:
                        k, v = p.split("=")
                        k = k.strip().lower()
                        if k == "method":
                            r["method"] = v.strip().lower()
                        elif k == "threshold":
                            r["threshold"] = float(v.strip())
                parsed_rules.append(r)
    elif isinstance(rules_raw, list):
        parsed_rules = rules_raw
    
    mode = config.get("validationMode", "flag")
    if config.get("_source_category") == "Keuangan / Finance":
        print("[VALIDATE] Financial dataset detected: forcing 'flag' mode (no rows dropped for audit integrity)")
        mode = "flag"
    
    issues = pd.Series("", index=result.index, dtype="string")
    drop_mask = pd.Series(False, index=result.index)

    def flag_issues(mask: pd.Series, msg: str):
        nonlocal issues, drop_mask
        if mode == "drop":
            drop_mask = drop_mask | mask
        else:
            issues = issues.where(~mask, issues + msg + "; ")
    
    for rule in parsed_rules:
        rule_type = rule.get("type", "").upper()
        
        if rule_type == "NOT_NULL":
            col = find_column_robust(result, rule.get("column", ""))
            if col:
                null_mask = result[col].isna() | (result[col].astype(str).str.strip() == "")
                count = null_mask.sum()
                if count > 0:
                    flag_issues(null_mask, f"Missing {col}")
                    print(f"[VALIDATE] {count} rows with missing '{col}' {'dropped' if mode=='drop' else 'flagged'}")
        
        elif rule_type == "COMPARE":
            col1 = find_column_robust(result, rule.get("col1", ""))
            col2 = find_column_robust(result, rule.get("col2", ""))
            tol = rule.get("tolerance", 0)
            if col1 and col2:
                a = pd.to_numeric(to_numeric_clean(result[col1]), errors="coerce").fillna(0.0)
                b = pd.to_numeric(to_numeric_clean(result[col2]), errors="coerce").fillna(0.0)
                diff = (a - b).abs()
                mismatch = diff > tol
                count = mismatch.sum()
                if count > 0:
                    flag_issues(mismatch, f"Mismatch {col1} vs {col2}")
                    print(f"[VALIDATE] {count} rows with {col1} != {col2} {'dropped' if mode=='drop' else 'flagged'}")
        
        elif rule_type == "NUMBER":
            col = find_column_robust(result, rule.get("column", ""))
            if col:
                was_not_null = df[col].notna() & (df[col].astype(str).str.strip() != "")
                cleaned_nums = to_numeric_clean(result[col])
                
                col_lower = col.lower()
                is_debit_credit = any(k in col_lower for k in ["debit", "debet", "credit", "kredit"])
                if is_debit_credit:
                    result[col] = pd.to_numeric(cleaned_nums, errors="coerce").fillna(0.0)
                else:
                    result[col] = pd.to_numeric(cleaned_nums, errors="coerce")
                    
                invalid_mask = was_not_null & result[col].isna()
                count = invalid_mask.sum()
                if count > 0:
                    flag_issues(invalid_mask, f"Invalid number in {col}")
                    print(f"[VALIDATE] {count} rows with invalid number in '{col}' {'dropped' if mode=='drop' else 'flagged'}")

                min_val = rule.get("min")
                max_val = rule.get("max")
                if min_val is not None:
                    mask = result[col] < min_val
                    if mask.sum():
                        flag_issues(mask, f"{col} < {min_val}")
                        print(f"[VALIDATE] {mask.sum()} rows where {col} < {min_val} {'dropped' if mode=='drop' else 'flagged'}")
                if max_val is not None:
                    mask = result[col] > max_val
                    if mask.sum():
                        flag_issues(mask, f"{col} > {max_val}")
                        print(f"[VALIDATE] {mask.sum()} rows where {col} > {max_val} {'dropped' if mode=='drop' else 'flagged'}")
        
        elif rule_type == "DATE":
            col = find_column_robust(result, rule.get("column", ""))
            if col:
                was_not_null = df[col].notna() & (df[col].astype(str).str.strip() != "")
                
                fmt_str = rule.get("format")
                py_format = None
                if fmt_str:
                    if "format=" in fmt_str:
                        fmt_str = fmt_str.split("format=", 1)[1]
                    py_format = translate_format(fmt_str)

                locale = rule.get("locale")
                day_first = locale != "US"

                try:
                    if py_format:
                        result[col] = pd.to_datetime(result[col], format=py_format, errors="coerce")
                    else:
                        result[col] = pd.to_datetime(result[col], dayfirst=day_first, errors="coerce")
                except Exception:
                    pass

                invalid_mask = was_not_null & result[col].isna()
                count = invalid_mask.sum()
                if count > 0:
                    flag_issues(invalid_mask, f"Invalid date format in {col}")
                    print(f"[VALIDATE] {count} rows with invalid date in '{col}' {'dropped' if mode=='drop' else 'flagged'}")

        elif rule_type == "UNIQUE":
            col = find_column_robust(result, rule.get("column", ""))
            if col:
                dup_mask = result[col].duplicated(keep=False)
                count = dup_mask.sum()
                if count > 0:
                    flag_issues(dup_mask, f"Duplicate {col}")
                    print(f"[VALIDATE] {count} duplicate rows in '{col}' {'dropped' if mode=='drop' else 'flagged'}")

        elif rule_type == "REGEX":
            col = find_column_robust(result, rule.get("column", ""))
            pattern = rule.get("pattern", "")
            if col and pattern:
                try:
                    str_col = result[col].astype(str)
                    invalid_mask = ~str_col.str.match(pattern, na=False)
                except Exception as e:
                    print(f"[VALIDATE] Invalid regex pattern '{pattern}': {e}")
                    invalid_mask = pd.Series(True, index=result.index)

                count = invalid_mask.sum()
                if count > 0:
                    flag_issues(invalid_mask, f"Regex mismatch {col}")
                    print(f"[VALIDATE] {count} rows failed regex '{pattern}' on '{col}' {'dropped' if mode=='drop' else 'flagged'}")

        elif rule_type == "ENUM":
            col = find_column_robust(result, rule.get("column", ""))
            values = rule.get("values", [])
            if col and values:
                allowed = [v.strip().upper() for v in values]
                str_col = result[col].astype(str).str.upper()
                invalid_mask = ~str_col.isin(allowed) | result[col].isna()
                count = invalid_mask.sum()
                if count > 0:
                    flag_issues(invalid_mask, f"Invalid enum {col}")
                    print(f"[VALIDATE] {count} rows with invalid enum in '{col}' {'dropped' if mode=='drop' else 'flagged'}")

        elif rule_type == "OUTLIER":
            col = find_column_robust(result, rule.get("column", ""))
            if col:
                raw_series = result[col]
                if not pd.api.types.is_numeric_dtype(raw_series):
                    raw_series = pd.to_numeric(to_numeric_clean(raw_series), errors="coerce")
                
                non_null_vals = raw_series.dropna()
                
                if len(non_null_vals) >= 15:
                    method = rule.get("method", "iqr")
                    threshold = rule.get("threshold", 3.0)
                    outlier_mask = pd.Series(False, index=result.index)
                    
                    if method == "zscore":
                        mean = non_null_vals.mean()
                        std = non_null_vals.std()
                        if std > 0:
                            z_scores = (raw_series - mean).abs() / std
                            outlier_mask = z_scores > threshold
                    else: # iqr
                        q1 = non_null_vals.quantile(0.25)
                        q3 = non_null_vals.quantile(0.75)
                        iqr = q3 - q1
                        if iqr > 0:
                            lower_bound = q1 - 1.5 * iqr
                            upper_bound = q3 + 1.5 * iqr
                            outlier_mask = (raw_series < lower_bound) | (raw_series > upper_bound)
                    
                    outlier_mask = outlier_mask.fillna(False)
                    count = outlier_mask.sum()
                    if count > 0:
                        flag_issues(outlier_mask, f"Outlier {col}")
                        print(f"[VALIDATE] {count} rows flagged as outliers in '{col}' using {method.upper()} method")

    # Apply mode
    if mode == "drop":
        failed_df = result[drop_mask]
        result = result[~drop_mask]
        print(f"[VALIDATE] {drop_mask.sum()} total rows removed, {len(result)} remaining")
        if not failed_df.empty:
            write_dlq_failed_rows(failed_df, config, "Validation rules dropped rows")
    else:
        clean_issues = issues.str.rstrip("; ")
        result["_validation_issues"] = clean_issues.where(clean_issues != "", "PASS")
        passed = (result["_validation_issues"] == "PASS").sum()
        failed = len(result) - passed
        print(f"[VALIDATE] {passed} rows PASS, {failed} rows with issues (flagged in _validation_issues)")
    
    return result


def write_dlq_failed_rows(df_failed: pd.DataFrame, config: dict, reason: str = "Validation failure"):
    """Dead Letter Queue (DLQ): Write failed/dropped rows into _failed_rows table for audit & retry."""
    if df_failed.empty:
        return
    try:
        db_url = os.environ.get("DATABASE_URL")
        if not db_url:
            return
        import psycopg2
        conn = psycopg2.connect(db_url)
        conn.autocommit = True
        cur = conn.cursor()
        cur.execute("CREATE SCHEMA IF NOT EXISTS silver")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS silver._failed_rows (
                id SERIAL PRIMARY KEY,
                reason TEXT,
                failed_data TEXT,
                created_at TIMESTAMPTZ DEFAULT NOW()
            )
        """)
        # Sample first 50 failed rows as JSON for DLQ inspection
        sample_json = df_failed.head(50).to_json(orient="records")
        cur.execute(
            "INSERT INTO silver._failed_rows (reason, failed_data) VALUES (%s, %s)",
            (f"{reason} ({len(df_failed)} rows)", sample_json)
        )
        cur.close()
        conn.close()
        print(f"[DLQ] Logged {len(df_failed)} failed rows to silver._failed_rows")
    except Exception as e:
        print(f"[DLQ] Warning: Failed to write to DLQ: {e}")



def step_transform(df: pd.DataFrame, config: dict) -> pd.DataFrame:
    """Transform: calculated columns, rename, type cast."""
    result = df.copy()

    if config.get("calculatedColumns"):
        for col_name, expr in config["calculatedColumns"].items():
            try:
                result[col_name] = result.eval(expr)
                print(f"[TRANSFORM] Created calculated column '{col_name}' = {expr}")
            except Exception as e:
                print(f"[TRANSFORM] Failed to calculate '{col_name}': {e}")

    if config.get("rename"):
        result = result.rename(columns=config["rename"])
        print(f"[TRANSFORM] Renamed columns: {config['rename']}")

    if config.get("drop"):
        result = result.drop(columns=config["drop"], errors="ignore")
        print(f"[TRANSFORM] Dropped columns: {config['drop']}")

    return result


def step_filter(df: pd.DataFrame, config: dict) -> pd.DataFrame:
    """Filter rows using pandas query expression."""
    condition = config.get("condition", "")
    if not condition:
        return df

    rows_before = len(df)
    try:
        result = df.query(condition)
        removed = rows_before - len(result)
        if removed:
            print(f"[FILTER] Filtered out {removed} rows with condition: {condition}")
        return result
    except Exception as e:
        print(f"[FILTER] Query failed: {e}, returning original")
        return df


def step_categorize(df: pd.DataFrame, config: dict) -> pd.DataFrame:
    """Bucket numeric field into categories."""
    result = df.copy()
    field = config.get("field", "")
    new_col = config.get("newColumn", f"{field}_tier")
    categories = config.get("categories", [])

    if field not in result.columns:
        print(f"[CATEGORIZE] Field '{field}' not found")
        return result

    result[new_col] = "Unknown"
    for cat in categories:
        lo = cat.get("min", float("-inf"))
        hi = cat.get("max", float("inf"))
        label = cat.get("label", f"{lo}-{hi}")
        result.loc[(result[field] >= lo) & (result[field] < hi), new_col] = label

    print(f"[CATEGORIZE] Created '{new_col}' with {len(categories)} categories")
    return result


FUNC_MAP = {
    "SUM": "sum",
    "AVG": "mean",
    "AVERAGE": "mean",
    "COUNT": "count",
    "MIN": "min",
    "MAX": "max",
}

def _parse_aggregations(config: dict) -> dict:
    """
    Parse aggregations from config.  Supports two formats:

    Format A — dict (legacy / programmatic):
        {"total_amount": "Amount SUM", "tx_count": "* COUNT"}

    Format B — multi-line string (frontend):
        "total_amount = SUM(Amount)\\ntransaction_count = COUNT(*)\\navg_amount = AVG(Amount)"
    """
    raw = config.get("aggregations", {})

    if isinstance(raw, dict) and raw:
        return raw

    if isinstance(raw, str) and raw.strip():
        parsed = {}
        for line in raw.strip().split("\n"):
            line = line.strip()
            if not line or "=" not in line:
                continue
            out_col, expr = line.split("=", 1)
            out_col = out_col.strip()
            expr = expr.strip()
            # expr looks like "SUM(Amount)" or "COUNT(*)"
            if "(" in expr and expr.endswith(")"):
                func_name, col_part = expr[:-1].split("(", 1)
                func_name = func_name.strip().upper()
                inp_col = col_part.strip()
                # Map to internal format "inp_col FUNC" or "* FUNC"
                parsed[out_col] = f"{inp_col} {func_name}"
            else:
                parsed[out_col] = expr
        return parsed

    return {}


def step_aggregate(df: pd.DataFrame, config: dict) -> pd.DataFrame:
    """Aggregate: GROUP BY with aggregations."""
    group_by = config.get("groupBy", [])
    if isinstance(group_by, str):
        group_by = [g.strip() for g in group_by.split(",") if g.strip()]

    aggs = _parse_aggregations(config)

    if not aggs:
        return df

    # Build a temporary count column for COUNT(*)
    count_star_col = None
    agg_funcs: list[tuple] = []  # (pandas_func, column, output_name)

    for out_col, expr in aggs.items():
        parts = expr.split()
        if len(parts) < 2:
            continue
        inp_col = parts[0].strip('"')  # strip quotes from config
        func = parts[1].upper()
        pandas_func = FUNC_MAP.get(func)
        if not pandas_func:
            print(f"[AGGREGATE] Unknown function '{func}', skipping")
            continue

        if inp_col == "*" and func == "COUNT":
            # COUNT(*) — count rows per group
            if count_star_col is None:
                count_star_col = "_count_star_"
                # Use any column for counting; create a dedicated column
                df[count_star_col] = 1
            agg_funcs.append(("count_star", count_star_col, out_col))
        elif inp_col in df.columns:
            agg_funcs.append((pandas_func, inp_col, out_col))
        else:
            print(f"[AGGREGATE] Column '{inp_col}' not found, skipping")

    if not agg_funcs:
        return df

    # Build pandas agg dict
    pandas_agg = {}
    for func, col, out_col in agg_funcs:
        if func == "count_star":
            pandas_agg[out_col] = pd.NamedAgg(column=col, aggfunc="sum")
        else:
            pandas_agg[out_col] = pd.NamedAgg(column=col, aggfunc=func)

    if group_by:
        # Validate group-by columns
        valid_groups = [g for g in group_by if g in df.columns]
        if not valid_groups:
            print(f"[AGGREGATE] No valid group-by columns found in data")
            return df
        result = df.groupby(valid_groups, as_index=False).agg(**pandas_agg)
    else:
        result = df.agg(**pandas_agg)
        result = pd.DataFrame([result])

    # Drop the temporary count column
    if count_star_col and count_star_col in df.columns:
        df.drop(columns=[count_star_col], inplace=True)

    print(f"[AGGREGATE] Grouped by {group_by}, result: {len(result)} rows")
    return result


def step_sort(df: pd.DataFrame, config: dict) -> pd.DataFrame:
    """Sort by columns."""
    by = config.get("by", [])
    ascending = config.get("ascending", True)
    if by:
        result = df.sort_values(by=by, ascending=ascending)
        print(f"[SORT] Sorted by {by} {'ASC' if ascending else 'DESC'}")
        return result
    return df


def step_join(df: pd.DataFrame, config: dict) -> pd.DataFrame:
    """Join with another table (from DB)."""
    join_type = config.get("type", "left")
    join_key = config.get("on", df.columns[0])
    join_source = config.get("source", "")

    if not join_source:
        return df

    engine = get_engine()
    table_name = join_source.replace("silver.", "").replace("bronze.", "").replace("gold.", "")
    other = pd.read_sql_table(table_name, engine)

    result = df.merge(other, on=join_key, how=join_type)
    print(f"[JOIN] {join_type} join with {join_source} on '{join_key}': {len(result)} rows")
    return result


# ============================================================
# Database helpers
# ============================================================

def ensure_layer_schema(engine, layer: str):
    """Create schema if not exists."""
    schema = sanitize_layer(layer)
    with engine.connect() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema}"))
        conn.commit()
    print(f"[DB] Ensured schema '{schema}' exists")


def write_output(df: pd.DataFrame, config: dict, pipeline_context: dict | None = None):
    """Write DataFrame to PostgreSQL lakehouse layer using psycopg2 directly.
    
    Improvements:
    - Transactional: writes to temp table, then renames/merges atomically
    - Backup: for 'overwrite', renames old table to {table}__bak_{timestamp}
    - SQL injection safe: all identifiers sanitized
    - Data lineage: auto-adds _etl_timestamp and _pipeline_run_id columns
    - Incremental Load: supports 'overwrite', 'append', and 'upsert' modes
    """
    import psycopg2
    import psycopg2.extras as extras
    import math

    # Extract layer and table name with robust fallbacks
    step_cfg = config.get("config", {}) if isinstance(config.get("config"), dict) else {}
    layer = sanitize_layer(
        config.get("outputLayer") or step_cfg.get("outputLayer") or step_cfg.get("layer") or "SILVER"
    )
    table = sanitize_identifier(
        config.get("outputTable") or step_cfg.get("outputTable") or step_cfg.get("tableName") or "output"
    ).lower()
    
    write_mode = (config.get("writeMode") or step_cfg.get("writeMode") or "overwrite").lower().strip()
    primary_key_raw = config.get("primaryKey") or step_cfg.get("primaryKey") or ""
    
    db_url = clean_db_url(os.environ.get("DATABASE_URL", ""))
    if not db_url:
        raise RuntimeError("DATABASE_URL not set")

    # Add data lineage columns
    df_out = df.copy()
    now_ts = datetime.now(timezone.utc)
    df_out["_etl_timestamp"] = now_ts
    if pipeline_context:
        df_out["_pipeline_run_id"] = pipeline_context.get("run_id", 0)
        df_out["_source_pipeline_id"] = pipeline_context.get("pipeline_id", 0)

    # Sanitize and parse primary key columns
    pk_cols = []
    if primary_key_raw:
        if isinstance(primary_key_raw, list):
            pk_cols = [sanitize_identifier(str(k)).lower() for k in primary_key_raw if k]
        else:
            pk_cols = [sanitize_identifier(k.strip()).lower() for k in str(primary_key_raw).split(",") if k.strip()]

    conn = psycopg2.connect(db_url)
    try:
        # Use autocommit only for schema creation
        conn.autocommit = True
        cur = conn.cursor()
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {layer}")
        cur.close()

        # Switch to transactional mode for the actual data write
        conn.autocommit = False
        cur = conn.cursor()

        full_table = f'{layer}."{table}"'
        run_id = pipeline_context.get("run_id", 0) if pipeline_context else 0
        temp_table_name = f"{table}__tmp_{int(now_ts.timestamp())}_{run_id}"
        temp_full_table = f'{layer}."{temp_table_name}"'
        backup_table_name = f"{table}__bak_{now_ts.strftime('%Y%m%d%H%M%S')}_{run_id}"
        backup_full_table = f'{layer}."{backup_table_name}"'

        # Check if original table exists
        cur.execute(
            "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = %s AND table_name = %s)",
            (layer, table)
        )
        table_exists = cur.fetchone()[0]

        # Determine column types from pandas dtypes with schema hardening overrides
        col_defs = []
        col_type_map = {}
        for col in df_out.columns:
            dtype = str(df_out[col].dtype)
            col_lower = str(col).lower()
            
            if "int" in dtype:
                pg_type = "BIGINT"
            elif "float" in dtype:
                pg_type = "DOUBLE PRECISION"
            elif "datetime" in dtype:
                pg_type = "TIMESTAMP"
            else:
                # Schema hardening: force TEXT/object columns to correct PostgreSQL type if their names match patterns
                if any(x in col_lower for x in ["debit", "debet", "credit", "kredit", "balance", "saldo", "amount", "total", "nominal", "gaji", "price", "harga"]):
                    pg_type = "DOUBLE PRECISION"
                    # Clean and convert the column values in df_out to numeric safely
                    raw_cleaned = to_numeric_clean(df_out[col])
                    df_out[col] = pd.to_numeric(raw_cleaned, errors="coerce")
                elif any(x in col_lower for x in ["date", "time", "timestamp", "tanggal"]):
                    pg_type = "TIMESTAMP"
                    # Clean and convert the column values in df_out to datetime safely
                    def parse_date_element(x):
                        if pd.isna(x) or str(x).strip() == "" or str(x).strip().lower() == "null":
                            return pd.NaT
                        try:
                            return pd.to_datetime(x, dayfirst=True)
                        except Exception:
                            return pd.NaT
                    df_out[col] = df_out[col].apply(parse_date_element)
                else:
                    pg_type = "TEXT"
            
            sanitized_col_name = sanitize_identifier(str(col))
            col_type_map[sanitized_col_name] = pg_type
            col_defs.append(f'"{sanitized_col_name}" {pg_type}')

        # Perform Schema Evolution for append/upsert if original table exists
        if table_exists and write_mode != "overwrite":
            cur.execute(
                "SELECT column_name FROM information_schema.columns WHERE table_schema = %s AND table_name = %s",
                (layer, table)
            )
            existing_cols = {row[0].lower() for row in cur.fetchall()}
            for col_name_sanitized, pg_type in col_type_map.items():
                if col_name_sanitized.lower() not in existing_cols:
                    alter_sql = f'ALTER TABLE {full_table} ADD COLUMN "{col_name_sanitized}" {pg_type}'
                    print(f"[SCHEMA-EVOLUTION] Adding missing column '{col_name_sanitized}' ({pg_type}) to {full_table}")
                    cur.execute(alter_sql)

        # 1. Create target/temp table if needed
        if write_mode == "overwrite" or not table_exists:
            # For overwrite or new table, create a clean table
            cur.execute(f"DROP TABLE IF EXISTS {temp_full_table}")
            
            # If upsert mode on a new table, define the Primary Key constraint
            if write_mode == "upsert" and pk_cols:
                # Filter out any primary key columns that don't exist in the data
                valid_pk_cols = [f'"{k}"' for k in pk_cols if k in [c.lower() for c in df_out.columns]]
                if valid_pk_cols:
                    create_sql = f"CREATE TABLE {temp_full_table} ({', '.join(col_defs)}, PRIMARY KEY ({', '.join(valid_pk_cols)}))"
                else:
                    create_sql = f"CREATE TABLE {temp_full_table} ({', '.join(col_defs)})"
            else:
                create_sql = f"CREATE TABLE {temp_full_table} ({', '.join(col_defs)})"
                
            cur.execute(create_sql)
        else:
            # Table exists and we are in append/upsert mode: create temp staging table without constraints
            cur.execute(f"DROP TABLE IF EXISTS {temp_full_table}")
            create_sql = f"CREATE TABLE {temp_full_table} ({', '.join(col_defs)})"
            cur.execute(create_sql)


        # 2. Insert data into temp table using COPY (10-50x faster than execute_batch)
        columns = [f'"{sanitize_identifier(str(c))}"' for c in df_out.columns]

        try:
            import io
            buffer = io.StringIO()

            # Prepare data: convert timestamps, handle NaN/None
            df_copy = df_out.copy()
            for col in df_copy.columns:
                dtype = str(df_copy[col].dtype)
                if "datetime" in dtype:
                    df_copy[col] = df_copy[col].apply(
                        lambda x: x.isoformat() if pd.notna(x) else None
                    )
                elif "float" in dtype or "int" in dtype:
                    df_copy[col] = df_copy[col].where(pd.notna(df_copy[col]), None)

            df_copy.to_csv(buffer, index=False, header=False, sep='\t', na_rep='\\N')
            buffer.seek(0)

            copy_sql = f"COPY {temp_full_table} ({', '.join(columns)}) FROM STDIN WITH (FORMAT csv, DELIMITER E'\\t', NULL '\\N')"
            cur.copy_expert(copy_sql, buffer)
            print(f"[OUTPUT] Bulk COPY completed for {len(df_out)} rows")
        except Exception as copy_err:
            print(f"[OUTPUT] COPY failed ({copy_err}), falling back to execute_batch")
            # Fallback to execute_batch
            def sanitize_val(val):
                if pd.isna(val):
                    return None
                if isinstance(val, float) and math.isnan(val):
                    return None
                if isinstance(val, pd.Timestamp):
                    return val.to_pydatetime()
                return val

            placeholders = ",".join(["%s"] * len(columns))
            insert_sql = f"INSERT INTO {temp_full_table} ({', '.join(columns)}) VALUES ({placeholders})"
            rows = []
            for row in df_out.itertuples(index=False, name=None):
                rows.append(tuple(sanitize_val(x) for x in row))
            extras.execute_batch(cur, insert_sql, rows, page_size=2000)

        # 3. Apply changes from temp table to target table based on write_mode
        if write_mode == "overwrite":
            if table_exists:
                # Rename old table to backup
                cur.execute(f"ALTER TABLE {full_table} RENAME TO \"{backup_table_name}\"")
                print(f"[OUTPUT] Backed up existing table to {backup_full_table}")

            # Rename temp table to final
            cur.execute(f"ALTER TABLE {temp_full_table} RENAME TO \"{table}\"")

            # Cleanup old backups (keep last 3, drop anything older)
            cur.execute(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = %s AND table_name LIKE %s "
                "ORDER BY table_name DESC",
                (layer, f"{table}__bak_%")
            )
            old_backups = [row[0] for row in cur.fetchall()]
            for old_bak in old_backups[3:]:
                try:
                    cur.execute(f'DROP TABLE IF EXISTS {layer}."{sanitize_identifier(old_bak)}"')
                    print(f"[OUTPUT] Cleaned up old backup: {old_bak}")
                except Exception:
                    pass  # Non-critical

        elif write_mode == "append":
            if not table_exists:
                # Temp table is the new table, just rename it
                cur.execute(f"ALTER TABLE {temp_full_table} RENAME TO \"{table}\"")
            else:
                # Append rows from temp table to target table
                cols_str = ", ".join(columns)
                cur.execute(f"INSERT INTO {full_table} ({cols_str}) SELECT {cols_str} FROM {temp_full_table}")
                cur.execute(f"DROP TABLE IF EXISTS {temp_full_table}")

        elif write_mode == "upsert":
            if not table_exists:
                # Temp table has the PK constraint and is the new table, just rename it
                cur.execute(f"ALTER TABLE {temp_full_table} RENAME TO \"{table}\"")
            else:
                # Upsert using ON CONFLICT DO UPDATE
                # Find matching primary key columns in the target table columns
                valid_pk_cols = [k for k in pk_cols if k in [c.lower() for c in df_out.columns]]
                
                if not valid_pk_cols:
                    # Fallback to append if no primary key columns match
                    print("[OUTPUT] WARNING: Upsert requested but no valid primary key columns found. Falling back to append.")
                    cols_str = ", ".join(columns)
                    cur.execute(f"INSERT INTO {full_table} ({cols_str}) SELECT {cols_str} FROM {temp_full_table}")
                else:
                    # Construct ON CONFLICT clause
                    conflict_target = ", ".join([f'"{k}"' for k in valid_pk_cols])
                    
                    # Update all columns except the primary keys
                    update_cols = [c for c in df_out.columns if sanitize_identifier(str(c)).lower() not in valid_pk_cols]
                    update_clause = ", ".join([f'"{sanitize_identifier(str(c))}" = EXCLUDED."{sanitize_identifier(str(c))}"' for c in update_cols])
                    
                    cols_str = ", ".join(columns)
                    if update_clause:
                        upsert_sql = f"""
                            INSERT INTO {full_table} ({cols_str}) 
                            SELECT {cols_str} FROM {temp_full_table}
                            ON CONFLICT ({conflict_target}) 
                            DO UPDATE SET {update_clause}
                        """
                    else:
                        # Nothing to update if all columns are primary keys
                        upsert_sql = f"""
                            INSERT INTO {full_table} ({cols_str}) 
                            SELECT {cols_str} FROM {temp_full_table}
                            ON CONFLICT ({conflict_target}) 
                            DO NOTHING
                        """
                    cur.execute(upsert_sql)
                
                cur.execute(f"DROP TABLE IF EXISTS {temp_full_table}")

        conn.commit()
        cur.close()
        print(f"[OUTPUT] Wrote {len(df)} rows to {full_table} (mode={write_mode})")
        return len(df)

    except Exception as e:
        conn.rollback()
        print(f"[OUTPUT] ERROR: Transaction rolled back: {e}")
        raise
    finally:
        conn.close()


def ingest_csv_to_bronze(file_path: str, source_id: int, file_size: int | None = None) -> tuple[str, int, list[dict]]:
    """Ingest a CSV/Excel file into the Bronze layer.
    
    This ensures all data sources go through Bronze first, maintaining
    lakehouse architecture consistency.
    
    Returns: (bronze_table_name, row_count, column_metadata)
    """
    import psycopg2
    import psycopg2.extras as extras

    source = {"filePath": file_path, "fileSize": file_size}
    df = load_source(source)
    
    if df.empty:
        print("[BRONZE] No data to ingest")
        return "", 0, []

    # Generate bronze table name
    table_name = f"csv_{source_id}"
    
    db_url = clean_db_url(os.environ.get("DATABASE_URL", ""))
    if not db_url:
        raise RuntimeError("DATABASE_URL not set")

    conn = psycopg2.connect(db_url)
    try:
        conn.autocommit = True
        cur = conn.cursor()
        cur.execute("CREATE SCHEMA IF NOT EXISTS bronze")

        full_table = f'bronze."{table_name}"'
        
        # All columns stored as TEXT in bronze (raw layer)
        col_defs = [f'"{sanitize_identifier(str(col))}" TEXT' for col in df.columns]
        col_defs.append('"_ingested_at" TIMESTAMPTZ DEFAULT NOW()')
        col_defs.append(f'"_source_id" INTEGER DEFAULT {int(source_id)}')
        
        # Truncate if exists, create if not
        cur.execute(f"DROP TABLE IF EXISTS {full_table}")
        cur.execute(f"CREATE TABLE {full_table} ({', '.join(col_defs)})")

        # Insert all rows as text using COPY (fast bulk load)
        columns = [f'"{sanitize_identifier(str(c))}"' for c in df.columns]
        try:
            import io
            buffer = io.StringIO()
            df_str = df.astype(str).where(pd.notna(df), None)
            df_str.to_csv(buffer, index=False, header=False, sep='\t', na_rep='\\N')
            buffer.seek(0)

            copy_sql = f"COPY {full_table} ({', '.join(columns)}) FROM STDIN WITH (FORMAT csv, DELIMITER E'\\t', NULL '\\N')"
            cur.copy_expert(copy_sql, buffer)
        except Exception as copy_err:
            print(f"[BRONZE] COPY failed ({copy_err}), falling back to execute_batch")
            placeholders = ",".join(["%s"] * len(columns))
            insert_sql = f"INSERT INTO {full_table} ({', '.join(columns)}) VALUES ({placeholders})"
            rows = []
            for row in df.itertuples(index=False, name=None):
                rows.append(tuple(str(v) if pd.notna(v) else None for v in row))
            extras.execute_batch(cur, insert_sql, rows, page_size=2000)
        cur.close()
    finally:
        conn.close()

    column_metadata = [{"name": str(col), "type": "TEXT"} for col in df.columns]
    print(f"[BRONZE] Ingested {len(df)} rows to bronze.\"{table_name}\"")
    return table_name, len(df), column_metadata


# ============================================================
# Pipeline runner
# ============================================================

def step_source(_df: None, config: dict, source_data: dict) -> pd.DataFrame:
    """Load source data — from lakehouse table or CSV file."""
    # Lakehouse source (from existing bronze/silver/gold table)
    if source_data.get("fromLakehouse") or source_data.get("sourceTable"):
        source_config = {
            "sourceTable": source_data.get("sourceTable") or config.get("sourceTable", ""),
            "sourceLayer": source_data.get("sourceLayer") or config.get("sourceLayer", "BRONZE"),
        }
        return load_lakehouse_source(source_config)

    # CSV file source
    file_path = config.get("filePath") or source_data.get("filePath")
    if not file_path:
        raise ValueError("No filePath in source config and no lakehouse sourceTable")

    return load_source({"filePath": file_path, "fileSize": config.get("fileSize")})


def compute_data_quality(df: pd.DataFrame) -> dict:
    """Compute Data Quality Trust Score and detailed metrics for a DataFrame."""
    if df.empty:
        return {
            "score": 100.0,
            "details": {
                "completeness": 100.0,
                "uniqueness": 100.0,
                "consistency": 100.0,
                "freshness": 100.0,
                "accuracy": 100.0
            }
        }
    
    total_cells = df.size
    non_null_cells = df.notna().sum().sum()
    completeness = round(float((non_null_cells / total_cells) * 100.0), 2) if total_cells > 0 else 100.0
    
    total_rows = len(df)
    unique_rows = len(df.drop_duplicates())
    uniqueness = round(float((unique_rows / total_rows) * 100.0), 2) if total_rows > 0 else 100.0
    
    consistency = 100.0
    
    num_cols = df.select_dtypes(include=['number']).columns
    if len(num_cols) > 0:
        outlier_rows = 0
        for col in num_cols:
            s = df[col].dropna()
            if len(s) > 3:
                q1 = s.quantile(0.25)
                q3 = s.quantile(0.75)
                iqr = q3 - q1
                if iqr > 0:
                    outliers = ((s < (q1 - 1.5 * iqr)) | (s > (q3 + 1.5 * iqr))).sum()
                    outlier_rows += outliers
        accuracy = max(0.0, round(float(100.0 - (outlier_rows / (total_rows * len(num_cols))) * 100.0), 2))
    else:
        accuracy = 100.0

    freshness = 100.0

    weighted_score = round(
        completeness * 0.30 +
        uniqueness * 0.20 +
        consistency * 0.20 +
        freshness * 0.15 +
        accuracy * 0.15,
        1
    )

    return {
        "score": weighted_score,
        "details": {
            "completeness": completeness,
            "uniqueness": uniqueness,
            "consistency": consistency,
            "freshness": freshness,
            "accuracy": accuracy
        }
    }


def step_insight(df: pd.DataFrame, config: dict) -> pd.DataFrame:
    """Auto-Insight Engine — detects trends, correlations, outliers, and generates narrative in Indonesian."""
    narratives = []
    num_cols = df.select_dtypes(include=['number']).columns.tolist()
    cat_cols = df.select_dtypes(include=['object', 'string', 'category']).columns.tolist()

    narratives.append(f"📊 Dataset memiliki total {len(df):,} baris dan {len(df.columns)} kolom.")

    for col in num_cols[:3]:
        s = df[col].dropna()
        if len(s) > 0:
            avg_val = s.mean()
            max_val = s.max()
            min_val = s.min()
            narratives.append(f"💡 Rata-rata **{col}** adalah {avg_val:,.2f} (Maks: {max_val:,.2f}, Min: {min_val:,.2f}).")

    for col in num_cols[:2]:
        s = df[col].dropna()
        if len(s) > 5:
            q1 = s.quantile(0.25)
            q3 = s.quantile(0.75)
            iqr = q3 - q1
            if iqr > 0:
                outliers = s[(s < (q1 - 1.5 * iqr)) | (s > (q3 + 1.5 * iqr))]
                if len(outliers) > 0:
                    narratives.append(f"⚠️ Ditemukan **{len(outliers)} anomali/outlier** pada kolom **{col}** (Ekstrem maks: {outliers.max():,.2f}).")

    if len(num_cols) >= 2:
        corr_matrix = df[num_cols].corr()
        for i in range(len(num_cols)):
            for j in range(i + 1, len(num_cols)):
                c1, c2 = num_cols[i], num_cols[j]
                val = corr_matrix.loc[c1, c2]
                if abs(val) >= 0.70:
                    rel_type = "sangat kuat" if abs(val) >= 0.85 else "cukup kuat"
                    narratives.append(f"🔗 Hubungan **{rel_type}** (r={val:.2f}) antara **{c1}** dan **{c2}**.")

    for col in cat_cols[:2]:
        top_val = df[col].mode()
        if not top_val.empty:
            cnt = (df[col] == top_val[0]).sum()
            pct = (cnt / len(df)) * 100
            narratives.append(f"🏆 Kategori terbanyak pada **{col}** adalah '{top_val[0]}' ({cnt:,} baris / {pct:.1f}%).")

    print("[INSIGHT ENGINE] Generated narrative:")
    for n in narratives:
        try:
            print("  -", n)
        except UnicodeEncodeError:
            print("  -", n.encode('ascii', errors='ignore').decode('ascii'))

    df.attrs["insights"] = narratives
    return df


def step_anomaly_detect(df: pd.DataFrame, config: dict) -> pd.DataFrame:
    """Anomaly Detection Module — Isolation Forest or Z-Score based."""
    target_cols = config.get("columns", [])
    if not target_cols:
        target_cols = df.select_dtypes(include=['number']).columns.tolist()
    else:
        target_cols = [col for col in target_cols if col in df.columns]

    if not target_cols:
        print("[ANOMALY_DETECT] No numeric columns available for anomaly detection")
        df["_anomaly_score"] = 0.0
        df["_anomaly_label"] = "NORMAL"
        return df

    result_df = df.copy()

    try:
        from sklearn.ensemble import IsolationForest
        X = result_df[target_cols].fillna(result_df[target_cols].median())
        clf = IsolationForest(contamination=0.05, random_state=42)
        preds = clf.fit_predict(X)
        scores = -clf.decision_function(X)
        
        min_s, max_s = scores.min(), scores.max()
        norm_scores = (scores - min_s) / (max_s - min_s + 1e-6) if max_s > min_s else scores * 0.0
        
        result_df["_anomaly_score"] = norm_scores.round(4)
        result_df["_anomaly_label"] = ["ANOMALY" if p == -1 else "NORMAL" for p in preds]
        print(f"[ANOMALY_DETECT] IsolationForest completed. Found {(preds == -1).sum()} anomalies.")
        return result_df
    except Exception as e:
        print(f"[ANOMALY_DETECT] Fallback to Z-Score method ({e})")
        scores = pd.Series(0.0, index=result_df.index)
        for col in target_cols:
            s = result_df[col].fillna(result_df[col].median())
            std = s.std()
            if std > 0:
                z = ((s - s.mean()).abs()) / std
                scores = scores.clip(lower=z)
        
        norm_scores = (scores / 3.0).clip(0.0, 1.0)
        result_df["_anomaly_score"] = norm_scores.round(4)
        result_df["_anomaly_label"] = ["ANOMALY" if s > 0.8 else ("SUSPICIOUS" if s > 0.5 else "NORMAL") for s in norm_scores]
        return result_df


def step_forecast(df: pd.DataFrame, config: dict) -> pd.DataFrame:
    """Predictive Forecasting Engine — Time-series forecast into future periods."""
    date_col = config.get("dateColumn")
    target_col = config.get("targetColumn")
    horizon = int(config.get("horizon", 30))

    if not date_col or date_col not in df.columns:
        date_cols = df.select_dtypes(include=['datetime', 'datetime64']).columns
        date_col = date_cols[0] if len(date_cols) > 0 else None

    if not target_col or target_col not in df.columns:
        num_cols = df.select_dtypes(include=['number']).columns
        target_col = num_cols[0] if len(num_cols) > 0 else None

    if not date_col or not target_col:
        print("[FORECAST] Missing required dateColumn or targetColumn. Skipping forecast.")
        return df

    result_df = df.copy()
    result_df[date_col] = pd.to_datetime(result_df[date_col], errors='coerce')
    ts_df = result_df.dropna(subset=[date_col, target_col]).sort_values(by=date_col)

    if len(ts_df) < 3:
        print("[FORECAST] Insufficient data points for forecast.")
        return df

    try:
        import importlib
        prophet_mod = importlib.import_module("prophet")
        Prophet = getattr(prophet_mod, "Prophet")
        p_df = pd.DataFrame({"ds": ts_df[date_col], "y": ts_df[target_col]})
        m = Prophet(yearly_seasonality=False, weekly_seasonality=True, daily_seasonality=False)
        m.fit(p_df)
        future = m.make_future_dataframe(periods=horizon)
        forecast = m.predict(future)
        
        forecast_df = forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper']].rename(columns={
            'ds': date_col,
            'yhat': f"{target_col}_forecast",
            'yhat_lower': f"{target_col}_lower",
            'yhat_upper': f"{target_col}_upper",
        })
        print(f"[FORECAST] Prophet forecast completed for {horizon} horizon steps.")
        return forecast_df
    except Exception as e:
        print(f"[FORECAST] Fallback linear trend forecasting ({e})")
        import numpy as np
        y = ts_df[target_col].values
        x = np.arange(len(y))
        poly = np.polyfit(x, y, 1)
        
        last_date = ts_df[date_col].max()
        future_dates = [last_date + pd.Timedelta(days=i+1) for i in range(horizon)]
        future_x = np.arange(len(y), len(y) + horizon)
        pred_y = np.polyval(poly, future_x)
        
        std_err = np.std(y - np.polyval(poly, x)) if len(y) > 1 else 0.0
        
        forecast_df = pd.DataFrame({
            date_col: future_dates,
            f"{target_col}_forecast": np.round(pred_y, 2),
            f"{target_col}_lower": np.round(pred_y - 1.96 * std_err, 2),
            f"{target_col}_upper": np.round(pred_y + 1.96 * std_err, 2),
        })
        print(f"[FORECAST] Linear forecast completed for {horizon} future days.")
        return pd.concat([result_df, forecast_df], ignore_index=True)


def step_classify(df: pd.DataFrame, config: dict) -> pd.DataFrame:
    """Smart Data Classifier — Auto-label text data (Sentiment / Category)."""
    text_col = config.get("textColumn")
    mode = config.get("mode", "sentiment")
    output_col = config.get("outputColumn", f"{mode}_label")

    if not text_col or text_col not in df.columns:
        print(f"[CLASSIFY] Column '{text_col}' not found. Skipping classification.")
        return df

    result_df = df.copy()

    def get_sentiment(text_val):
        if pd.isna(text_val):
            return "NETRAL"
        text_str = str(text_val).lower()
        pos_words = ["bagus", "baik", "mantap", "puas", "hebat", "sukses", "keren", "top", "good", "great", "excellent", "profit", "naik"]
        neg_words = ["buruk", "jelek", "kecewa", "rugi", "turun", "gagal", "rusak", "error", "bad", "poor", "fail", "loss", "drop"]
        
        pos_count = sum(w in text_str for w in pos_words)
        neg_count = sum(w in text_str for w in neg_words)

        if pos_count > neg_count:
            return "POSITIF"
        elif neg_count > pos_count:
            return "NEGATIF"
        return "NETRAL"

    result_df[output_col] = result_df[text_col].apply(get_sentiment)
    print(f"[CLASSIFY] Text classification ({mode}) completed on column '{text_col}'.")
    return result_df


STEP_HANDLERS = {
    "CLEAN":          step_clean,
    "VALIDATE":       step_validate,
    "TRANSFORM":      step_transform,
    "FILTER":         step_filter,
    "CATEGORIZE":     step_categorize,
    "AGGREGATE":      step_aggregate,
    "SORT":           step_sort,
    "JOIN":           step_join,
    "INSIGHT":        step_insight,
    "ANOMALY_DETECT": step_anomaly_detect,
    "FORECAST":       step_forecast,
    "CLASSIFY":       step_classify,
}



def dtype_to_sql(dtype) -> str:
    """Convert pandas dtype to SQL type name."""
    d = str(dtype)
    if "int" in d:
        return "INTEGER"
    elif "float" in d:
        return "DECIMAL"
    elif "datetime" in d:
        return "TIMESTAMP"
    elif "bool" in d:
        return "BOOLEAN"
    else:
        return "VARCHAR"


def run_pipeline(config_path: str) -> dict:
    """Execute pipeline steps sequentially.
    
    Improvements:
    - Step-level error handling with detailed context
    - Pipeline context passed for data lineage tracking
    - WS error reporting per step
    """
    with open(config_path) as f:
        pipeline = json.load(f)

    run_id = pipeline.get("runId", 0)
    pipeline_id = pipeline.get("pipelineId", 0)

    # Pipeline context for data lineage
    pipeline_context = {
        "run_id": run_id,
        "pipeline_id": pipeline_id,
    }

    # Import WS reporter lazily (optional)
    try:
        from ws_reporter import report as ws_report
    except ImportError:
        ws_report = None

    print(f"=== Gaung ETL Worker ===")
    print(f"Pipeline: {pipeline_id}, Run: {run_id}")

    source_data = pipeline.get("source", {})
    steps = sorted(pipeline.get("steps", []), key=lambda s: s["order"])

    df: pd.DataFrame | None = None
    rows_output = 0
    column_metadata: list[dict] = []
    outputs: list[dict] = []  # metadata for each OUTPUT step
    step_errors: list[dict] = []  # track per-step errors

    total_steps = len(steps)

    for i, step in enumerate(steps):
        step_type = step.get("type", "UNKNOWN")
        config = step.get("config", {})

        print(f"\n--- Step {i+1}/{total_steps}: {step_type} ---")

        # Report progress via WebSocket
        if ws_report:
            try:
                ws_report(run_id, "step_start", {
                    "step": i + 1,
                    "total": total_steps,
                    "type": step_type,
                    "progress": round((i / total_steps) * 100),
                })
            except Exception:
                pass

        try:
            if step_type == "SOURCE":
                df = step_source(None, config, source_data)

            elif step_type == "OUTPUT":
                if df is None:
                    print("[OUTPUT] No data to write, skipping")
                    continue
                # Collect column metadata BEFORE lineage columns are added
                cols = [{"name": str(col), "type": dtype_to_sql(df[col].dtype)} for col in df.columns]
                layer = sanitize_layer(step.get("outputLayer") or config.get("outputLayer") or "SILVER")
                table = sanitize_identifier(
                    step.get("outputTable") or config.get("outputTable") or "output"
                ).lower()
                nrows = write_output(df, step, pipeline_context)
                rows_output = nrows
                column_metadata = cols
                outputs.append({
                    "layer": layer,
                    "table": table,
                    "rows": nrows,
                    "columns": cols,
                })
                # Don't set df = None — multiple OUTPUT steps can share the pipeline,
                # and subsequent steps (AGGREGATE→OUTPUT) need the data.

            else:
                if df is None:
                    print(f"[{step_type}] No data in pipeline, skipping")
                    continue
                handler = STEP_HANDLERS.get(step_type)
                if handler:
                    if step_type == "VALIDATE":
                        config = config.copy()
                        config["_source_category"] = source_data.get("category")
                    df = handler(df, config)
                else:
                    print(f"[{step_type}] Unknown step type, skipping")

        except Exception as e:
            error_msg = f"Step {i+1}/{total_steps} ({step_type}) failed: {e}"
            print(f"[ERROR] {error_msg}")
            step_errors.append({
                "step": i + 1,
                "type": step_type,
                "error": str(e),
            })
            # Report error via WebSocket
            if ws_report:
                try:
                    ws_report(run_id, "step_error", {
                        "step": i + 1,
                        "total": total_steps,
                        "type": step_type,
                        "error": str(e),
                    })
                except Exception:
                    pass
            # For SOURCE and OUTPUT errors, abort the pipeline
            if step_type in ("SOURCE", "OUTPUT"):
                raise RuntimeError(error_msg) from e
            # For transformation step errors, log and continue with previous df
            print(f"[{step_type}] Continuing pipeline with data from previous step...")

    # Report completion via WebSocket
    if ws_report:
        try:
            ws_report(run_id, "complete", {
                "rows": rows_output,
                "outputs": outputs,
                "progress": 100,
                "errors": step_errors,
            })
        except Exception:
            pass

    result = {"rows": rows_output, "columns": column_metadata, "outputs": outputs}
    if step_errors:
        result["step_errors"] = step_errors
    print(f"\n=== Pipeline Complete === rows={rows_output}" + 
          (f" (with {len(step_errors)} step error(s))" if step_errors else ""))
    return result


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python3 etl_runner.py <config.json>")
        sys.exit(1)

    config_path = sys.argv[1]
    result = run_pipeline(config_path)
    print(json.dumps(result))
