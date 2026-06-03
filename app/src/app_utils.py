import re
from pathlib import Path
import pandas as pd


def count_connections(data_dir: Path, iso3_to_iso2: dict, csv_files=None) -> dict:
    """
    Scan CSVs in data_dir and count occurrences of country codes.
    - Accepts ISO2 codes directly
    - Converts ISO3 -> ISO2 where possible
    """
    counts = {}
    csv_files = csv_files or ["Address.csv", "Entity.csv", "Intermediary.csv", "Officer.csv"]

    for fname in csv_files:
        filepath = data_dir / fname
        if not filepath.exists():
            print(f"Warning: {filepath} not found, skipping")
            continue

        df = pd.read_csv(filepath, low_memory=False)

        if "country_codes" in df.columns:
            col = "country_codes"
        elif "countries" in df.columns:
            col = "countries"
        else:
            continue

        for codes in df[col].dropna():
            if not isinstance(codes, str):
                continue

            for code in re.split(r"[;,]\s*", codes.strip()):
                code = code.strip().upper()

                # Convert ISO3 → ISO2 if possible
                if len(code) == 3 and code in iso3_to_iso2:
                    code = iso3_to_iso2[code]

                # Only count if it's a 2-letter ISO2 code
                if len(code) == 2:
                    counts[code] = counts.get(code, 0) + 1

    return counts


def clamp_arg(args, name: str, default: int, lo: int, hi: int) -> int:
    """
    Clamp an integer query parameter from a Flask request.args-like mapping.
    """
    try:
        return max(lo, min(hi, int(args.get(name, default))))
    except (TypeError, ValueError):
        return default