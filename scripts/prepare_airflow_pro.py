"""Create private local Airflow secrets once; never rotate existing credentials."""

from __future__ import annotations

import argparse
import base64
import os
import secrets
from pathlib import Path

KEYS = {
    "AIRFLOW_PRO_DB_PASSWORD",
    "AIRFLOW_PRO_JWT_SECRET",
    "AIRFLOW_PRO_API_SECRET",
    "AIRFLOW_PRO_FERNET_KEY",
}


def prepare(root: Path) -> Path:
    csv = root / "data/raw/datatran2024.csv"
    if not csv.is_file():
        raise FileNotFoundError(f"Run PRF batch ingestion first; missing {csv}")
    path = root / ".env.airflow-pro"
    if path.is_symlink():
        raise ValueError("Refusing a symlink for the secrets file")
    if path.exists():
        values = dict(
            line.split("=", 1)
            for line in path.read_text().splitlines()
            if line.strip() and not line.lstrip().startswith("#")
        )
        if not KEYS.issubset(values) or any(not values[key] for key in KEYS):
            raise ValueError("Existing Airflow secrets file is incomplete; no changes made")
        if any(
            not all(c in "0123456789abcdef" for c in values[key])
            for key in KEYS
            if key != "AIRFLOW_PRO_FERNET_KEY"
        ):
            raise ValueError("Existing secrets are not the generated URL-safe hex values")
        if len(base64.urlsafe_b64decode(values["AIRFLOW_PRO_FERNET_KEY"])) != 32:
            raise ValueError("Invalid existing Fernet key")
        print("Airflow secrets already exist; credentials preserved.")
        return path
    values = {key: secrets.token_hex(32) for key in sorted(KEYS)}
    values["AIRFLOW_PRO_FERNET_KEY"] = base64.urlsafe_b64encode(secrets.token_bytes(32)).decode()
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, "w") as file:
        file.write("".join(f"{key}={value}\n" for key, value in values.items()))
    print("Created private .env.airflow-pro; no secret values printed.")
    return path


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[1])
    args = parser.parse_args()
    prepare(args.root)
