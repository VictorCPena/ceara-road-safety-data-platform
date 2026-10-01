from pathlib import Path
import hashlib
import zipfile

import pandas as pd


BRONZE_PATH = Path(
    "data/bronze/prf"
)

CAUSE_SILVER_PATH = Path(
    "data/silver/prf/accident_cause"
)

TYPE_SILVER_PATH = Path(
    "data/silver/prf/accident_type"
)

CAUSE_REJECTED_PATH = Path(
    "data/rejected/prf/accident_cause"
)

TYPE_REJECTED_PATH = Path(
    "data/rejected/prf/accident_type"
)


YEARS = [
    2024,
    2025,
    2026,
]


# ============================================================
# BRONZE
# ============================================================


def get_latest_snapshot(
    year: int,
) -> Path:

    dataset_path = (
        BRONZE_PATH
        / f"year={year}"
        / "dataset=person_all_causes"
    )

    snapshots = sorted(
        dataset_path.glob("snapshot=*")
    )

    if not snapshots:
        raise FileNotFoundError(
            f"Nenhum snapshot encontrado para {year}"
        )

    return snapshots[-1]


def get_zip_file(
    snapshot: Path,
) -> Path:

    files = list(
        snapshot.glob("*.zip")
    )

    if len(files) != 1:
        raise RuntimeError(
            f"Esperado 1 ZIP em {snapshot}. "
            f"Encontrados: {len(files)}"
        )

    return files[0]


def load_bronze(
    zip_path: Path,
) -> pd.DataFrame:

    with zipfile.ZipFile(
        zip_path,
        "r",
    ) as archive:

        csv_files = [
            name
            for name in archive.namelist()
            if name.lower().endswith(".csv")
        ]

        if len(csv_files) != 1:
            raise RuntimeError(
                f"Esperado 1 CSV em {zip_path}. "
                f"Encontrados: {len(csv_files)}"
            )

        with archive.open(
            csv_files[0]
        ) as file:

            return pd.read_csv(
                file,
                sep=";",
                encoding="latin-1",
                dtype=str,
                low_memory=False,
            )


# ============================================================
# HELPERS
# ============================================================


def clean_string(
    series: pd.Series,
) -> pd.Series:

    result = (
        series
        .astype("string")
        .str.strip()
    )

    return result.mask(
        result == ""
    )


def integer_column(
    series: pd.Series,
) -> pd.Series:

    return pd.to_numeric(
        series,
        errors="coerce",
    ).astype("Int64")


def hash_key(
    values: list,
) -> str:

    raw_key = "|".join(
        ""
        if pd.isna(value)
        else str(value)
        for value in values
    )

    return hashlib.sha256(
        raw_key.encode("utf-8")
    ).hexdigest()[:24]


# ============================================================
# ACCIDENT CAUSE
# ============================================================


def transform_causes(
    raw: pd.DataFrame,
    year: int,
    snapshot: Path,
    source_file: Path,
) -> pd.DataFrame:

    df = pd.DataFrame(
        index=raw.index
    )

    df["accident_id"] = integer_column(
        raw["id"]
    )

    df["accident_cause"] = clean_string(
        raw["causa_acidente"]
    )

    primary_raw = clean_string(
        raw["causa_principal"]
    )

    primary_map = {
        "Sim": True,
        "Não": False,
        "Nao": False,
    }

    df["is_primary_cause"] = (
        primary_raw
        .map(primary_map)
        .astype("boolean")
    )

    df["_source_year"] = year

    df["_source_snapshot"] = (
        snapshot.name
        .replace(
            "snapshot=",
            "",
        )
    )

    df["_source_file"] = (
        source_file.name
    )

    # --------------------------------------------------------
    # Remover redundância causada por
    # pessoa × causa × tipo
    # --------------------------------------------------------

    df = (
        df
        .drop_duplicates(
            subset=[
                "accident_id",
                "accident_cause",
                "is_primary_cause",
            ]
        )
        .reset_index(
            drop=True
        )
    )

    # --------------------------------------------------------
    # Surrogate key
    # --------------------------------------------------------

    df["cause_record_id"] = df.apply(
        lambda row: hash_key(
            [
                row["_source_year"],
                row["accident_id"],
                row["accident_cause"],
            ]
        ),
        axis=1,
    )

    return df


def validate_causes(
    df: pd.DataFrame,
):

    reasons = pd.Series(
        "",
        index=df.index,
        dtype="string",
    )

    def reject(mask, reason):

        nonlocal reasons

        reasons.loc[mask] = (
            reasons.loc[mask]
            + reason
            + ";"
        )

    reject(
        df["accident_id"].isna(),
        "missing_accident_id",
    )

    reject(
        df["accident_cause"].isna(),
        "missing_accident_cause",
    )

    reject(
        df["is_primary_cause"].isna(),
        "invalid_primary_cause_flag",
    )

    valid_mask = (
        reasons == ""
    )

    valid = (
        df.loc[valid_mask]
        .copy()
    )

    rejected = (
        df.loc[~valid_mask]
        .copy()
    )

    rejected["rejection_reason"] = (
        reasons.loc[~valid_mask]
        .str.rstrip(";")
    )

    return valid, rejected


# ============================================================
# ACCIDENT TYPE
# ============================================================


def transform_types(
    raw: pd.DataFrame,
    year: int,
    snapshot: Path,
    source_file: Path,
) -> pd.DataFrame:

    df = pd.DataFrame(
        index=raw.index
    )

    df["accident_id"] = integer_column(
        raw["id"]
    )

    df["accident_type_order"] = (
        integer_column(
            raw["ordem_tipo_acidente"]
        )
    )

    df["accident_type"] = clean_string(
        raw["tipo_acidente"]
    )

    df["_source_year"] = year

    df["_source_snapshot"] = (
        snapshot.name
        .replace(
            "snapshot=",
            "",
        )
    )

    df["_source_file"] = (
        source_file.name
    )

    # --------------------------------------------------------
    # Remove produto cartesiano com
    # pessoas e causas
    # --------------------------------------------------------

    df = (
        df
        .drop_duplicates(
            subset=[
                "accident_id",
                "accident_type_order",
                "accident_type",
            ]
        )
        .reset_index(
            drop=True
        )
    )

    # --------------------------------------------------------
    # Surrogate key
    # --------------------------------------------------------

    df["type_record_id"] = df.apply(
        lambda row: hash_key(
            [
                row["_source_year"],
                row["accident_id"],
                row["accident_type_order"],
            ]
        ),
        axis=1,
    )

    return df


def validate_types(
    df: pd.DataFrame,
):

    reasons = pd.Series(
        "",
        index=df.index,
        dtype="string",
    )

    def reject(mask, reason):

        nonlocal reasons

        reasons.loc[mask] = (
            reasons.loc[mask]
            + reason
            + ";"
        )

    reject(
        df["accident_id"].isna(),
        "missing_accident_id",
    )

    reject(
        df["accident_type_order"].isna(),
        "missing_accident_type_order",
    )

    reject(
        (
            df["accident_type_order"]
            .notna()
            &
            (
                df["accident_type_order"]
                <= 0
            )
        ),
        "invalid_accident_type_order",
    )

    reject(
        df["accident_type"].isna(),
        "missing_accident_type",
    )

    valid_mask = (
        reasons == ""
    )

    valid = (
        df.loc[valid_mask]
        .copy()
    )

    rejected = (
        df.loc[~valid_mask]
        .copy()
    )

    rejected["rejection_reason"] = (
        reasons.loc[~valid_mask]
        .str.rstrip(";")
    )

    return valid, rejected


# ============================================================
# OUTPUT
# ============================================================


def write_parquet(
    df: pd.DataFrame,
    path: Path,
):

    path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    df.to_parquet(
        path,
        engine="pyarrow",
        compression="snappy",
        index=False,
    )


def handle_rejected(
    rejected: pd.DataFrame,
    path: Path,
):

    if not rejected.empty:

        write_parquet(
            rejected,
            path,
        )

    elif path.exists():

        # Remove rejected antigo para evitar stale data
        path.unlink()


# ============================================================
# PIPELINE
# ============================================================


def process_year(
    year: int,
):

    print()
    print("=" * 80)
    print(
        f"PROCESSANDO PERSON_ALL_CAUSES {year}"
    )
    print("=" * 80)

    snapshot = get_latest_snapshot(
        year
    )

    zip_file = get_zip_file(
        snapshot
    )

    raw = load_bronze(
        zip_file
    )

    print()
    print(
        f"Bronze original: {len(raw):,}"
    )

    # ========================================================
    # CAUSES
    # ========================================================

    causes = transform_causes(
        raw=raw,
        year=year,
        snapshot=snapshot,
        source_file=zip_file,
    )

    valid_causes, rejected_causes = (
        validate_causes(
            causes
        )
    )

    print()
    print(
        f"Accident Cause após deduplicação: "
        f"{len(causes):,}"
    )

    print(
        f"Cause válidos: "
        f"{len(valid_causes):,}"
    )

    print(
        f"Cause rejeitados: "
        f"{len(rejected_causes):,}"
    )

    cause_file = (
        CAUSE_SILVER_PATH
        / f"year={year}"
        / "part-000.parquet"
    )

    cause_rejected_file = (
        CAUSE_REJECTED_PATH
        / f"year={year}"
        / "rejected.parquet"
    )

    write_parquet(
        valid_causes,
        cause_file,
    )

    handle_rejected(
        rejected_causes,
        cause_rejected_file,
    )

    # ========================================================
    # TYPES
    # ========================================================

    types = transform_types(
        raw=raw,
        year=year,
        snapshot=snapshot,
        source_file=zip_file,
    )

    valid_types, rejected_types = (
        validate_types(
            types
        )
    )

    print()
    print(
        f"Accident Type após deduplicação: "
        f"{len(types):,}"
    )

    print(
        f"Type válidos: "
        f"{len(valid_types):,}"
    )

    print(
        f"Type rejeitados: "
        f"{len(rejected_types):,}"
    )

    type_file = (
        TYPE_SILVER_PATH
        / f"year={year}"
        / "part-000.parquet"
    )

    type_rejected_file = (
        TYPE_REJECTED_PATH
        / f"year={year}"
        / "rejected.parquet"
    )

    write_parquet(
        valid_types,
        type_file,
    )

    handle_rejected(
        rejected_types,
        type_rejected_file,
    )

    print()
    print(
        f"Cause Silver: {cause_file}"
    )

    print(
        f"Type Silver:  {type_file}"
    )


def main():

    for year in YEARS:

        process_year(
            year
        )

    print()
    print("=" * 80)
    print(
        "PERSON_ALL_CAUSES decomposto "
        "com sucesso."
    )
    print("=" * 80)


if __name__ == "__main__":
    main()
