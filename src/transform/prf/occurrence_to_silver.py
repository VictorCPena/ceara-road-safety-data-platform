from pathlib import Path
import zipfile

import pandas as pd


BRONZE_PATH = Path("data/bronze/prf")

SILVER_PATH = Path(
    "data/silver/prf/occurrence"
)

REJECTED_PATH = Path(
    "data/rejected/prf/occurrence"
)


YEARS = [
    2024,
    2025,
    2026,
]


# ========================================
# BRONZE
# ========================================


def get_latest_snapshot(
    year: int,
) -> Path:

    dataset_path = (
        BRONZE_PATH
        / f"year={year}"
        / "dataset=occurrence"
    )

    snapshots = sorted(
        dataset_path.glob("snapshot=*")
    )

    if not snapshots:
        raise FileNotFoundError(
            f"Nenhum snapshot para {year}"
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
            f"Esperado 1 ZIP em {snapshot}"
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
                f"Esperado 1 CSV em {zip_path}"
            )

        with archive.open(
            csv_files[0]
        ) as file:

            df = pd.read_csv(
                file,
                sep=";",
                encoding="latin-1",
                dtype=str,
                low_memory=False,
            )

    return df


# ========================================
# HELPERS
# ========================================


def decimal_to_number(
    series: pd.Series,
) -> pd.Series:

    return pd.to_numeric(
        series
        .str.strip()
        .str.replace(
            ",",
            ".",
            regex=False,
        ),
        errors="coerce",
    )


def integer_column(
    series: pd.Series,
) -> pd.Series:

    return pd.to_numeric(
        series,
        errors="coerce",
    ).astype("Int64")


def clean_string(
    series: pd.Series,
) -> pd.Series:

    return (
        series
        .astype("string")
        .str.strip()
    )


# ========================================
# TRANSFORMAÇÃO
# ========================================


def transform(
    raw: pd.DataFrame,
    year: int,
    snapshot: Path,
    source_file: Path,
) -> pd.DataFrame:

    df = pd.DataFrame()

    # Identificação
    df["accident_id"] = integer_column(
        raw["id"]
    )

    # Data / hora
    df["event_date"] = pd.to_datetime(
        raw["data_inversa"],
        errors="coerce",
    )

    df["event_timestamp"] = pd.to_datetime(
        raw["data_inversa"]
        + " "
        + raw["horario"],
        errors="coerce",
    )

    df["weekday"] = clean_string(
        raw["dia_semana"]
    )

    # Localização
    df["state"] = (
        clean_string(raw["uf"])
        .str.upper()
    )

    df["highway"] = integer_column(
        raw["br"]
    )

    df["km"] = decimal_to_number(
        raw["km"]
    )

    df["municipality"] = clean_string(
        raw["municipio"]
    )

    df["latitude"] = decimal_to_number(
        raw["latitude"]
    )

    df["longitude"] = decimal_to_number(
        raw["longitude"]
    )

    # Características
    df["accident_cause"] = clean_string(
        raw["causa_acidente"]
    )

    df["accident_type"] = clean_string(
        raw["tipo_acidente"]
    )

    df["accident_classification"] = (
        clean_string(
            raw["classificacao_acidente"]
        )
    )

    df["day_phase"] = clean_string(
        raw["fase_dia"]
    )

    df["road_direction"] = clean_string(
        raw["sentido_via"]
    )

    df["weather_condition"] = clean_string(
        raw["condicao_metereologica"]
    )

    df["road_type"] = clean_string(
        raw["tipo_pista"]
    )

    df["road_layout"] = clean_string(
        raw["tracado_via"]
    )

    df["land_use"] = clean_string(
        raw["uso_solo"]
    )

    # Métricas
    count_columns = {
        "people_count": "pessoas",
        "deaths": "mortos",
        "minor_injuries": "feridos_leves",
        "serious_injuries": "feridos_graves",
        "uninjured": "ilesos",
        "unknown_status": "ignorados",
        "injured": "feridos",
        "vehicles": "veiculos",
    }

    for target, source in count_columns.items():

        df[target] = integer_column(
            raw[source]
        )

    # Estrutura organizacional PRF
    df["regional"] = clean_string(
        raw["regional"]
    )

    df["police_station"] = clean_string(
        raw["delegacia"]
    )

    df["uop"] = clean_string(
        raw["uop"]
    )

    # Lineage
    df["_source_year"] = year

    df["_source_snapshot"] = (
        snapshot.name
        .replace("snapshot=", "")
    )

    df["_source_file"] = (
        source_file.name
    )

    return df


# ========================================
# DATA QUALITY
# ========================================


def validate(
    df: pd.DataFrame,
):

    reasons = pd.Series(
        "",
        index=df.index,
        dtype="string",
    )

    def reject(
        mask: pd.Series,
        reason: str,
    ):

        nonlocal reasons

        reasons.loc[mask] = (
            reasons.loc[mask]
            + reason
            + ";"
        )

    # ID obrigatório
    reject(
        df["accident_id"].isna(),
        "missing_accident_id",
    )

    # Grain esperado:
    # 1 linha = 1 acidente
    duplicate_id = (
        df["accident_id"]
        .notna()
        &
        df["accident_id"].duplicated(
            keep=False
        )
    )

    reject(
        duplicate_id,
        "duplicate_accident_id",
    )

    # Datas
    reject(
        df["event_date"].isna(),
        "invalid_event_date",
    )

    reject(
        df["event_timestamp"].isna(),
        "invalid_event_timestamp",
    )

    # UF
    invalid_state = (
        df["state"].isna()
        |
        ~df["state"].str.match(
            r"^[A-Z]{2}$",
            na=False,
        )
    )

    reject(
        invalid_state,
        "invalid_state",
    )

    # Latitude
    invalid_latitude = (
        df["latitude"].notna()
        &
        ~df["latitude"].between(
            -90,
            90,
        )
    )

    reject(
        invalid_latitude,
        "invalid_latitude",
    )

    # Longitude
    invalid_longitude = (
        df["longitude"].notna()
        &
        ~df["longitude"].between(
            -180,
            180,
        )
    )

    reject(
        invalid_longitude,
        "invalid_longitude",
    )

    # Valores que não podem ser negativos
    non_negative_columns = [
        "people_count",
        "deaths",
        "minor_injuries",
        "serious_injuries",
        "uninjured",
        "unknown_status",
        "injured",
        "vehicles",
    ]

    for column in non_negative_columns:

        invalid = (
            df[column].notna()
            &
            (df[column] < 0)
        )

        reject(
            invalid,
            f"negative_{column}",
        )

    valid_mask = reasons.eq("")

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


# ========================================
# WRITE
# ========================================


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
        index=False,
        compression="snappy",
    )


# ========================================
# PIPELINE
# ========================================


def process_year(
    year: int,
):

    print()
    print("=" * 70)
    print(f"PROCESSANDO {year}")
    print("=" * 70)

    snapshot = get_latest_snapshot(
        year
    )

    zip_file = get_zip_file(
        snapshot
    )

    raw = load_bronze(
        zip_file
    )

    print(
        f"Bronze: {len(raw):,} registros"
    )

    transformed = transform(
        raw=raw,
        year=year,
        snapshot=snapshot,
        source_file=zip_file,
    )

    valid, rejected = validate(
        transformed
    )

    print(
        f"Válidos: {len(valid):,}"
    )

    print(
        f"Rejeitados: {len(rejected):,}"
    )

    silver_file = (
        SILVER_PATH
        / f"year={year}"
        / "part-000.parquet"
    )

    rejected_file = (
        REJECTED_PATH
        / f"year={year}"
        / "rejected.parquet"
    )

    write_parquet(
        valid,
        silver_file,
    )

    if not rejected.empty:

        write_parquet(
            rejected,
            rejected_file,
        )

    print(
        f"Silver: {silver_file}"
    )


def main():

    for year in YEARS:

        process_year(
            year
        )

    print()
    print(
        "Transformação Bronze → Silver concluída."
    )


if __name__ == "__main__":
    main()
