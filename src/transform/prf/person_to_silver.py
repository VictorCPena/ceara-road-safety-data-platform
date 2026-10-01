from pathlib import Path
import hashlib
import zipfile

import pandas as pd


BRONZE_PATH = Path("data/bronze/prf")
SILVER_PATH = Path("data/silver/prf/person")
REJECTED_PATH = Path("data/rejected/prf/person")

YEARS = [2024, 2025, 2026]


# ============================================================
# BRONZE
# ============================================================


def get_latest_snapshot(year: int) -> Path:

    dataset_path = (
        BRONZE_PATH
        / f"year={year}"
        / "dataset=person"
    )

    snapshots = sorted(
        dataset_path.glob("snapshot=*")
    )

    if not snapshots:
        raise FileNotFoundError(
            f"Nenhum snapshot para {year}"
        )

    return snapshots[-1]


def get_zip_file(snapshot: Path) -> Path:

    files = list(snapshot.glob("*.zip"))

    if len(files) != 1:
        raise RuntimeError(
            f"Esperado 1 ZIP em {snapshot}. "
            f"Encontrados: {len(files)}"
        )

    return files[0]


def load_bronze(zip_path: Path) -> pd.DataFrame:

    with zipfile.ZipFile(zip_path, "r") as archive:

        csv_files = [
            name
            for name in archive.namelist()
            if name.lower().endswith(".csv")
        ]

        if len(csv_files) != 1:
            raise RuntimeError(
                f"Esperado 1 CSV em {zip_path}"
            )

        with archive.open(csv_files[0]) as file:

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


def decimal_column(
    series: pd.Series,
) -> pd.Series:

    return pd.to_numeric(
        clean_string(series)
        .str.replace(",", ".", regex=False),
        errors="coerce",
    )


def normalize_age(
    raw_age: pd.Series,
) -> tuple[
    pd.Series,
    pd.Series,
    pd.Series,
]:
    """
    Retorna:
    - age_raw: valor recebido da fonte
    - age: idade válida entre 0 e 120
    - age_quality_flag: classificação da qualidade

    A PRF documenta -1 como idade não coletada.
    Outros valores fora de 0..120 são preservados
    em age_raw, mas não usados como idade válida.
    """

    age_raw = clean_string(raw_age)

    numeric_age = pd.to_numeric(
        age_raw,
        errors="coerce",
    ).astype("Int64")

    age = numeric_age.where(
        numeric_age.between(
            0,
            120,
        )
    )

    quality = pd.Series(
        "invalid_format",
        index=raw_age.index,
        dtype="string",
    )

    quality.loc[
        age_raw.isna()
    ] = "missing"

    quality.loc[
        numeric_age == -1
    ] = "missing_source"

    quality.loc[
        numeric_age.between(
            0,
            120,
        )
    ] = "valid"

    invalid_range = (
        numeric_age.notna()
        &
        ~numeric_age.between(
            -1,
            120,
        )
    )

    quality.loc[
        invalid_range
    ] = "invalid_out_of_range"

    return (
        age_raw,
        age,
        quality,
    )


def normalize_vehicle_year(
    raw_year: pd.Series,
    event_date: pd.Series,
) -> tuple[
    pd.Series,
    pd.Series,
]:
    """
    0 é tratado como desconhecido.

    Valores fora do intervalo plausível
    também são normalizados para NULL,
    sem rejeitar o registro inteiro.
    """

    numeric_year = integer_column(
        raw_year
    )

    event_year = (
        event_date
        .dt.year
        .astype("Int64")
    )

    result = numeric_year.mask(
        numeric_year == 0
    )

    valid = (
        result.isna()
        |
        (
            (result >= 1900)
            &
            (result <= event_year)
        )
    )

    quality = pd.Series(
        "valid",
        index=raw_year.index,
        dtype="string",
    )

    quality.loc[
        numeric_year == 0
    ] = "missing_source"

    quality.loc[
        numeric_year.isna()
    ] = "missing"

    quality.loc[
        ~valid
    ] = "invalid_out_of_range"

    result = result.where(valid)

    return result, quality


def generate_record_ids(
    df: pd.DataFrame,
) -> pd.Series:

    def normalize(value):

        if pd.isna(value):
            return ""

        return str(int(value))

    keys = (
        df["_source_year"]
        .astype(str)
        + "|"
        + df["accident_id"].map(normalize)
        + "|"
        + df["person_id"].map(normalize)
        + "|"
        + df["vehicle_id"].map(normalize)
    )

    return keys.map(
        lambda value:
        hashlib.sha256(
            value.encode("utf-8")
        ).hexdigest()[:24]
    )


# ============================================================
# TRANSFORMAÇÃO
# ============================================================


def transform(
    raw: pd.DataFrame,
    year: int,
    snapshot: Path,
    source_file: Path,
) -> pd.DataFrame:

    df = pd.DataFrame(
        index=raw.index
    )

    # --------------------------------------------------------
    # IDs
    # --------------------------------------------------------

    df["accident_id"] = integer_column(
        raw["id"]
    )

    person_id = integer_column(
        raw["pesid"]
    )

    # A investigação mostrou que 0 não funciona
    # como identificador real de pessoa.
    df["person_id"] = person_id.mask(
        person_id == 0
    )

    vehicle_id = integer_column(
        raw["id_veiculo"]
    )

    df["vehicle_id"] = vehicle_id.mask(
        vehicle_id == 0
    )

    # --------------------------------------------------------
    # DATA / HORA
    # --------------------------------------------------------

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

    # --------------------------------------------------------
    # LOCALIZAÇÃO
    # --------------------------------------------------------

    df["state"] = (
        clean_string(raw["uf"])
        .str.upper()
    )

    df["highway"] = integer_column(
        raw["br"]
    )

    df["km"] = decimal_column(
        raw["km"]
    )

    df["municipality"] = clean_string(
        raw["municipio"]
    )

    df["latitude"] = decimal_column(
        raw["latitude"]
    )

    df["longitude"] = decimal_column(
        raw["longitude"]
    )

    # --------------------------------------------------------
    # ACIDENTE
    # --------------------------------------------------------

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

    df["weather_condition"] = (
        clean_string(
            raw["condicao_metereologica"]
        )
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

    # --------------------------------------------------------
    # VEÍCULO
    # --------------------------------------------------------

    df["vehicle_type"] = clean_string(
        raw["tipo_veiculo"]
    )

    df["vehicle_brand"] = clean_string(
        raw["marca"]
    )

    (
        df["vehicle_manufacture_year"],
        df["vehicle_year_quality_flag"],
    ) = normalize_vehicle_year(
        raw["ano_fabricacao_veiculo"],
        df["event_date"],
    )

    # --------------------------------------------------------
    # PESSOA
    # --------------------------------------------------------

    df["person_type"] = clean_string(
        raw["tipo_envolvido"]
    )

    df["physical_condition"] = (
        clean_string(
            raw["estado_fisico"]
        )
    )

    (
        df["age_raw"],
        df["age"],
        df["age_quality_flag"],
    ) = normalize_age(
        raw["idade"]
    )

    df["sex"] = clean_string(
        raw["sexo"]
    )

    # --------------------------------------------------------
    # INDICADORES
    # --------------------------------------------------------

    df["uninjured"] = integer_column(
        raw["ilesos"]
    )

    df["minor_injury"] = integer_column(
        raw["feridos_leves"]
    )

    df["serious_injury"] = integer_column(
        raw["feridos_graves"]
    )

    df["death"] = integer_column(
        raw["mortos"]
    )

    # --------------------------------------------------------
    # ESTRUTURA PRF
    # --------------------------------------------------------

    df["regional"] = clean_string(
        raw["regional"]
    )

    df["police_station"] = clean_string(
        raw["delegacia"]
    )

    df["uop"] = clean_string(
        raw["uop"]
    )

    # --------------------------------------------------------
    # LINEAGE
    # --------------------------------------------------------

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
    # SURROGATE KEY
    # --------------------------------------------------------

    df["record_id"] = (
        generate_record_ids(df)
    )

    return df


# ============================================================
# DATA QUALITY
# ============================================================


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

    # ID do acidente é essencial
    reject(
        df["accident_id"].isna(),
        "missing_accident_id",
    )

    # Pelo menos pessoa ou veículo
    # precisa identificar o envolvimento.
    reject(
        (
            df["person_id"].isna()
            &
            df["vehicle_id"].isna()
        ),
        "missing_person_and_vehicle_id",
    )

    reject(
        df["event_date"].isna(),
        "invalid_event_date",
    )

    reject(
        df["event_timestamp"].isna(),
        "invalid_event_timestamp",
    )

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

    reject(
        (
            df["latitude"].notna()
            &
            ~df["latitude"].between(
                -90,
                90,
            )
        ),
        "invalid_latitude",
    )

    reject(
        (
            df["longitude"].notna()
            &
            ~df["longitude"].between(
                -180,
                180,
            )
        ),
        "invalid_longitude",
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


# ============================================================
# PIPELINE
# ============================================================


def process_year(
    year: int,
):

    print()
    print("=" * 70)
    print(
        f"PROCESSANDO PERSON {year}"
    )
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
        f"Bronze: {len(raw):,}"
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

    print(
        "Sem person_id real: "
        f"{valid['person_id'].isna().sum():,}"
    )

    print(
        "Idade inválida normalizada: "
        f"{(
            valid['age_quality_flag']
            == 'invalid_out_of_range'
        ).sum():,}"
    )

    print(
        "Ano veículo desconhecido: "
        f"{valid[
            'vehicle_manufacture_year'
        ].isna().sum():,}"
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

    elif rejected_file.exists():

        rejected_file.unlink()

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
        "Transformação PERSON "
        "Bronze → Silver concluída."
    )


if __name__ == "__main__":
    main()