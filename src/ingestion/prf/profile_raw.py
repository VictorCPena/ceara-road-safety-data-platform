import json
import zipfile
from pathlib import Path

import pandas as pd


BRONZE_PATH = Path("data/bronze/prf")

OUTPUT_PATH = Path(
    "metadata/prf_raw_profile.json"
)


YEARS = [
    2024,
    2025,
    2026,
]


DATASETS = [
    "occurrence",
    "person",
    "person_all_causes",
]


def get_latest_snapshot(
    year: int,
    dataset: str,
) -> Path:

    dataset_path = (
        BRONZE_PATH
        / f"year={year}"
        / f"dataset={dataset}"
    )

    snapshots = sorted(
        dataset_path.glob("snapshot=*")
    )

    if not snapshots:
        raise FileNotFoundError(
            f"Nenhum snapshot em {dataset_path}"
        )

    return snapshots[-1]


def get_zip_file(
    snapshot_path: Path,
) -> Path:

    files = list(
        snapshot_path.glob("*.zip")
    )

    if len(files) != 1:
        raise RuntimeError(
            f"Esperado 1 ZIP em {snapshot_path}, "
            f"encontrados: {len(files)}"
        )

    return files[0]


def get_csv_name(
    zip_path: Path,
) -> str:

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
            f"Esperado 1 CSV em {zip_path}, "
            f"encontrados: {len(csv_files)}"
        )

    return csv_files[0]


def load_raw_dataset(
    zip_path: Path,
) -> pd.DataFrame:

    csv_name = get_csv_name(
        zip_path
    )

    with zipfile.ZipFile(
        zip_path,
        "r",
    ) as archive:

        with archive.open(
            csv_name
        ) as csv_file:

            df = pd.read_csv(
                csv_file,
                sep=";",
                encoding="latin-1",

                # Muito importante:
                # nesta etapa não queremos
                # que Pandas invente tipos.
                dtype=str,

                low_memory=False,
            )

    return df


def safe_examples(
    series: pd.Series,
    limit: int = 5,
) -> list:

    values = (
        series
        .dropna()
        .astype(str)
        .unique()
        .tolist()
    )

    return values[:limit]


def profile_dataset(
    year: int,
    dataset: str,
) -> dict:

    snapshot = get_latest_snapshot(
        year=year,
        dataset=dataset,
    )

    zip_path = get_zip_file(
        snapshot
    )

    print()
    print("=" * 70)
    print(
        f"{year} | {dataset}"
    )
    print(
        f"Lendo: {zip_path}"
    )

    df = load_raw_dataset(
        zip_path
    )

    row_count = len(df)

    profile = {
        "year": year,
        "dataset": dataset,
        "snapshot": snapshot.name,
        "rows": row_count,
        "columns": len(df.columns),
        "column_profile": {},
    }

    print(
        f"Linhas: {row_count:,}"
    )

    for column in df.columns:

        null_count = int(
            df[column].isna().sum()
        )

        distinct_count = int(
            df[column].nunique(
                dropna=True
            )
        )

        profile[
            "column_profile"
        ][column] = {
            "nulls": null_count,
            "null_pct": round(
                null_count
                / row_count
                * 100,
                4,
            )
            if row_count
            else 0,
            "distinct": distinct_count,
            "examples": safe_examples(
                df[column]
            ),
        }

    # --------------------------------
    # Algumas verificações de grain
    # --------------------------------

    if "id" in df.columns:

        profile["id_analysis"] = {
            "nulls": int(
                df["id"].isna().sum()
            ),
            "distinct": int(
                df["id"].nunique(
                    dropna=True
                )
            ),
            "duplicates": int(
                df["id"].duplicated(
                    keep=False
                ).sum()
            ),
        }

    if "pesid" in df.columns:

        profile["pesid_analysis"] = {
            "nulls": int(
                df["pesid"].isna().sum()
            ),
            "distinct": int(
                df["pesid"].nunique(
                    dropna=True
                )
            ),
            "duplicates": int(
                df["pesid"].duplicated(
                    keep=False
                ).sum()
            ),
        }

    # --------------------------------
    # Mostrar exemplos importantes
    # --------------------------------

    interesting_columns = [
        "id",
        "pesid",
        "data_inversa",
        "horario",
        "uf",
        "br",
        "km",
        "municipio",
        "idade",
        "sexo",
        "latitude",
        "longitude",
    ]

    print()
    print("Exemplos:")

    for column in interesting_columns:

        if column in df.columns:

            examples = safe_examples(
                df[column]
            )

            print(
                f"{column}: {examples}"
            )

    return profile


def main():

    report = []

    for year in YEARS:

        for dataset in DATASETS:

            profile = profile_dataset(
                year=year,
                dataset=dataset,
            )

            report.append(
                profile
            )

    OUTPUT_PATH.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    with OUTPUT_PATH.open(
        "w",
        encoding="utf-8",
    ) as file:

        json.dump(
            report,
            file,
            indent=2,
            ensure_ascii=False,
        )

    print()
    print("=" * 70)
    print(
        f"Profile salvo em: {OUTPUT_PATH}"
    )


if __name__ == "__main__":
    main()
