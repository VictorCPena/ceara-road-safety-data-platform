from pathlib import Path
import json
import unicodedata

import pandas as pd


BRONZE_PATH = Path(
    "data/bronze/ibge/"
    "dataset=municipality_population"
)

SILVER_PATH = Path(
    "data/silver/ibge/"
    "municipality_population"
)

SOURCE_TABLE = 6579
SOURCE_VARIABLE = 9324


def normalize_label(
    value,
):

    value = str(
        value
    ).strip()

    normalized = (
        unicodedata.normalize(
            "NFKD",
            value,
        )
    )

    return "".join(
        char
        for char in normalized
        if not unicodedata.combining(
            char
        )
    ).lower()


def find_header_key(
    header: dict,
    expected_label: str,
):

    expected = normalize_label(
        expected_label
    )

    for key, value in header.items():

        if (
            normalize_label(value)
            == expected
        ):

            return key

    raise KeyError(
        "Coluna SIDRA não encontrada: "
        f"{expected_label}"
    )


def get_latest_snapshot() -> Path:

    snapshots = sorted(
        BRONZE_PATH.glob(
            "snapshot=*"
        )
    )

    if not snapshots:

        raise FileNotFoundError(
            "Nenhum snapshot de "
            "população encontrado."
        )

    return snapshots[-1]


def main():

    print()
    print("=" * 90)
    print(
        "IBGE POPULAÇÃO MUNICIPAL "
        "- BRONZE → SILVER"
    )
    print("=" * 90)

    snapshot = (
        get_latest_snapshot()
    )

    input_file = (
        snapshot
        / "population_ce_2024_2026.json"
    )

    with input_file.open(
        "r",
        encoding="utf-8",
    ) as file:

        raw = json.load(
            file
        )

    if len(raw) <= 1:

        raise RuntimeError(
            "Arquivo SIDRA sem dados."
        )

    header = raw[0]
    records = raw[1:]

    municipality_code_key = (
        find_header_key(
            header,
            "Município (Código)",
        )
    )

    municipality_name_key = (
        find_header_key(
            header,
            "Município",
        )
    )

    year_key = find_header_key(
        header,
        "Ano (Código)",
    )

    value_key = find_header_key(
        header,
        "Valor",
    )

    rows = []

    for record in records:

        population_raw = (
            record.get(
                value_key
            )
        )

        population = (
            pd.to_numeric(
                population_raw,
                errors="coerce",
            )
        )

        year = pd.to_numeric(
            record.get(
                year_key
            ),
            errors="coerce",
        )

        municipality_code = (
            record.get(
                municipality_code_key
            )
        )

        municipality_name = (
            record.get(
                municipality_name_key
            )
        )

        rows.append(
            {
                "ibge_code": (
                    str(
                        municipality_code
                    ).zfill(7)
                    if municipality_code
                    is not None
                    else None
                ),

                "municipality":
                    municipality_name,

                "year": (
                    int(year)
                    if pd.notna(year)
                    else None
                ),

                "population": (
                    int(population)
                    if pd.notna(
                        population
                    )
                    else None
                ),

                "_source_table":
                    SOURCE_TABLE,

                "_source_variable":
                    SOURCE_VARIABLE,

                "_source_snapshot":
                    snapshot.name.replace(
                        "snapshot=",
                        "",
                    ),

                "_source_file":
                    input_file.name,
            }
        )

    df = pd.DataFrame(
        rows
    )

    print()
    print(
        f"Bronze records: "
        f"{len(records):,}"
    )

    print(
        f"Silver records: "
        f"{len(df):,}"
    )

    for year in sorted(
        df["year"]
        .dropna()
        .unique()
    ):

        year_df = (
            df.loc[
                df["year"] == year
            ]
            .copy()
        )

        output_file = (
            SILVER_PATH
            / f"year={year}"
            / "part-000.parquet"
        )

        output_file.parent.mkdir(
            parents=True,
            exist_ok=True,
        )

        year_df.to_parquet(
            output_file,
            engine="pyarrow",
            compression="snappy",
            index=False,
        )

        print(
            f"{year}: "
            f"{len(year_df):,} registros "
            f"→ {output_file}"
        )

    print()
    print(
        "Amostra:"
    )

    print()

    print(
        df[
            [
                "ibge_code",
                "municipality",
                "year",
                "population",
            ]
        ]
        .head(15)
        .to_string(
            index=False
        )
    )


if __name__ == "__main__":
    main()
