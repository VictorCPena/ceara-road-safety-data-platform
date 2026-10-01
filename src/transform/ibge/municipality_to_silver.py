from pathlib import Path
import json
import unicodedata

import pandas as pd


BRONZE_PATH = Path(
    "data/bronze/ibge/dataset=municipality"
)

SILVER_PATH = Path(
    "data/silver/ibge/municipality"
)

SOURCE_URL = (
    "https://servicodados.ibge.gov.br/"
    "api/v1/localidades/estados/23/municipios"
    "?orderBy=nome"
)


def get_latest_snapshot() -> Path:

    snapshots = sorted(
        BRONZE_PATH.glob(
            "snapshot=*"
        )
    )

    if not snapshots:

        raise FileNotFoundError(
            "Nenhum snapshot IBGE encontrado."
        )

    return snapshots[-1]


def normalize_name(
    value: str,
) -> str | None:

    if value is None:
        return None

    value = str(value).strip()

    if not value:
        return None

    normalized = unicodedata.normalize(
        "NFKD",
        value,
    )

    without_accents = "".join(
        char
        for char in normalized
        if not unicodedata.combining(
            char
        )
    )

    return (
        without_accents
        .upper()
        .strip()
    )


def get_nested(
    dictionary,
    *keys,
):

    current = dictionary

    for key in keys:

        if not isinstance(
            current,
            dict,
        ):

            return None

        current = current.get(
            key
        )

    return current


def main():

    print()
    print("=" * 80)
    print(
        "IBGE MUNICIPALITY - BRONZE → SILVER"
    )
    print("=" * 80)

    snapshot = get_latest_snapshot()

    input_file = (
        snapshot
        / "municipalities_ce.json"
    )

    with input_file.open(
        "r",
        encoding="utf-8",
    ) as file:

        raw = json.load(
            file
        )

    print()
    print(
        f"Bronze: {len(raw):,}"
    )

    rows = []

    for item in raw:

        immediate = item.get(
            "regiao-imediata"
        ) or {}

        intermediate = immediate.get(
            "regiao-intermediaria"
        ) or {}

        state = intermediate.get(
            "UF"
        ) or {}

        region = state.get(
            "regiao"
        ) or {}

        municipality_code = item.get(
            "id"
        )

        municipality_name = item.get(
            "nome"
        )

        row = {

            "ibge_code": (
                str(municipality_code)
                .zfill(7)
                if municipality_code
                is not None
                else None
            ),

            "municipality":
                municipality_name,

            "municipality_match_key":
                normalize_name(
                    municipality_name
                ),

            "state_code": (
                str(
                    state.get("id")
                ).zfill(2)
                if state.get("id")
                is not None
                else None
            ),

            "state_abbr":
                state.get("sigla"),

            "state_name":
                state.get("nome"),

            "region_code": (
                str(
                    region.get("id")
                )
                if region.get("id")
                is not None
                else None
            ),

            "region_abbr":
                region.get("sigla"),

            "region_name":
                region.get("nome"),

            "immediate_region_code": (
                str(
                    immediate.get("id")
                ).zfill(6)
                if immediate.get("id")
                is not None
                else None
            ),

            "immediate_region":
                immediate.get("nome"),

            "intermediate_region_code": (
                str(
                    intermediate.get(
                        "id"
                    )
                ).zfill(4)
                if intermediate.get(
                    "id"
                ) is not None
                else None
            ),

            "intermediate_region":
                intermediate.get(
                    "nome"
                ),

            "_source_snapshot":
                snapshot.name.replace(
                    "snapshot=",
                    "",
                ),

            "_source_file":
                input_file.name,

            "_source_url":
                SOURCE_URL,
        }

        rows.append(
            row
        )

    df = pd.DataFrame(
        rows
    )

    output_file = (
        SILVER_PATH
        / "part-000.parquet"
    )

    output_file.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    df.to_parquet(
        output_file,
        index=False,
        engine="pyarrow",
        compression="snappy",
    )

    print(
        f"Silver: {len(df):,}"
    )

    print()
    print(
        df[
            [
                "ibge_code",
                "municipality",
                "municipality_match_key",
                "immediate_region",
                "intermediate_region",
            ]
        ]
        .head(10)
        .to_string(
            index=False
        )
    )

    print()
    print(
        f"Arquivo: {output_file}"
    )


if __name__ == "__main__":
    main()
