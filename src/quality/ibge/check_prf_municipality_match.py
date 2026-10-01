from pathlib import Path
import unicodedata

import pandas as pd


PRF_PATH = Path(
    "data/silver/prf/occurrence"
)

IBGE_FILE = Path(
    "data/silver/ibge/municipality/part-000.parquet"
)


def normalize_name(
    value,
):

    if pd.isna(value):
        return None

    value = str(
        value
    ).strip()

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


def load_prf():

    files = sorted(
        PRF_PATH.glob(
            "year=*/part-000.parquet"
        )
    )

    if not files:
        raise FileNotFoundError(
            "Nenhum arquivo PRF occurrence encontrado."
        )

    frames = []

    for file in files:

        df = pd.read_parquet(
            file,
            columns=[
                "state",
                "municipality",
                "accident_id",
                "_source_year",
            ],
        )

        frames.append(
            df
        )

    return pd.concat(
        frames,
        ignore_index=True,
    )


def main():

    print()
    print("=" * 100)
    print(
        "MATCH PRF ↔ IBGE - MUNICÍPIOS DO CEARÁ"
    )
    print("=" * 100)

    prf = load_prf()

    prf = prf.loc[
        prf["state"] == "CE"
    ].copy()

    ibge = pd.read_parquet(
        IBGE_FILE
    )

    prf[
        "municipality_match_key"
    ] = (
        prf["municipality"]
        .map(normalize_name)
    )

    # ========================================================
    # MUNICÍPIOS DISTINTOS
    # ========================================================

    prf_municipalities = (
        prf[
            [
                "municipality",
                "municipality_match_key",
            ]
        ]
        .drop_duplicates()
        .sort_values(
            "municipality"
        )
    )

    ibge_keys = set(
        ibge[
            "municipality_match_key"
        ]
        .dropna()
    )

    prf_municipalities[
        "matched_ibge"
    ] = (
        prf_municipalities[
            "municipality_match_key"
        ]
        .isin(
            ibge_keys
        )
    )

    total_names = len(
        prf_municipalities
    )

    matched_names = int(
        prf_municipalities[
            "matched_ibge"
        ]
        .sum()
    )

    unmatched_names = (
        total_names
        - matched_names
    )

    print()
    print(
        f"Municípios distintos na PRF CE: "
        f"{total_names:,}"
    )

    print(
        f"Com match no IBGE: "
        f"{matched_names:,}"
    )

    print(
        f"Sem match no IBGE: "
        f"{unmatched_names:,}"
    )

    # ========================================================
    # MATCH POR LINHA DE ACIDENTE
    # ========================================================

    prf[
        "matched_ibge"
    ] = (
        prf[
            "municipality_match_key"
        ]
        .isin(
            ibge_keys
        )
    )

    total_accidents = len(
        prf
    )

    matched_accidents = int(
        prf[
            "matched_ibge"
        ]
        .sum()
    )

    coverage = (
        matched_accidents
        / total_accidents
        * 100
        if total_accidents
        else 0
    )

    print()
    print(
        f"Acidentes CE: "
        f"{total_accidents:,}"
    )

    print(
        f"Acidentes com município "
        f"mapeado no IBGE: "
        f"{matched_accidents:,}"
    )

    print(
        f"Cobertura: "
        f"{coverage:.2f}%"
    )

    # ========================================================
    # NÃO CASADOS
    # ========================================================

    unmatched = (
        prf_municipalities.loc[
            ~prf_municipalities[
                "matched_ibge"
            ]
        ]
    )

    print()
    print("=" * 100)
    print(
        "MUNICÍPIOS DA PRF SEM MATCH NO IBGE"
    )
    print("=" * 100)

    if unmatched.empty:

        print()
        print(
            "Nenhum. Match completo."
        )

    else:

        print()
        print(
            unmatched[
                [
                    "municipality",
                    "municipality_match_key",
                ]
            ]
            .to_string(
                index=False
            )
        )

        print()
        print(
            "Quantidade de acidentes "
            "afetados por município:"
        )

        affected = (
            prf.loc[
                ~prf["matched_ibge"]
            ]
            .groupby(
                [
                    "municipality",
                    "municipality_match_key",
                ],
                dropna=False,
            )
            .size()
            .reset_index(
                name="accidents"
            )
            .sort_values(
                "accidents",
                ascending=False,
            )
        )

        print()
        print(
            affected.to_string(
                index=False
            )
        )

    # ========================================================
    # RESULTADO
    # ========================================================

    print()
    print("=" * 100)

    if unmatched_names == 0:

        print(
            "STATUS: MATCH PRF ↔ IBGE APROVADO"
        )

    else:

        print(
            "STATUS: EXISTEM NOMES PARA TRATAR"
        )

        raise SystemExit(1)


if __name__ == "__main__":
    main()
