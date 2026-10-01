from pathlib import Path
import zipfile

import duckdb
import pandas as pd


BRONZE_PATH = Path(
    "data/bronze/prf"
)

YEARS = [
    2024,
    2025,
    2026,
]


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


def load_raw(
    year: int,
) -> pd.DataFrame:

    snapshot = get_latest_snapshot(
        year
    )

    zip_path = get_zip_file(
        snapshot
    )

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

    df["_source_year"] = year

    return df


def main():

    print()
    print("=" * 100)
    print(
        "INVESTIGAÇÃO DO GRAIN - "
        "PERSON_ALL_CAUSES"
    )
    print("=" * 100)

    frames = []

    for year in YEARS:

        print(
            f"Carregando {year}..."
        )

        frames.append(
            load_raw(year)
        )

    df = pd.concat(
        frames,
        ignore_index=True,
    )

    con = duckdb.connect()

    con.register(
        "all_causes",
        df,
    )

    print()
    print("=" * 100)
    print("CONTAGENS GERAIS")
    print("=" * 100)

    overview = con.execute(
        """
        SELECT
            _source_year AS year,

            COUNT(*) AS rows,

            COUNT(DISTINCT id)
                AS accidents,

            COUNT(DISTINCT pesid)
                AS distinct_person_ids,

            COUNT(DISTINCT id_veiculo)
                AS distinct_vehicle_ids

        FROM all_causes

        GROUP BY _source_year

        ORDER BY _source_year
        """
    ).fetchdf()

    print(
        overview.to_string(
            index=False
        )
    )

    # ========================================================
    # QUANTAS LINHAS POR PESSOA
    # ========================================================

    print()
    print("=" * 100)
    print(
        "DISTRIBUIÇÃO DE LINHAS POR "
        "PESSOA/ACIDENTE/VEÍCULO"
    )
    print("=" * 100)

    distribution = con.execute(
        """
        WITH x AS (

            SELECT

                _source_year,
                id,
                pesid,
                id_veiculo,

                COUNT(*) AS rows_per_person

            FROM all_causes

            GROUP BY
                _source_year,
                id,
                pesid,
                id_veiculo
        )

        SELECT

            rows_per_person,

            COUNT(*) AS groups

        FROM x

        GROUP BY rows_per_person

        ORDER BY rows_per_person
        """
    ).fetchdf()

    print(
        distribution.to_string(
            index=False
        )
    )

    # ========================================================
    # CHAVES CANDIDATAS
    # ========================================================

    print()
    print("=" * 100)
    print("TESTE DE CHAVES CANDIDATAS")
    print("=" * 100)

    candidates = {

        "pesid":
            "pesid",

        "id + pesid":
            "id, pesid",

        "id + pesid + id_veiculo":
            """
            id,
            pesid,
            id_veiculo
            """,

        (
            "id + pesid + id_veiculo "
            "+ causa_acidente"
        ):
            """
            id,
            pesid,
            id_veiculo,
            causa_acidente
            """,

        (
            "id + pesid + id_veiculo "
            "+ causa_acidente "
            "+ tipo_acidente"
        ):
            """
            id,
            pesid,
            id_veiculo,
            causa_acidente,
            tipo_acidente
            """,

        (
            "id + pesid + vehicle "
            "+ cause + type + ordem"
        ):
            """
            id,
            pesid,
            id_veiculo,
            causa_acidente,
            tipo_acidente,
            ordem_tipo_acidente
            """,
    }

    for name, columns in candidates.items():

        duplicate_groups = con.execute(
            f"""
            SELECT COUNT(*)

            FROM (

                SELECT
                    {columns},
                    COUNT(*) AS n

                FROM all_causes

                GROUP BY
                    {columns}

                HAVING COUNT(*) > 1
            )
            """
        ).fetchone()[0]

        print(
            f"{name:<70} "
            f"duplicated_groups="
            f"{duplicate_groups:,}"
        )

    # ========================================================
    # CAUSA PRINCIPAL
    # ========================================================

    print()
    print("=" * 100)
    print("CAUSA_PRINCIPAL")
    print("=" * 100)

    main_cause = con.execute(
        """
        SELECT

            causa_principal,

            COUNT(*) AS records

        FROM all_causes

        GROUP BY causa_principal

        ORDER BY records DESC
        """
    ).fetchdf()

    print(
        main_cause.to_string(
            index=False
        )
    )

    # ========================================================
    # ORDEM DO TIPO DE ACIDENTE
    # ========================================================

    print()
    print("=" * 100)
    print("ORDEM_TIPO_ACIDENTE")
    print("=" * 100)

    order_values = con.execute(
        """
        SELECT

            ordem_tipo_acidente,

            COUNT(*) AS records

        FROM all_causes

        GROUP BY ordem_tipo_acidente

        ORDER BY records DESC
        LIMIT 30
        """
    ).fetchdf()

    print(
        order_values.to_string(
            index=False
        )
    )

    # ========================================================
    # EXEMPLO DE UMA PESSOA MULTIPLICADA
    # ========================================================

    print()
    print("=" * 100)
    print(
        "EXEMPLO DE PESSOA COM "
        "MÚLTIPLAS LINHAS"
    )
    print("=" * 100)

    example = con.execute(
        """
        WITH repeated AS (

            SELECT
                _source_year,
                id,
                pesid,
                id_veiculo,

                COUNT(*) AS n

            FROM all_causes

            WHERE
                pesid IS NOT NULL
                AND pesid <> '0'

            GROUP BY
                _source_year,
                id,
                pesid,
                id_veiculo

            HAVING COUNT(*) > 3

            ORDER BY n DESC

            LIMIT 1
        )

        SELECT

            a._source_year,
            a.id,
            a.pesid,
            a.id_veiculo,

            a.causa_principal,
            a.causa_acidente,

            a.ordem_tipo_acidente,
            a.tipo_acidente,

            a.tipo_envolvido,
            a.estado_fisico,
            a.idade,
            a.sexo

        FROM all_causes a

        INNER JOIN repeated r

            ON a._source_year
                = r._source_year

            AND a.id = r.id

            AND a.pesid = r.pesid

            AND
                COALESCE(
                    a.id_veiculo,
                    ''
                )
                =
                COALESCE(
                    r.id_veiculo,
                    ''
                )

        ORDER BY
            a.causa_principal DESC,
            a.causa_acidente,
            a.ordem_tipo_acidente
        """
    ).fetchdf()

    print(
        example.to_string(
            index=False
        )
    )


if __name__ == "__main__":
    main()
