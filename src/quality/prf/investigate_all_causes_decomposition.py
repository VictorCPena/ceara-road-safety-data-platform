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
        "TESTE DE DECOMPOSIÇÃO - "
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

    # ========================================================
    # CAUSAS DIFERENTES ENTRE PESSOAS DO MESMO ACIDENTE?
    # ========================================================

    print()
    print("=" * 100)
    print(
        "ACIDENTES COM CONJUNTOS DE CAUSAS "
        "DIFERENTES ENTRE PESSOAS"
    )
    print("=" * 100)

    different_cause_sets = con.execute(
        """
        WITH involvement AS (

            SELECT

                _source_year,
                id,
                pesid,
                id_veiculo,

                STRING_AGG(
                    DISTINCT causa_acidente,
                    ' | '
                    ORDER BY causa_acidente
                ) AS cause_signature

            FROM all_causes

            GROUP BY
                _source_year,
                id,
                pesid,
                id_veiculo
        ),

        accident_check AS (

            SELECT

                _source_year,
                id,

                COUNT(
                    DISTINCT cause_signature
                ) AS different_signatures

            FROM involvement

            GROUP BY
                _source_year,
                id
        )

        SELECT COUNT(*)

        FROM accident_check

        WHERE
            different_signatures > 1
        """
    ).fetchone()[0]

    print(
        "Acidentes com conjuntos diferentes "
        f"de causas: {different_cause_sets:,}"
    )

    # ========================================================
    # TIPOS DIFERENTES ENTRE PESSOAS?
    # ========================================================

    print()
    print("=" * 100)
    print(
        "ACIDENTES COM CONJUNTOS DE TIPOS "
        "DIFERENTES ENTRE PESSOAS"
    )
    print("=" * 100)

    different_type_sets = con.execute(
        """
        WITH involvement AS (

            SELECT

                _source_year,
                id,
                pesid,
                id_veiculo,

                STRING_AGG(
                    DISTINCT
                    COALESCE(
                        ordem_tipo_acidente,
                        ''
                    )
                    || ':'
                    ||
                    COALESCE(
                        tipo_acidente,
                        ''
                    ),
                    ' | '
                    ORDER BY
                        COALESCE(
                            ordem_tipo_acidente,
                            ''
                        )
                        || ':'
                        ||
                        COALESCE(
                            tipo_acidente,
                            ''
                        )
                ) AS type_signature

            FROM all_causes

            GROUP BY
                _source_year,
                id,
                pesid,
                id_veiculo
        ),

        accident_check AS (

            SELECT

                _source_year,
                id,

                COUNT(
                    DISTINCT type_signature
                ) AS different_signatures

            FROM involvement

            GROUP BY
                _source_year,
                id
        )

        SELECT COUNT(*)

        FROM accident_check

        WHERE
            different_signatures > 1
        """
    ).fetchone()[0]

    print(
        "Acidentes com conjuntos diferentes "
        f"de tipos: {different_type_sets:,}"
    )

    # ========================================================
    # CAUSA PRINCIPAL CONSISTENTE?
    # ========================================================

    print()
    print("=" * 100)
    print(
        "CONSISTÊNCIA DE CAUSA_PRINCIPAL"
    )
    print("=" * 100)

    inconsistent_primary = con.execute(
        """
        SELECT COUNT(*)

        FROM (

            SELECT

                _source_year,
                id,
                causa_acidente,

                COUNT(
                    DISTINCT causa_principal
                ) AS flags

            FROM all_causes

            GROUP BY
                _source_year,
                id,
                causa_acidente

            HAVING
                COUNT(
                    DISTINCT causa_principal
                ) > 1
        )
        """
    ).fetchone()[0]

    print(
        "Acidente + causa com Sim/Não "
        f"conflitante: {inconsistent_primary:,}"
    )

    # ========================================================
    # ORDEM → TIPO CONSISTENTE?
    # ========================================================

    print()
    print("=" * 100)
    print(
        "CONSISTÊNCIA ORDEM → TIPO"
    )
    print("=" * 100)

    inconsistent_order = con.execute(
        """
        SELECT COUNT(*)

        FROM (

            SELECT

                _source_year,
                id,
                ordem_tipo_acidente,

                COUNT(
                    DISTINCT tipo_acidente
                ) AS types

            FROM all_causes

            GROUP BY
                _source_year,
                id,
                ordem_tipo_acidente

            HAVING
                COUNT(
                    DISTINCT tipo_acidente
                ) > 1
        )
        """
    ).fetchone()[0]

    print(
        "Acidente + ordem apontando "
        f"para múltiplos tipos: {inconsistent_order:,}"
    )

    # ========================================================
    # QUANTOS REGISTROS TERÍAMOS APÓS DECOMPOSIÇÃO?
    # ========================================================

    print()
    print("=" * 100)
    print(
        "TAMANHO DAS TABELAS DECOMPOSTAS"
    )
    print("=" * 100)

    sizes = con.execute(
        """
        SELECT

            (
                SELECT COUNT(*)
                FROM all_causes
            ) AS original_rows,

            (
                SELECT COUNT(*)
                FROM (
                    SELECT DISTINCT
                        _source_year,
                        id,
                        causa_acidente,
                        causa_principal
                    FROM all_causes
                )
            ) AS accident_cause_rows,

            (
                SELECT COUNT(*)
                FROM (
                    SELECT DISTINCT
                        _source_year,
                        id,
                        ordem_tipo_acidente,
                        tipo_acidente
                    FROM all_causes
                )
            ) AS accident_type_rows

        """
    ).fetchdf()

    print(
        sizes.to_string(
            index=False
        )
    )

    # ========================================================
    # NÚMERO DE CAUSAS/TIPOS POR ACIDENTE
    # ========================================================

    print()
    print("=" * 100)
    print(
        "MÉDIA E MÁXIMO POR ACIDENTE"
    )
    print("=" * 100)

    stats = con.execute(
        """
        WITH causes AS (

            SELECT

                _source_year,
                id,

                COUNT(
                    DISTINCT causa_acidente
                ) AS cause_count

            FROM all_causes

            GROUP BY
                _source_year,
                id
        ),

        types AS (

            SELECT

                _source_year,
                id,

                COUNT(
                    DISTINCT
                    ordem_tipo_acidente
                    || ':'
                    ||
                    tipo_acidente
                ) AS type_count

            FROM all_causes

            GROUP BY
                _source_year,
                id
        )

        SELECT

            c._source_year AS year,

            ROUND(
                AVG(c.cause_count),
                2
            ) AS avg_causes,

            MAX(
                c.cause_count
            ) AS max_causes,

            ROUND(
                AVG(t.type_count),
                2
            ) AS avg_types,

            MAX(
                t.type_count
            ) AS max_types

        FROM causes c

        INNER JOIN types t

            ON
                c._source_year
                = t._source_year

            AND
                c.id = t.id

        GROUP BY
            c._source_year

        ORDER BY
            c._source_year
        """
    ).fetchdf()

    print(
        stats.to_string(
            index=False
        )
    )

    # ========================================================
    # EXEMPLOS DE INCONSISTÊNCIA, SE HOUVER
    # ========================================================

    if (
        different_cause_sets == 0
        and
        different_type_sets == 0
        and
        inconsistent_primary == 0
        and
        inconsistent_order == 0
    ):

        print()
        print("=" * 100)
        print(
            "RESULTADO"
        )
        print("=" * 100)

        print(
            "Os dados suportam decomposição "
            "em accident_cause e accident_type."
        )

    else:

        print()
        print("=" * 100)
        print(
            "RESULTADO"
        )
        print("=" * 100)

        print(
            "Existem inconsistências. "
            "Investigar antes de decompor."
        )


if __name__ == "__main__":
    main()
