from pathlib import Path
import shutil

import pyarrow as pa
import pyarrow.parquet as pq


ROOT = Path(__file__).resolve().parents[1]

SILVER_DIR = ROOT / "data" / "silver"
FIXTURE_DIR = ROOT / "tests" / "fixtures" / "silver"

YEARS = [2024, 2025, 2026]
ACCIDENTS_PER_YEAR = 5


def read_parquet(path: Path) -> pa.Table:
    if not path.exists():
        raise FileNotFoundError(f"Arquivo não encontrado: {path}")

    # ParquetFile evita inferência acidental das partições year=...
    return pq.ParquetFile(path).read()


def find_column(table: pa.Table, candidates: list[str]) -> str:
    for column in candidates:
        if column in table.column_names:
            return column

    raise ValueError(
        f"Nenhuma das colunas {candidates} encontrada.\n"
        f"Disponíveis: {table.column_names}"
    )


def normalize(value) -> str | None:
    if value is None:
        return None

    return str(value).strip()


def id_set(table: pa.Table, id_column: str) -> set[str]:
    return {
        normalize(value)
        for value in table[id_column].to_pylist()
        if normalize(value) is not None
    }


def filter_by_ids(
    table: pa.Table,
    id_column: str,
    selected_ids: set[str],
) -> pa.Table:

    mask = [
        normalize(value) in selected_ids
        for value in table[id_column].to_pylist()
    ]

    return table.filter(pa.array(mask))


def write_table(table: pa.Table, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)

    pq.write_table(
        table,
        path,
        compression="snappy",
    )


def copy_full_file(source: Path, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(source, destination)


def select_ce_occurrences(
    occurrence: pa.Table,
) -> tuple[pa.Table, str]:

    accident_column = find_column(
        occurrence,
        ["accident_id", "id"],
    )

    state_column = find_column(
        occurrence,
        ["state", "uf"],
    )

    mask = [
        normalize(value).upper() == "CE"
        if normalize(value) is not None
        else False
        for value in occurrence[state_column].to_pylist()
    ]

    return occurrence.filter(pa.array(mask)), accident_column


def main() -> None:
    print("=" * 70)
    print("CREATING CI FIXTURES")
    print("=" * 70)

    if FIXTURE_DIR.exists():
        shutil.rmtree(FIXTURE_DIR)

    FIXTURE_DIR.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    # IBGE MUNICIPALITY
    #
    # Mantemos os 184 municípios porque existe teste de contrato
    # garantindo exatamente essa cardinalidade.
    # ------------------------------------------------------------------

    municipality_source = (
        SILVER_DIR
        / "ibge"
        / "municipality"
        / "part-000.parquet"
    )

    municipality_target = (
        FIXTURE_DIR
        / "ibge"
        / "municipality"
        / "part-000.parquet"
    )

    copy_full_file(
        municipality_source,
        municipality_target,
    )

    print("IBGE municipality: copied")

    # ------------------------------------------------------------------
    # IBGE POPULATION
    #
    # Mantemos 184 municípios x 3 anos = 552 registros para preservar
    # os testes de contrato do mart.
    # ------------------------------------------------------------------

    for year in YEARS:
        source = (
            SILVER_DIR
            / "ibge"
            / "municipality_population"
            / f"year={year}"
            / "part-000.parquet"
        )

        target = (
            FIXTURE_DIR
            / "ibge"
            / "municipality_population"
            / f"year={year}"
            / "part-000.parquet"
        )

        copy_full_file(source, target)

    print("IBGE population: copied")

    # ------------------------------------------------------------------
    # PRF + OPEN-METEO
    #
    # Escolhemos apenas acidentes presentes simultaneamente em:
    # occurrence
    # accident_cause
    # accident_type
    # weather
    #
    # Isso preserva integridade referencial da fixture.
    # ------------------------------------------------------------------

    total_accidents = 0

    for year in YEARS:

        print()
        print(f"YEAR {year}")
        print("-" * 70)

        paths = {
            "occurrence": (
                SILVER_DIR
                / "prf"
                / "occurrence"
                / f"year={year}"
                / "part-000.parquet"
            ),

            "person": (
                SILVER_DIR
                / "prf"
                / "person"
                / f"year={year}"
                / "part-000.parquet"
            ),

            "accident_cause": (
                SILVER_DIR
                / "prf"
                / "accident_cause"
                / f"year={year}"
                / "part-000.parquet"
            ),

            "accident_type": (
                SILVER_DIR
                / "prf"
                / "accident_type"
                / f"year={year}"
                / "part-000.parquet"
            ),

            "weather": (
                SILVER_DIR
                / "open_meteo"
                / "accident_weather"
                / f"year={year}"
                / "part-000.parquet"
            ),
        }

        occurrence = read_parquet(paths["occurrence"])
        person = read_parquet(paths["person"])
        accident_cause = read_parquet(paths["accident_cause"])
        accident_type = read_parquet(paths["accident_type"])
        weather = read_parquet(paths["weather"])

        ce_occurrence, occurrence_id = select_ce_occurrences(
            occurrence
        )

        person_id = find_column(
            person,
            ["accident_id", "id"],
        )

        cause_id = find_column(
            accident_cause,
            ["accident_id", "id"],
        )

        type_id = find_column(
            accident_type,
            ["accident_id", "id"],
        )

        weather_id = find_column(
            weather,
            ["accident_id", "id"],
        )

        valid_ids = (
            id_set(ce_occurrence, occurrence_id)
            & id_set(accident_cause, cause_id)
            & id_set(accident_type, type_id)
            & id_set(weather, weather_id)
        )

        selected = sorted(valid_ids)[:ACCIDENTS_PER_YEAR]

        if len(selected) < ACCIDENTS_PER_YEAR:
            raise RuntimeError(
                f"{year}: somente {len(selected)} acidentes elegíveis "
                f"para fixture."
            )

        selected_ids = set(selected)

        print(
            f"Selected accidents: "
            f"{', '.join(selected)}"
        )

        fixture_occurrence = filter_by_ids(
            ce_occurrence,
            occurrence_id,
            selected_ids,
        )

        fixture_person = filter_by_ids(
            person,
            person_id,
            selected_ids,
        )

        fixture_cause = filter_by_ids(
            accident_cause,
            cause_id,
            selected_ids,
        )

        fixture_type = filter_by_ids(
            accident_type,
            type_id,
            selected_ids,
        )

        fixture_weather = filter_by_ids(
            weather,
            weather_id,
            selected_ids,
        )

        outputs = [
            (
                fixture_occurrence,
                FIXTURE_DIR
                / "prf"
                / "occurrence"
                / f"year={year}"
                / "part-000.parquet",
            ),
            (
                fixture_person,
                FIXTURE_DIR
                / "prf"
                / "person"
                / f"year={year}"
                / "part-000.parquet",
            ),
            (
                fixture_cause,
                FIXTURE_DIR
                / "prf"
                / "accident_cause"
                / f"year={year}"
                / "part-000.parquet",
            ),
            (
                fixture_type,
                FIXTURE_DIR
                / "prf"
                / "accident_type"
                / f"year={year}"
                / "part-000.parquet",
            ),
            (
                fixture_weather,
                FIXTURE_DIR
                / "open_meteo"
                / "accident_weather"
                / f"year={year}"
                / "part-000.parquet",
            ),
        ]

        for table, destination in outputs:
            write_table(table, destination)

        print(
            f"occurrence:      {fixture_occurrence.num_rows}"
        )
        print(
            f"person:          {fixture_person.num_rows}"
        )
        print(
            f"accident_cause:  {fixture_cause.num_rows}"
        )
        print(
            f"accident_type:   {fixture_type.num_rows}"
        )
        print(
            f"weather:         {fixture_weather.num_rows}"
        )

        total_accidents += fixture_occurrence.num_rows

    print()
    print("=" * 70)
    print("FIXTURES CREATED SUCCESSFULLY")
    print("=" * 70)
    print(f"Accidents: {total_accidents}")
    print(f"Output: {FIXTURE_DIR.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
