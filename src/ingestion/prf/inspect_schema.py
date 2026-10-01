import csv
import io
import json
import zipfile
from pathlib import Path


BRONZE_PATH = Path("data/bronze/prf")

REPORT_PATH = Path(
    "metadata/prf_schema_report.json"
)


def get_latest_snapshot(
    year: int,
    dataset: str,
) -> Path:
    """
    Retorna o snapshot mais recente
    de um ano + dataset.
    """

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
            f"Nenhum snapshot encontrado em {dataset_path}"
        )

    return snapshots[-1]


def find_zip_file(snapshot_path: Path) -> Path:
    """
    Procura o arquivo ZIP dentro do snapshot.
    """

    zip_files = list(
        snapshot_path.glob("*.zip")
    )

    if not zip_files:
        raise FileNotFoundError(
            f"Nenhum ZIP encontrado em {snapshot_path}"
        )

    if len(zip_files) > 1:
        raise RuntimeError(
            f"Mais de um ZIP encontrado em {snapshot_path}"
        )

    return zip_files[0]


def decode_sample(raw_bytes: bytes) -> tuple[str, str]:
    """
    Tenta descobrir uma codificação utilizável.
    """

    encodings = [
        "utf-8-sig",
        "utf-8",
        "latin-1",
        "cp1252",
    ]

    for encoding in encodings:
        try:
            return (
                raw_bytes.decode(encoding),
                encoding,
            )
        except UnicodeDecodeError:
            continue

    raise UnicodeDecodeError(
        "unknown",
        b"",
        0,
        1,
        "Não foi possível decodificar o arquivo.",
    )


def detect_delimiter(text: str) -> str:
    """
    Tenta identificar o delimitador do CSV.
    """

    try:
        dialect = csv.Sniffer().sniff(
            text,
            delimiters=";,|\t,"
        )

        return dialect.delimiter

    except csv.Error:
        return ";"


def inspect_zip(
    zip_path: Path,
) -> list[dict]:
    """
    Inspeciona os CSVs existentes dentro
    de um arquivo ZIP sem extrair tudo.
    """

    results = []

    with zipfile.ZipFile(zip_path, "r") as archive:

        members = [
            name
            for name in archive.namelist()
            if name.lower().endswith(".csv")
        ]

        for member in members:

            with archive.open(member) as file:

                raw_sample = file.read(
                    100_000
                )

            text, encoding = decode_sample(
                raw_sample
            )

            delimiter = detect_delimiter(
                text
            )

            reader = csv.reader(
                io.StringIO(text),
                delimiter=delimiter,
            )

            try:
                header = next(reader)
            except StopIteration:
                header = []

            header = [
                column.strip()
                for column in header
            ]

            results.append(
                {
                    "zip_file": zip_path.name,
                    "csv_file": member,
                    "encoding": encoding,
                    "delimiter": delimiter,
                    "column_count": len(header),
                    "columns": header,
                }
            )

    return results


def main():

    years = [
        2024,
        2025,
        2026,
    ]

    datasets = [
        "occurrence",
        "person",
        "person_all_causes",
    ]

    report = []

    for year in years:

        for dataset in datasets:

            print()
            print("=" * 70)
            print(f"YEAR: {year}")
            print(f"DATASET: {dataset}")

            snapshot = get_latest_snapshot(
                year=year,
                dataset=dataset,
            )

            zip_file = find_zip_file(
                snapshot
            )

            print(
                f"ZIP: {zip_file}"
            )

            files = inspect_zip(
                zip_file
            )

            for file_info in files:

                print()
                print(
                    f"CSV: {file_info['csv_file']}"
                )
                print(
                    f"Encoding: {file_info['encoding']}"
                )
                print(
                    f"Delimiter: {repr(file_info['delimiter'])}"
                )
                print(
                    f"Colunas: {file_info['column_count']}"
                )

                print(
                    file_info["columns"]
                )

                report.append(
                    {
                        "year": year,
                        "dataset": dataset,
                        "snapshot": snapshot.name,
                        **file_info,
                    }
                )

    REPORT_PATH.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    with REPORT_PATH.open(
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
        f"Relatório salvo em: {REPORT_PATH}"
    )


if __name__ == "__main__":
    main()
