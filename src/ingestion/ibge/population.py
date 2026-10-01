from pathlib import Path
from datetime import datetime, timezone
import hashlib
import json
import uuid

import requests


YEARS = [
    2024,
    2025,
    2026,
]

SOURCE_URL = (
    "https://apisidra.ibge.gov.br/"
    "values/t/6579/"
    "n6/in%20n3%2023/"
    "p/2024-2026/"
    "v/9324"
    "?formato=json"
)

BRONZE_PATH = Path(
    "data/bronze/ibge/"
    "dataset=municipality_population"
)

MANIFEST_PATH = Path(
    "metadata/ibge_ingestion_manifest.jsonl"
)

FILENAME = (
    "population_ce_2024_2026.json"
)


def sha256_bytes(
    content: bytes,
) -> str:

    return hashlib.sha256(
        content
    ).hexdigest()


def load_manifest():

    if not MANIFEST_PATH.exists():
        return []

    records = []

    with MANIFEST_PATH.open(
        "r",
        encoding="utf-8",
    ) as file:

        for line in file:

            line = line.strip()

            if line:

                records.append(
                    json.loads(line)
                )

    return records


def append_manifest(
    record: dict,
):

    MANIFEST_PATH.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    with MANIFEST_PATH.open(
        "a",
        encoding="utf-8",
    ) as file:

        file.write(
            json.dumps(
                record,
                ensure_ascii=False,
            )
            + "\n"
        )


def find_existing_hash(
    checksum: str,
):

    for record in load_manifest():

        if (
            record.get("dataset")
            == "municipality_population"
            and
            record.get("sha256")
            == checksum
            and
            record.get("status")
            == "stored"
        ):

            return record

    return None


def main():

    print()
    print("=" * 90)
    print(
        "INGESTÃO IBGE - "
        "POPULAÇÃO MUNICIPAL DO CEARÁ"
    )
    print("=" * 90)

    run_id = uuid.uuid4().hex[:12]

    print()
    print(
        f"Run ID: {run_id}"
    )

    print(
        f"Fonte: {SOURCE_URL}"
    )

    response = requests.get(
        SOURCE_URL,
        timeout=60,
        headers={
            "User-Agent":
                "ceara-road-safety-data-platform/1.0"
        },
    )

    response.raise_for_status()

    content = response.content

    try:

        data = response.json()

    except ValueError as exc:

        raise RuntimeError(
            "SIDRA retornou conteúdo "
            "que não é JSON."
        ) from exc

    if (
        not isinstance(data, list)
        or len(data) <= 1
    ):

        raise RuntimeError(
            "Resposta inesperada "
            "da API SIDRA."
        )

    # Primeira linha da API SIDRA
    # contém o cabeçalho.
    records = len(data) - 1

    checksum = sha256_bytes(
        content
    )

    print()
    print(
        f"Bytes recebidos: "
        f"{len(content):,}"
    )

    print(
        f"SHA-256: {checksum}"
    )

    print(
        f"Registros recebidos: "
        f"{records:,}"
    )

    existing = find_existing_hash(
        checksum
    )

    ingested_at = datetime.now(
        timezone.utc
    )

    if existing:

        print()
        print(
            "Nenhuma alteração detectada."
        )

        print(
            "Snapshot idêntico já existe:"
        )

        print(
            existing[
                "bronze_path"
            ]
        )

        append_manifest(
            {
                "run_id": run_id,
                "source": "IBGE_SIDRA",
                "dataset":
                    "municipality_population",
                "state": "CE",
                "years": YEARS,
                "source_url": SOURCE_URL,
                "filename": FILENAME,
                "sha256": checksum,
                "bytes": len(content),
                "records": records,
                "ingested_at":
                    ingested_at.isoformat(),
                "status":
                    "skipped_unchanged",
                "bronze_path":
                    existing[
                        "bronze_path"
                    ],
            }
        )

        return

    snapshot = (
        ingested_at
        .strftime(
            "%Y%m%dT%H%M%SZ"
        )
    )

    snapshot_path = (
        BRONZE_PATH
        / f"snapshot={snapshot}"
    )

    snapshot_path.mkdir(
        parents=True,
        exist_ok=True,
    )

    output_file = (
        snapshot_path
        / FILENAME
    )

    output_file.write_bytes(
        content
    )

    append_manifest(
        {
            "run_id": run_id,
            "source": "IBGE_SIDRA",
            "dataset":
                "municipality_population",
            "state": "CE",
            "years": YEARS,
            "source_url": SOURCE_URL,
            "filename": FILENAME,
            "sha256": checksum,
            "bytes": len(content),
            "records": records,
            "ingested_at":
                ingested_at.isoformat(),
            "status": "stored",
            "bronze_path":
                str(output_file),
        }
    )

    print()
    print(
        "Snapshot armazenado:"
    )

    print(
        output_file
    )

    print()
    print(
        "Ingestão concluída."
    )


if __name__ == "__main__":
    main()
