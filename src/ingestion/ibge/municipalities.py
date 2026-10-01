from pathlib import Path
from datetime import datetime, timezone
import hashlib
import json
import uuid

import requests


SOURCE_URL = (
    "https://servicodados.ibge.gov.br/"
    "api/v1/localidades/estados/23/municipios"
    "?orderBy=nome"
)

BRONZE_PATH = Path(
    "data/bronze/ibge/dataset=municipality"
)

MANIFEST_PATH = Path(
    "metadata/ibge_ingestion_manifest.jsonl"
)

FILENAME = "municipalities_ce.json"


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

    manifest = load_manifest()

    for record in manifest:

        if (
            record.get("sha256")
            == checksum
            and record.get("status")
            == "stored"
        ):

            return record

    return None


def main():

    print()
    print("=" * 80)
    print(
        "INGESTÃO IBGE - MUNICÍPIOS DO CEARÁ"
    )
    print("=" * 80)

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

    checksum = sha256_bytes(
        content
    )

    print()
    print(
        f"Bytes recebidos: {len(content):,}"
    )

    print(
        f"SHA-256: {checksum}"
    )

    # Valida se a resposta é JSON antes
    # de persistir no Bronze.
    try:

        data = response.json()

    except ValueError as exc:

        raise RuntimeError(
            "IBGE retornou conteúdo que não é JSON."
        ) from exc

    if not isinstance(data, list):

        raise RuntimeError(
            "Estrutura inesperada da API do IBGE."
        )

    print(
        f"Municípios recebidos: {len(data):,}"
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
            existing["bronze_path"]
        )

        append_manifest(
            {
                "run_id": run_id,
                "source": "IBGE",
                "dataset": "municipality",
                "state": "CE",
                "source_url": SOURCE_URL,
                "filename": FILENAME,
                "sha256": checksum,
                "bytes": len(content),
                "records": len(data),
                "ingested_at": (
                    ingested_at.isoformat()
                ),
                "status": "skipped_unchanged",
                "bronze_path": (
                    existing["bronze_path"]
                ),
            }
        )

        return

    snapshot = (
        ingested_at
        .strftime("%Y%m%dT%H%M%SZ")
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
            "source": "IBGE",
            "dataset": "municipality",
            "state": "CE",
            "source_url": SOURCE_URL,
            "filename": FILENAME,
            "sha256": checksum,
            "bytes": len(content),
            "records": len(data),
            "ingested_at": (
                ingested_at.isoformat()
            ),
            "status": "stored",
            "bronze_path": str(
                output_file
            ),
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
