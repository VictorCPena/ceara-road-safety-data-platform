import json
from pathlib import Path
from typing import Any


MANIFEST_PATH = Path(
    "metadata/prf_ingestion_manifest.jsonl"
)


def load_manifest() -> list[dict[str, Any]]:
    """
    Carrega todos os registros anteriores
    do manifest de ingestão.
    """

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


def find_existing_version(
    records: list[dict[str, Any]],
    year: int,
    dataset: str,
    filename: str,
    sha256: str,
):
    """
    Procura se uma versão com o mesmo hash
    já foi armazenada anteriormente.
    """

    for record in reversed(records):
        if (
            record.get("status") == "stored"
            and record.get("year") == year
            and record.get("dataset") == dataset
            and record.get("filename") == filename
            and record.get("sha256") == sha256
        ):
            return record

    return None


def append_manifest(
    record: dict[str, Any]
) -> None:
    """
    Adiciona um novo evento ao manifest.
    """

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
