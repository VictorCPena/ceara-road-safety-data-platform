from datetime import datetime, timezone
from pathlib import Path

import hashlib
import shutil
import tempfile
import uuid

import gdown

from src.ingestion.prf.manifest import (
    append_manifest,
    find_existing_version,
    load_manifest,
)
from src.ingestion.prf.sources import PRF_SOURCES


BRONZE_PATH = Path("data/bronze/prf")


def calculate_sha256(
    file_path: Path,
    chunk_size: int = 1024 * 1024,
) -> str:
    """
    Calcula o SHA-256 de um arquivo.

    O arquivo é lido em blocos para evitar
    carregá-lo inteiro na memória.
    """

    sha256 = hashlib.sha256()

    with file_path.open("rb") as file:
        while chunk := file.read(chunk_size):
            sha256.update(chunk)

    return sha256.hexdigest()


def ingest_source(
    source: dict,
    run_id: str,
    manifest_records: list,
) -> None:

    year = source["year"]
    dataset = source["dataset"]
    file_id = source["file_id"]
    filename = source["filename"]

    print()
    print("=" * 70)
    print(f"Ano:     {year}")
    print(f"Dataset: {dataset}")
    print(f"Arquivo: {filename}")
    print("=" * 70)

    # Diretório temporário.
    #
    # Primeiro baixamos o arquivo aqui.
    # Só depois de validá-lo ele entra
    # oficialmente na Bronze.
    with tempfile.TemporaryDirectory() as temp_dir:

        temp_path = Path(temp_dir) / filename

        print("Baixando arquivo temporário...")

        result = gdown.download(
            id=file_id,
            output=str(temp_path),
            quiet=False,
        )

        if result is None:
            raise RuntimeError(
                f"Erro ao baixar {filename}"
            )

        print("Calculando SHA-256...")

        sha256 = calculate_sha256(temp_path)

        file_size = temp_path.stat().st_size

        print(f"SHA-256: {sha256}")
        print(f"Tamanho: {file_size:,} bytes")

        existing = find_existing_version(
            records=manifest_records,
            year=year,
            dataset=dataset,
            filename=filename,
            sha256=sha256,
        )

        ingestion_time = datetime.now(
            timezone.utc
        )

        # ==================================
        # Arquivo já conhecido
        # ==================================

        if existing:

            print()
            print(
                "SKIP: conteúdo já ingerido."
            )

            record = {
                "run_id": run_id,
                "source": "prf",
                "year": year,
                "dataset": dataset,
                "filename": filename,
                "source_file_id": file_id,
                "sha256": sha256,
                "bytes": file_size,
                "ingested_at": ingestion_time.isoformat(),
                "status": "skipped_unchanged",
                "matched_bronze_path": (
                    existing.get("bronze_path")
                ),
            }

            append_manifest(record)

            manifest_records.append(record)

            return

        # ==================================
        # Nova versão encontrada
        # ==================================

        snapshot = ingestion_time.strftime(
            "%Y%m%dT%H%M%SZ"
        )

        destination_dir = (
            BRONZE_PATH
            / f"year={year}"
            / f"dataset={dataset}"
            / f"snapshot={snapshot}"
        )

        destination_dir.mkdir(
            parents=True,
            exist_ok=True,
        )

        destination_file = (
            destination_dir / filename
        )

        shutil.move(
            str(temp_path),
            str(destination_file),
        )

        print()
        print("Nova versão detectada.")
        print(
            f"Armazenado em: {destination_file}"
        )

        record = {
            "run_id": run_id,
            "source": "prf",
            "year": year,
            "dataset": dataset,
            "filename": filename,
            "source_file_id": file_id,
            "sha256": sha256,
            "bytes": file_size,
            "ingested_at": ingestion_time.isoformat(),
            "status": "stored",
            "bronze_path": str(
                destination_file
            ),
        }

        append_manifest(record)

        manifest_records.append(record)


def main():

    run_id = uuid.uuid4().hex[:12]

    print()
    print("Iniciando ingestão da PRF")
    print(f"Run ID: {run_id}")
    print(
        f"Total de fontes: {len(PRF_SOURCES)}"
    )

    manifest_records = load_manifest()

    for source in PRF_SOURCES:
        ingest_source(
            source=source,
            run_id=run_id,
            manifest_records=manifest_records,
        )

    print()
    print("=" * 70)
    print("Ingestão finalizada.")
    print(f"Run ID: {run_id}")
    print("=" * 70)


if __name__ == "__main__":
    main()
    
