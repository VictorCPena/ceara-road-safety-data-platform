from __future__ import annotations

import json
import subprocess
import tempfile
import urllib.error
import urllib.request
from pathlib import Path


OUTPUT = Path("dashboard/assets/ceara_municipalities.geojson")

IBGE_MESH_URLS = [
    (
        "https://servicodados.ibge.gov.br/api/v3/malhas/estados/23"
        "?intrarregiao=municipio"
        "&formato=application/vnd.geo+json"
        "&qualidade=minima"
    ),
    (
        "https://servicodados.ibge.gov.br/api/v4/malhas/estados/23"
        "?intrarregiao=municipio"
        "&formato=application/vnd.geo+json"
        "&qualidade=minima"
    ),
]

# Fallback mirror. The repository declares IBGE as its data source.
FALLBACK_URL = (
    "https://raw.githubusercontent.com/tbrugz/geodata-br/"
    "master/geojson/geojs-23-mun.json"
)


def fetch_json_urllib(url: str) -> dict:
    request = urllib.request.Request(
        url,
        headers={
            "User-Agent": "ceara-road-safety-data-platform/1.0"
        },
    )

    with urllib.request.urlopen(
        request,
        timeout=60,
    ) as response:
        return json.load(response)


def fetch_json_curl(url: str) -> dict:
    """
    Uses macOS/system curl as a fallback.

    This does NOT disable TLS certificate verification.
    """
    with tempfile.NamedTemporaryFile(
        suffix=".json",
        delete=False,
    ) as tmp:
        temp_path = Path(tmp.name)

    try:
        subprocess.run(
            [
                "curl",
                "--fail",
                "--location",
                "--silent",
                "--show-error",
                "--connect-timeout",
                "20",
                "--max-time",
                "90",
                "--output",
                str(temp_path),
                url,
            ],
            check=True,
        )

        return json.loads(
            temp_path.read_text(encoding="utf-8")
        )

    finally:
        temp_path.unlink(missing_ok=True)


def validate_feature_collection(data: dict) -> bool:
    return (
        isinstance(data, dict)
        and data.get("type") == "FeatureCollection"
        and isinstance(data.get("features"), list)
        and len(data["features"]) > 0
    )


def fetch_mesh() -> tuple[dict, str]:
    errors = []

    # 1. Official IBGE API through Python HTTPS.
    for url in IBGE_MESH_URLS:
        try:
            data = fetch_json_urllib(url)

            if validate_feature_collection(data):
                return data, "IBGE API"

            errors.append(
                f"IBGE: resposta inválida em {url}"
            )

        except Exception as exc:
            errors.append(
                f"IBGE: {type(exc).__name__}: {exc}"
            )

    # 2. Mirror through system curl.
    try:
        data = fetch_json_curl(FALLBACK_URL)

        if validate_feature_collection(data):
            return data, "geodata-br (fonte declarada: IBGE)"

        errors.append(
            "Fallback curl: resposta não é FeatureCollection"
        )

    except Exception as exc:
        errors.append(
            f"Fallback curl: {type(exc).__name__}: {exc}"
        )

    # 3. Mirror through urllib, in case curl is unavailable.
    try:
        data = fetch_json_urllib(FALLBACK_URL)

        if validate_feature_collection(data):
            return data, "geodata-br (fonte declarada: IBGE)"

        errors.append(
            "Fallback urllib: resposta não é FeatureCollection"
        )

    except Exception as exc:
        errors.append(
            f"Fallback urllib: {type(exc).__name__}: {exc}"
        )

    raise RuntimeError(
        "Não foi possível obter a malha municipal do Ceará.\n\n"
        + "\n".join(errors)
    )


def extract_code(properties: dict) -> str | None:
    value = (
        properties.get("ibge_code")
        or properties.get("codarea")
        or properties.get("CD_MUN")
        or properties.get("CD_GEOCMU")
        or properties.get("id")
    )

    if value is None:
        return None

    return str(value).strip()


def extract_name(properties: dict) -> str | None:
    value = (
        properties.get("municipality")
        or properties.get("nome")
        or properties.get("name")
        or properties.get("NM_MUN")
    )

    if value is None:
        return None

    return str(value).strip()


def normalize_mesh(mesh: dict) -> dict:
    normalized = []
    seen_codes = set()

    for feature in mesh["features"]:
        properties = feature.get("properties", {})

        code = extract_code(properties)
        name = extract_name(properties)

        if not code:
            raise RuntimeError(
                f"Feição sem código IBGE: {properties}"
            )

        if not name:
            raise RuntimeError(
                f"Feição sem nome municipal: {properties}"
            )

        if code in seen_codes:
            raise RuntimeError(
                f"Código IBGE duplicado no GeoJSON: {code}"
            )

        seen_codes.add(code)

        normalized.append(
            {
                "type": "Feature",
                "properties": {
                    "ibge_code": code,
                    "municipality": name,
                },
                "geometry": feature["geometry"],
            }
        )

    if len(normalized) != 184:
        raise RuntimeError(
            "Quantidade inesperada de municípios: "
            f"{len(normalized)} (esperado: 184)"
        )

    return {
        "type": "FeatureCollection",
        "features": normalized,
    }


def main() -> None:
    mesh, source = fetch_mesh()
    output = normalize_mesh(mesh)

    OUTPUT.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    OUTPUT.write_text(
        json.dumps(
            output,
            ensure_ascii=False,
            separators=(",", ":"),
        ),
        encoding="utf-8",
    )

    size_kb = OUTPUT.stat().st_size / 1024

    print("OK")
    print(f"Fonte usada: {source}")
    print(f"Arquivo: {OUTPUT}")
    print(f"Municípios: {len(output['features'])}")
    print(f"Tamanho: {size_kb:.1f} KB")
    print()
    print(
        "Agora pode rodar: "
        "streamlit run dashboard/app.py"
    )


if __name__ == "__main__":
    main()
