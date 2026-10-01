from pathlib import Path
from datetime import datetime, timedelta, timezone
import json
import time
import uuid

import pandas as pd
import requests


OCCURRENCE_PATH = Path(
    "data/silver/prf/occurrence"
)

BRONZE_PATH = Path(
    "data/bronze/open_meteo/"
    "dataset=accident_weather"
)

API_URL = (
    "https://historical-forecast-api.open-meteo.com/"
    "v1/forecast"
)

TIMEZONE = "America/Fortaleza"

BATCH_SIZE = 25

HOURLY_VARIABLES = [
    "temperature_2m",
    "relative_humidity_2m",
    "precipitation",
    "rain",
    "weather_code",
    "cloud_cover",
    "visibility",
    "wind_speed_10m",
    "wind_gusts_10m",
]


def load_occurrences():

    files = sorted(
        OCCURRENCE_PATH.glob(
            "year=*/part-000.parquet"
        )
    )

    if not files:

        raise FileNotFoundError(
            "Occurrence Silver não encontrada."
        )

    frames = []

    for file in files:

        df = pd.read_parquet(
            file,
            columns=[
                "accident_id",
                "event_timestamp",
                "state",
                "latitude",
                "longitude",
                "_source_year",
            ],
        )

        frames.append(df)

    df = pd.concat(
        frames,
        ignore_index=True,
    )

    df = df.loc[
        df["state"] == "CE"
    ].copy()

    df = df.loc[
        df["latitude"].notna()
        &
        df["longitude"].notna()
        &
        df["event_timestamp"].notna()
    ].copy()

    df["accident_key"] = (
        df["_source_year"].astype(str)
        + "|"
        + df["accident_id"].astype(str)
    )

    return (
        df
        .drop_duplicates(
            subset=[
                "accident_key"
            ]
        )
        .reset_index(drop=True)
    )


def load_existing_keys():

    keys = set()

    for file in BRONZE_PATH.glob(
        "snapshot=*/part-*.jsonl"
    ):

        with file.open(
            "r",
            encoding="utf-8",
        ) as handle:

            for line in handle:

                line = line.strip()

                if not line:
                    continue

                record = json.loads(
                    line
                )

                keys.add(
                    record[
                        "accident_key"
                    ]
                )

    return keys


def chunks(
    frame,
    size,
):

    for start in range(
        0,
        len(frame),
        size,
    ):

        yield frame.iloc[
            start:start + size
        ]


def request_batch(
    session,
    batch,
    date,
):

    latitude = ",".join(
        batch[
            "latitude"
        ]
        .astype(str)
        .tolist()
    )

    longitude = ",".join(
        batch[
            "longitude"
        ]
        .astype(str)
        .tolist()
    )

    start_date = (
        pd.Timestamp(date)
        .date()
    )

    # Um dia adicional permite buscar
    # a hora mais próxima também para
    # acidentes perto da meia-noite.
    end_date = (
        start_date
        + timedelta(days=1)
    )

    params = {

        "latitude":
            latitude,

        "longitude":
            longitude,

        "start_date":
            start_date.isoformat(),

        "end_date":
            end_date.isoformat(),

        "hourly":
            ",".join(
                HOURLY_VARIABLES
            ),

        "timezone":
            TIMEZONE,

        "temperature_unit":
            "celsius",

        "wind_speed_unit":
            "kmh",

        "precipitation_unit":
            "mm",

        "cell_selection":
            "land",
    }

    last_error = None

    for attempt in range(4):

        try:

            response = session.get(
                API_URL,
                params=params,
                timeout=120,
            )

            if response.status_code == 429:

                time.sleep(
                    2 ** (attempt + 1)
                )

                continue

            response.raise_for_status()

            data = response.json()

            if isinstance(
                data,
                dict,
            ):

                data = [data]

            if len(data) != len(batch):

                raise RuntimeError(
                    "Quantidade de respostas "
                    "diferente da quantidade "
                    "de coordenadas solicitadas."
                )

            return (
                data,
                params,
            )

        except (
            requests.RequestException,
            ValueError,
            RuntimeError,
        ) as exc:

            last_error = exc

            time.sleep(
                2 ** attempt
            )

    raise RuntimeError(
        "Falha após múltiplas tentativas "
        "no Open-Meteo."
    ) from last_error


def main():

    print()
    print("=" * 90)
    print(
        "INGESTÃO OPEN-METEO - "
        "CLIMA DOS ACIDENTES"
    )
    print("=" * 90)

    accidents = (
        load_occurrences()
    )

    existing_keys = (
        load_existing_keys()
    )

    pending = accidents.loc[
        ~accidents[
            "accident_key"
        ].isin(
            existing_keys
        )
    ].copy()

    print()
    print(
        f"Acidentes CE elegíveis: "
        f"{len(accidents):,}"
    )

    print(
        f"Já existentes no Bronze: "
        f"{len(existing_keys):,}"
    )

    print(
        f"Pendentes: "
        f"{len(pending):,}"
    )

    if pending.empty:

        print()
        print(
            "Nenhuma chamada necessária."
        )

        return

    run_id = uuid.uuid4().hex[:12]

    ingested_at = datetime.now(
        timezone.utc
    )

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
        / "part-000.jsonl"
    )

    pending[
        "event_date"
    ] = (
        pd.to_datetime(
            pending[
                "event_timestamp"
            ]
        )
        .dt.date
    )

    session = requests.Session()

    session.headers.update(
        {
            "User-Agent":
                "ceara-road-safety-data-platform/1.0"
        }
    )

    written = 0

    with output_file.open(
        "a",
        encoding="utf-8",
    ) as handle:

        grouped = pending.groupby(
            "event_date",
            sort=True,
        )

        total_dates = (
            pending[
                "event_date"
            ]
            .nunique()
        )

        for date_number, (
            date,
            date_frame,
        ) in enumerate(
            grouped,
            start=1,
        ):

            print(
                f"[{date_number}/{total_dates}] "
                f"{date} - "
                f"{len(date_frame):,} acidentes"
            )

            for batch in chunks(
                date_frame,
                BATCH_SIZE,
            ):

                responses, params = (
                    request_batch(
                        session=session,
                        batch=batch,
                        date=date,
                    )
                )

                for (
                    (_, accident),
                    weather_response,
                ) in zip(
                    batch.iterrows(),
                    responses,
                ):

                    record = {

                        "run_id":
                            run_id,

                        "accident_key":
                            accident[
                                "accident_key"
                            ],

                        "accident_id":
                            int(
                                accident[
                                    "accident_id"
                                ]
                            ),

                        "source_year":
                            int(
                                accident[
                                    "_source_year"
                                ]
                            ),

                        "event_timestamp":
                            pd.Timestamp(
                                accident[
                                    "event_timestamp"
                                ]
                            ).isoformat(),

                        "requested_latitude":
                            float(
                                accident[
                                    "latitude"
                                ]
                            ),

                        "requested_longitude":
                            float(
                                accident[
                                    "longitude"
                                ]
                            ),

                        "requested_start_date":
                            params[
                                "start_date"
                            ],

                        "requested_end_date":
                            params[
                                "end_date"
                            ],

                        "timezone":
                            TIMEZONE,

                        "source_url":
                            API_URL,

                        "ingested_at":
                            ingested_at.isoformat(),

                        "payload":
                            weather_response,
                    }

                    handle.write(
                        json.dumps(
                            record,
                            ensure_ascii=False,
                        )
                        + "\n"
                    )

                    written += 1

            # pequena pausa entre dias
            time.sleep(0.05)

    print()
    print(
        f"Registros armazenados: "
        f"{written:,}"
    )

    print(
        f"Snapshot: {output_file}"
    )

    print()
    print(
        "Ingestão concluída."
    )


if __name__ == "__main__":
    main()
