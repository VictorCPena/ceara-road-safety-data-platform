from pathlib import Path
import json

import pandas as pd


BRONZE_PATH = Path(
    "data/bronze/open_meteo/"
    "dataset=accident_weather"
)

SILVER_PATH = Path(
    "data/silver/open_meteo/"
    "accident_weather"
)


VARIABLES = [
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


def load_bronze():

    records = []

    files = sorted(
        BRONZE_PATH.glob(
            "snapshot=*/part-*.jsonl"
        )
    )

    if not files:

        raise FileNotFoundError(
            "Nenhum Bronze Open-Meteo encontrado."
        )

    for file in files:

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

                record[
                    "_bronze_snapshot"
                ] = (
                    file.parent.name
                    .replace(
                        "snapshot=",
                        "",
                    )
                )

                record[
                    "_bronze_file"
                ] = file.name

                records.append(
                    record
                )

    return records


def nearest_hour(
    event_timestamp,
    hourly,
):

    times = pd.to_datetime(
        hourly[
            "time"
        ],
        errors="coerce",
    )

    event_time = pd.Timestamp(
        event_timestamp
    )

    differences = (
        times
        - event_time
    ).to_series(
        index=range(
            len(times)
        )
    )

    index = (
        differences
        .abs()
        .idxmin()
    )

    weather_time = (
        times[index]
    )

    offset_minutes = (
        weather_time
        - event_time
    ).total_seconds() / 60

    return (
        index,
        weather_time,
        offset_minutes,
    )


def main():

    print()
    print("=" * 90)
    print(
        "OPEN-METEO ACCIDENT WEATHER "
        "- BRONZE → SILVER"
    )
    print("=" * 90)

    records = load_bronze()

    rows = []

    for record in records:

        payload = (
            record[
                "payload"
            ]
        )

        hourly = (
            payload.get(
                "hourly"
            )
            or {}
        )

        if not hourly.get(
            "time"
        ):

            continue

        (
            index,
            weather_time,
            offset_minutes,
        ) = nearest_hour(
            record[
                "event_timestamp"
            ],
            hourly,
        )

        row = {

            "accident_key":
                record[
                    "accident_key"
                ],

            "accident_id":
                record[
                    "accident_id"
                ],

            "source_year":
                record[
                    "source_year"
                ],

            "event_timestamp":
                pd.Timestamp(
                    record[
                        "event_timestamp"
                    ]
                ),

            "weather_timestamp":
                weather_time,

            "weather_time_offset_minutes":
                float(
                    offset_minutes
                ),

            "requested_latitude":
                record[
                    "requested_latitude"
                ],

            "requested_longitude":
                record[
                    "requested_longitude"
                ],

            "weather_grid_latitude":
                payload.get(
                    "latitude"
                ),

            "weather_grid_longitude":
                payload.get(
                    "longitude"
                ),

            "weather_grid_elevation":
                payload.get(
                    "elevation"
                ),

            "weather_timezone":
                payload.get(
                    "timezone"
                ),

            "_source_url":
                record[
                    "source_url"
                ],

            "_source_ingested_at":
                record[
                    "ingested_at"
                ],

            "_source_snapshot":
                record[
                    "_bronze_snapshot"
                ],

            "_source_file":
                record[
                    "_bronze_file"
                ],
        }

        for variable in VARIABLES:

            values = hourly.get(
                variable
            )

            row[variable] = (
                values[index]
                if (
                    isinstance(
                        values,
                        list,
                    )
                    and index
                    < len(values)
                )
                else None
            )

        rows.append(row)

    df = pd.DataFrame(
        rows
    )

    # Se houver reprocessamento futuro,
    # o snapshot mais recente vence.
    df = (
        df
        .drop_duplicates(
            subset=[
                "accident_key"
            ],
            keep="last",
        )
        .reset_index(
            drop=True
        )
    )

    df[
        "weather_code"
    ] = pd.to_numeric(
        df[
            "weather_code"
        ],
        errors="coerce",
    ).astype("Int64")

    for year in sorted(
        df[
            "source_year"
        ]
        .dropna()
        .unique()
    ):

        year_df = (
            df.loc[
                df[
                    "source_year"
                ]
                == year
            ]
            .copy()
        )

        output_file = (
            SILVER_PATH
            / f"year={int(year)}"
            / "part-000.parquet"
        )

        output_file.parent.mkdir(
            parents=True,
            exist_ok=True,
        )

        year_df.to_parquet(
            output_file,
            index=False,
            engine="pyarrow",
            compression="snappy",
        )

        print(
            f"{year}: "
            f"{len(year_df):,} "
            f"→ {output_file}"
        )

    print()
    print(
        f"Total Silver: "
        f"{len(df):,}"
    )


if __name__ == "__main__":
    main()
