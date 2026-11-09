import argparse
from pathlib import Path
from lakehouse.core import ingest
from lakehouse.fetch import download
import zipfile


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--csv", type=Path)
    parser.add_argument("--root", type=Path, default=Path("work/lakehouse"))
    args = parser.parse_args()
    csv = args.csv
    if csv is None:
        path = Path("data/raw/prf2024.zip")
        download("https://drive.google.com/uc?export=download&id=14lB0vqMFkaZj8HZ44b0njYgxs9nAN8KO", path)
        with zipfile.ZipFile(path) as archive:
            info = next(i for i in archive.infolist() if i.filename.endswith(".csv"))
            csv = path.parent / "datatran2024.csv"
            with archive.open(info) as source, csv.open("wb") as target:
                import shutil

                shutil.copyfileobj(source, target)
    ingest(csv, args.root)


if __name__ == "__main__":
    main()
