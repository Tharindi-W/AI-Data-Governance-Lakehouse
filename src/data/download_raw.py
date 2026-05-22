"""Download Telecom Italia Milan CDR dataset from Harvard Dataverse.

Dataset: Telecom Italia Big Data Challenge 2014
DOI:     https://doi.org/10.7910/DVN/EGZHFV
License: Creative Commons Attribution 4.0 (CC BY 4.0)

Files are daily tab-separated .txt files, one per day (Nov 2013 – Jan 2014).
Schema: square_id  time_interval  country_code  sms_in  sms_out  call_in  call_out  internet
"""
from __future__ import annotations

import argparse
import urllib.request
from pathlib import Path

from rich.console import Console
from rich.progress import (
    BarColumn, DownloadColumn, Progress,
    TextColumn, TimeRemainingColumn, TransferSpeedColumn,
)

from src.utils.paths import RAW_PATH, ensure_paths

console = Console()

# Harvard Dataverse file IDs for each daily CDR file
# Covers Nov 1 2013 → Jan 1 2014 (62 days)
DAILY_FILES: list[tuple[str, int]] = [
    ("sms-call-internet-mi-2013-11-01.txt", 2674255),
    ("sms-call-internet-mi-2013-11-02.txt", 2674265),
    ("sms-call-internet-mi-2013-11-03.txt", 2674273),
    ("sms-call-internet-mi-2013-11-04.txt", 2674282),
    ("sms-call-internet-mi-2013-11-05.txt", 2674279),
    ("sms-call-internet-mi-2013-11-06.txt", 2674283),
    ("sms-call-internet-mi-2013-11-07.txt", 2674271),
    ("sms-call-internet-mi-2013-11-08.txt", 2674261),
    ("sms-call-internet-mi-2013-11-09.txt", 2674268),
    ("sms-call-internet-mi-2013-11-10.txt", 2674259),
    ("sms-call-internet-mi-2013-11-11.txt", 2674272),
    ("sms-call-internet-mi-2013-11-12.txt", 2674284),
    ("sms-call-internet-mi-2013-11-13.txt", 2674257),
    ("sms-call-internet-mi-2013-11-14.txt", 2674267),
    ("sms-call-internet-mi-2013-11-15.txt", 2674281),
    ("sms-call-internet-mi-2013-11-16.txt", 2674256),
    ("sms-call-internet-mi-2013-11-17.txt", 2674266),
    ("sms-call-internet-mi-2013-11-18.txt", 2674260),
    ("sms-call-internet-mi-2013-11-19.txt", 2674280),
    ("sms-call-internet-mi-2013-11-20.txt", 2674263),
]

BASE_URL = "https://dataverse.harvard.edu/api/access/datafile"


def download_file(filename: str, file_id: int, dest: Path) -> bool:
    url  = f"{BASE_URL}/{file_id}"
    path = dest / filename

    if path.exists() and path.stat().st_size > 1_000_000:
        console.print(f"  [dim]↩  {filename} already exists, skipping[/dim]")
        return True

    with Progress(
        TextColumn(f"  [cyan]{filename}[/cyan]"),
        BarColumn(),
        DownloadColumn(),
        TransferSpeedColumn(),
        TimeRemainingColumn(),
        console=console,
        transient=True,
    ) as progress:
        task = progress.add_task("dl", total=None)

        def reporthook(block, block_size, total):
            if total > 0:
                progress.update(task, total=total, completed=block * block_size)

        try:
            urllib.request.urlretrieve(url, path, reporthook)
            size_mb = path.stat().st_size / 1_048_576
            console.print(f"  [green]✓[/green] {filename}  ({size_mb:.0f} MB)")
            return True
        except Exception as e:
            console.print(f"  [red]✗[/red] {filename}: {e}")
            if path.exists():
                path.unlink()
            return False


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=7,
                        help="Number of daily files to download (default 7)")
    args = parser.parse_args()

    ensure_paths()
    files = DAILY_FILES[: args.days]

    console.print(f"\n[bold cyan]Downloading {len(files)} day(s) of Telecom Italia Milan CDR data[/bold cyan]")
    console.print(f"  Source : Harvard Dataverse doi:10.7910/DVN/EGZHFV")
    console.print(f"  Dest   : {RAW_PATH}\n")

    ok = sum(download_file(fn, fid, RAW_PATH) for fn, fid in files)
    total_mb = sum((RAW_PATH / fn).stat().st_size for fn, _ in files if (RAW_PATH / fn).exists()) / 1_048_576

    console.print(f"\n[bold green]Downloaded {ok}/{len(files)} files — {total_mb:.0f} MB total[/bold green]\n")


if __name__ == "__main__":
    main()
