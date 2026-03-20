import argparse
import logging
import os
from pathlib import Path

from dotenv import load_dotenv

from timelineparser.db import Database

load_dotenv()


def cmd_parse(args: argparse.Namespace) -> None:
    from timelineparser.parser import parse_timeline

    if args.db:
        db_path = Path(args.db)
    else:
        db_path = Path(args.input).parent / "timeline.db"
    db_path.parent.mkdir(parents=True, exist_ok=True)

    with Database(db_path) as db:
        db.create_tables()
        stats = parse_timeline(args.input, db)

    print("\nParse complete:")
    for key, val in stats.items():
        print(f"  {key}: {val}")
    print(f"\nDatabase: {db_path}")


def cmd_enrich(args: argparse.Namespace) -> None:
    from timelineparser.enricher import enrich_places

    api_key = args.api_key or os.environ.get("GOOGLE_PLACES_API_KEY")
    if not api_key:
        print("Error: provide --api-key or set GOOGLE_PLACES_API_KEY")
        raise SystemExit(1)

    with Database(args.db) as db:
        stats = enrich_places(db, api_key, args.batch_size, args.delay)

    print("\nEnrichment complete:")
    for key, val in stats.items():
        print(f"  {key}: {val}")


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="timelineparser",
        description="Parse Google Maps Timeline JSON exports into SQLite",
    )
    parser.add_argument(
        "--log-level",
        default=os.environ.get("LOG_LEVEL", "info"),
        choices=["debug", "info", "warning", "error"],
    )
    sub = parser.add_subparsers(dest="command", required=True)

    # parse
    p_parse = sub.add_parser("parse", help="Parse Timeline.json into SQLite")
    p_parse.add_argument("--input", required=True, help="Path to Timeline.json")
    p_parse.add_argument(
        "--db", default=None, help="Database path (default: alongside input file)"
    )

    # enrich
    p_enrich = sub.add_parser("enrich", help="Enrich places with Google Places API")
    p_enrich.add_argument("--db", required=True, help="Path to timeline.db")
    p_enrich.add_argument("--api-key", default=None)
    p_enrich.add_argument("--batch-size", type=int, default=50)
    p_enrich.add_argument("--delay", type=float, default=0.1)

    args = parser.parse_args()
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper()),
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )

    commands = {"parse": cmd_parse, "enrich": cmd_enrich}
    commands[args.command](args)
