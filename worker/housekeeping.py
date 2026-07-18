import os
import sys
import argparse
from datetime import datetime, timezone, timedelta
import psycopg2

LAYERS = ["bronze", "silver", "gold"]

def get_db_connection():
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        print("[Housekeeping] ERROR: DATABASE_URL not set in environment.")
        sys.exit(1)
    return psycopg2.connect(db_url)


def cleanup_temp_and_backup_tables(dry_run: bool = True):
    """Cleanup temporary staging tables (__tmp_) and old backup tables (__bak_)."""
    print(f"=== Gaung Housekeeping: Database Maintenance (dry_run={dry_run}) ===")
    conn = get_db_connection()
    try:
        cur = conn.cursor()

        for layer in LAYERS:
            # 1. Find temporary staging tables (__tmp_)
            cur.execute(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = %s AND table_name LIKE %s",
                (layer, "%__tmp_%")
            )
            tmp_tables = [row[0] for row in cur.fetchall()]

            print(f"[{layer.upper()}] Found {len(tmp_tables)} temporary staging table(s)")
            for tbl in tmp_tables:
                if dry_run:
                    print(f"  [DRY-RUN] Would DROP TABLE {layer}.\"{tbl}\"")
                else:
                    cur.execute(f'DROP TABLE IF EXISTS {layer}."{tbl}"')
                    print(f"  [APPLIED] Dropped {layer}.\"{tbl}\"")

            # 2. Find backup tables (__bak_)
            cur.execute(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = %s AND table_name LIKE %s "
                "ORDER BY table_name DESC",
                (layer, "%__bak_%")
            )
            bak_tables = [row[0] for row in cur.fetchall()]
            
            # Keep recent 3 backups per base table, drop older ones
            table_groups: dict[str, list[str]] = {}
            for bak in bak_tables:
                base_name = bak.split("__bak_")[0]
                table_groups.setdefault(base_name, []).append(bak)

            for base_name, baks in table_groups.items():
                to_drop = baks[3:]  # keep newest 3
                if to_drop:
                    print(f"[{layer.upper()}] Base table '{base_name}': keeping 3 backups, {len(to_drop)} to clean")
                    for old_bak in to_drop:
                        if dry_run:
                            print(f"  [DRY-RUN] Would DROP backup {layer}.\"{old_bak}\"")
                        else:
                            cur.execute(f'DROP TABLE IF EXISTS {layer}."{old_bak}"')
                            print(f"  [APPLIED] Dropped backup {layer}.\"{old_bak}\"")

        if not dry_run:
            conn.commit()
        cur.close()
    finally:
        conn.close()
    print("[Housekeeping] Maintenance check finished.\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Gaung Lakehouse Data Housekeeping Script")
    parser.add_argument("--apply", action="store_true", help="Apply database drops (default is dry-run mode)")
    args = parser.parse_args()

    dry_run = not args.apply
    cleanup_temp_and_backup_tables(dry_run=dry_run)
