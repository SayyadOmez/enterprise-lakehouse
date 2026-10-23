import argparse
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path


def initialize(db):
    db.executescript("""
    CREATE TABLE IF NOT EXISTS watermark (source TEXT PRIMARY KEY, last_ts TEXT NOT NULL);
    CREATE TABLE IF NOT EXISTS bronze (event_id TEXT PRIMARY KEY, updated_at TEXT NOT NULL, payload TEXT NOT NULL);
    CREATE TABLE IF NOT EXISTS silver (event_id TEXT PRIMARY KEY, customer_id TEXT NOT NULL, amount REAL NOT NULL, updated_at TEXT NOT NULL);
    CREATE TABLE IF NOT EXISTS gold (customer_id TEXT PRIMARY KEY, total_amount REAL NOT NULL, order_count INTEGER NOT NULL);
    CREATE TABLE IF NOT EXISTS rejected (event_id TEXT, reason TEXT, payload TEXT);
    """)


def process(db, events, source='orders'):
    initialize(db)
    last = db.execute('SELECT last_ts FROM watermark WHERE source=?', (source,)).fetchone()
    cutoff = last[0] if last else ''
    incoming = sorted((e for e in events if e['updated_at'] > cutoff), key=lambda e: e['updated_at'])
    with db:
        for event in incoming:
            eid = str(event.get('event_id', ''))
            try:
                amount = float(event['amount'])
                if not eid or not event.get('customer_id') or amount < 0:
                    raise ValueError('invalid required fields')
                datetime.fromisoformat(event['updated_at'].replace('Z','+00:00'))
            except (KeyError, ValueError, TypeError) as exc:
                db.execute('INSERT INTO rejected VALUES (?,?,?)',(eid,str(exc),json.dumps(event)))
                continue
            db.execute('INSERT INTO bronze VALUES (?,?,?) ON CONFLICT(event_id) DO UPDATE SET updated_at=excluded.updated_at,payload=excluded.payload WHERE excluded.updated_at > bronze.updated_at', (eid,event['updated_at'],json.dumps(event)))
            db.execute('INSERT INTO silver VALUES (?,?,?,?) ON CONFLICT(event_id) DO UPDATE SET customer_id=excluded.customer_id,amount=excluded.amount,updated_at=excluded.updated_at WHERE excluded.updated_at > silver.updated_at', (eid,event['customer_id'],amount,event['updated_at']))
        db.execute('DELETE FROM gold')
        db.execute('INSERT INTO gold SELECT customer_id,SUM(amount),COUNT(*) FROM silver GROUP BY customer_id')
        if incoming:
            db.execute('INSERT INTO watermark VALUES (?,?) ON CONFLICT(source) DO UPDATE SET last_ts=excluded.last_ts', (source,max(e['updated_at'] for e in incoming)))
    return len(incoming)


def main():
    parser=argparse.ArgumentParser();parser.add_argument('--input',default='data/orders.jsonl');parser.add_argument('--db',default='output/lakehouse.db');args=parser.parse_args()
    Path(args.db).parent.mkdir(parents=True,exist_ok=True)
    with sqlite3.connect(args.db) as db:
        events=[json.loads(line) for line in Path(args.input).read_text().splitlines() if line.strip()]
        print('processed:',process(db,events))
        print('gold:',db.execute('SELECT * FROM gold ORDER BY customer_id').fetchall())
if __name__=='__main__':main()
