import sqlite3
from src.pipeline import process

def test_incremental_and_idempotent():
    db=sqlite3.connect(':memory:')
    e={'event_id':'1','customer_id':'a','amount':5,'updated_at':'2026-01-01T00:00:00Z'}
    assert process(db,[e])==1
    assert process(db,[e])==0
    assert db.execute('SELECT total_amount FROM gold').fetchone()[0]==5
    e2={**e,'amount':8,'updated_at':'2026-01-02T00:00:00Z'}
    process(db,[e2]);assert db.execute('SELECT total_amount FROM gold').fetchone()[0]==8
