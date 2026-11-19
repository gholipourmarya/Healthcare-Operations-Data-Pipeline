"""Small dependency-free ETL example. Input is synthetic demonstration data."""
import csv
import json
import sqlite3
from collections import defaultdict
from pathlib import Path

BASE = Path(__file__).resolve().parents[1]
DOMAIN = "healthcare"

def transform(row):
    """Return normalized record or raise ValueError for invalid source rows."""
    out = dict(row)
    try:
        if DOMAIN == 'retail':
            qty, price, stock = int(row['quantity']), float(row['unit_price']), int(row['stock'])
            if min(qty, price, stock) < 0: raise ValueError('negative retail value')
            out['value'] = round(qty * price, 2)
            out['flag'] = int(stock < 15)
        elif DOMAIN == 'iot':
            temp, vibration, pressure = float(row['temperature_c']), float(row['vibration_mm_s']), float(row['pressure_bar'])
            if pressure < 0: raise ValueError('negative pressure')
            out['value'] = 1.0
            out['flag'] = int(temp > 85 or vibration > 6)
        elif DOMAIN == 'banking':
            amount, hour = float(row['amount']), int(row['hour'])
            if amount < 0 or not 0 <= hour <= 23: raise ValueError('invalid transaction')
            out['value'] = amount
            out['flag'] = int(amount >= 10000 or hour < 5)
        elif DOMAIN == 'energy':
            consumption = float(row['consumption_kwh'])
            if consumption < 0: raise ValueError('negative consumption')
            out['value'] = consumption
            out['flag'] = int(consumption > 30)
        elif DOMAIN == 'healthcare':
            tests = int(row['lab_tests'])
            if tests < 0 or row['status'] not in ('completed', 'cancelled', 'scheduled'): raise ValueError('invalid appointment')
            out['value'] = 1.0
            out['flag'] = int(row['status'] == 'cancelled')
        else: raise ValueError('unknown domain')
    except (KeyError, TypeError, ValueError) as exc:
        raise ValueError(f'Invalid row: {exc}') from exc
    return out

def run():
    input_path = BASE / 'data' / 'input.csv'
    output = BASE / 'output'
    output.mkdir(exist_ok=True)
    valid, rejected = [], []
    with input_path.open(newline='', encoding='utf-8') as handle:
        for row in csv.DictReader(handle):
            try: valid.append(transform(row))
            except ValueError as exc: rejected.append({'row': row, 'reason': str(exc)})
    if not valid: raise RuntimeError('No valid records')
    with (output / 'clean.jsonl').open('w', encoding='utf-8') as handle:
        for row in valid: handle.write(json.dumps(row) + '\n')
    (output / 'rejected.json').write_text(json.dumps(rejected, indent=2), encoding='utf-8')
    con = sqlite3.connect(output / 'warehouse.db')
    try:
        con.execute('DROP TABLE IF EXISTS events')
        con.execute('CREATE TABLE events (group_key TEXT NOT NULL, value REAL NOT NULL, flag INTEGER NOT NULL)')
        con.executemany('INSERT INTO events VALUES (?, ?, ?)', [(r['department'], r['value'], r['flag']) for r in valid])
        con.commit()
        result = con.execute('SELECT group_key, COUNT(*), ROUND(SUM(value),2), SUM(flag) FROM events GROUP BY group_key ORDER BY group_key').fetchall()
    finally: con.close()
    summary = [{'group': g, 'records': n, 'total_value': value, 'flags': flags} for g,n,value,flags in result]
    (output / 'summary.json').write_text(json.dumps(summary, indent=2), encoding='utf-8')
    print(json.dumps({'accepted': len(valid), 'rejected': len(rejected), 'summary': summary}, indent=2))
    return summary

if __name__ == '__main__': run()
