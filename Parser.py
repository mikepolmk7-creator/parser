#!/usr/bin/env python3
"""
Парсер лидов (Воронеж): бухаутсорсинг / касса / 1С, СБИС, Контур, R-Keeper / IT-поддержка.
Все данные хранятся в SQLite (leads.db).

Команды (запускать по порядку):
  python leads.py import-msp ПУТЬ        # ПУТЬ: папка с XML, .xml или .zip реестра МСП
  python leads.py import-inns inns.txt   # альтернатива: свой список ИНН
  python leads.py enrich [--limit 500]   # обогащение через DaData (можно прерывать и продолжать)
  python leads.py classify               # фильтры по обороту/ОКВЭД и разметка сегментов
  python leads.py export [--segment bukh] [--out leads.csv]
  python leads.py stats                  # сводка по базе

Токен DaData: переменная окружения DADATA_TOKEN
"""
import argparse
import csv
import io
import json
import os
import sqlite3
import sys
import time
import zipfile
import xml.etree.ElementTree as ET
from datetime import datetime

import requests

DB_PATH = os.environ.get("LEADS_DB", "leads.db")
DADATA_URL = "https://suggestions.dadata.ru/suggestions/api/4_1/rs/findById/party"

# ---------------- Настройки фильтров ----------------
REGION_CODE = "36"          # Воронежская область
DEFAULT_CITY = "Воронеж"    # пусто = вся область

LIMIT_BUKH = 50_000_000     # бухобслуживание: оборот до 50 млн
LIMIT_OTHER = 100_000_000   # касса / 1С / IT: оборот до 100 млн

# Не берём (префиксы основного ОКВЭД-2)
EXCLUDE_PREFIXES = (
    "01", "02", "03",       # сельское/лесное хозяйство, рыболовство
    "41", "42", "43",       # строительство
    "49.4",                 # грузоперевозки
    "85",                   # образование
)
KASSA_PREFIXES = ("47", "56", "55", "96", "93", "86.23")  # розница, общепит, услуги
RKEEPER_PREFIXES = ("56", "55.3")                         # рестораны, кафе


def starts(code, prefixes):
    return bool(code) and code.startswith(tuple(prefixes))


# ---------------- База данных ----------------
SCHEMA = """
CREATE TABLE IF NOT EXISTS companies (
    inn TEXT PRIMARY KEY,
    name TEXT,
    msp_category TEXT,
    region_code TEXT,
    city TEXT,
    okved TEXT,
    okveds TEXT,
    source TEXT,
    imported_at TEXT
);
CREATE TABLE IF NOT EXISTS enrichment (
    inn TEXT PRIMARY KEY,
    status TEXT, type TEXT, name_short TEXT,
    okved TEXT, okveds TEXT,
    ifns TEXT, region TEXT, city TEXT, address TEXT,
    revenue REAL, fin_year INTEGER, tax_system TEXT, employees INTEGER,
    director TEXT, phones TEXT, emails TEXT,
    raw_json TEXT, error TEXT, fetched_at TEXT
);
CREATE TABLE IF NOT EXISTS leads (
    inn TEXT PRIMARY KEY,
    segments TEXT,
    needs_review INTEGER DEFAULT 0,
    reason TEXT,
    classified_at TEXT
);
"""


def db():
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    con.executescript(SCHEMA)
    return con


def now():
    return datetime.now().isoformat(timespec="seconds")


# ---------------- Импорт реестра МСП ----------------
def local(tag):
    return tag.rsplit("}", 1)[-1]


def parse_doc(el):
    d = {"inn": None, "name": None, "cat": el.get("КатСубМСП"),
         "region": None, "city": None, "okved": None, "extra": [], "legal": False}
    for e in el.iter():
        t = local(e.tag)
        if t == "ОргВклМСП":
            d["legal"] = True
            d["inn"] = e.get("ИННЮЛ")
            d["name"] = e.get("НаимОргСокр") or e.get("НаимОрг")
        elif t == "СведМН":
            d["region"] = e.get("КодРегион")
        elif t == "Город":
            d["city"] = e.get("Наим")
        elif t == "НаселПункт" and not d["city"]:
            d["city"] = e.get("Наим")
        elif t == "СвОКВЭДОсн":
            d["okved"] = e.get("КодОКВЭД")
        elif t == "СвОКВЭДДоп":
            if e.get("КодОКВЭД"):
                d["extra"].append(e.get("КодОКВЭД"))
    return d


def iter_xml_streams(path):
    if os.path.isdir(path):
        for root, _, files in os.walk(path):
            for fn in sorted(files):
                full = os.path.join(root, fn)
                if fn.lower().endswith(".xml"):
                    yield fn, open(full, "rb")
                elif fn.lower().endswith(".zip"):
                    yield from iter_xml_streams(full)
    elif path.lower().endswith(".zip"):
        with zipfile.ZipFile(path) as z:
            for name in z.namelist():
                if name.lower().endswith(".xml"):
                    yield name, io.BufferedReader(z.open(name))
    else:
        yield os.path.basename(path), open(path, "rb")


def cmd_import_msp(args):
    con = db()
    city = (args.city or "").strip().lower()
    seen = kept = 0
    for name, stream in iter_xml_streams(args.path):
        print(f"читаю {name} ...")
        try:
            for _, el in ET.iterparse(stream, events=("end",)):
                if local(el.tag) != "Документ":
                    continue
                seen += 1
                d = parse_doc(el)
                el.clear()
                if not d["legal"] or not d["inn"]:
                    continue
                if d["region"] != REGION_CODE:
                    continue
                if city and (d["city"] or "").strip().lower() != city:
                    continue
                if starts(d["okved"], EXCLUDE_PREFIXES):
                    continue
                con.execute(
                    "INSERT OR REPLACE INTO companies VALUES (?,?,?,?,?,?,?,?,?)",
                    (d["inn"], d["name"], d["cat"], d["region"], d["city"],
                     d["okved"], ",".join(d["extra"]), "msp", now()),
                )
                kept += 1
                if kept % 2000 == 0:
                    con.commit()
        except ET.ParseError as e:
            print(f"  ошибка разбора {name}: {e}")
        finally:
            stream.close()
    con.commit()
    print(f"просмотрено записей: {seen}, сохранено в базу: {kept}")
    if seen and not kept:
        print("Ничего не подошло. Проверьте структуру XML (атрибуты КодРегион, Город, "
              "ОКВЭДОсн) и значения REGION_CODE / --city.")


def cmd_import_inns(args):
    con = db()
    n = 0
    with open(args.path, encoding="utf-8") as f:
        for line in f:
            inn = line.strip()
            if inn.isdigit() and len(inn) == 10:
                con.execute(
                    "INSERT OR IGNORE INTO companies (inn, source, imported_at) VALUES (?,?,?)",
                    (inn, "manual", now()),
                )
                n += 1
    con.commit()
    print(f"добавлено ИНН: {n}")


# ---------------- Обогащение через DaData ----------------
def dadata_find(inn, token):
    for attempt in range(4):
        r = requests.post(
            DADATA_URL,
            json={"query": inn},
            headers={"Authorization": f"Token {token}",
                     "Content-Type": "application/json", "Accept": "application/json"},
            timeout=20,
        )
        if r.status_code == 429:
            time.sleep(2 * (attempt + 1))
            continue
        r.raise_for_status()
        sug = r.json().get("suggestions", [])
        return sug[0]["data"] if sug else None
    raise RuntimeError("DaData: превышен лимит запросов (429)")


def flatten(d):
    fin = d.get("finance") or {}
    addr = (d.get("address") or {}).get("data") or {}
    auth = (d.get("authorities") or {}).get("fts_registration") or {}
    return {
        "status": (d.get("state") or {}).get("status"),
        "type": d.get("type"),
        "name_short": (d.get("name") or {}).get("short_with_opf"),
        "okved": d.get("okved"),
        "okveds": ",".join(o.get("code", "") for o in (d.get("okveds") or [])),
        "ifns": auth.get("code") or addr.get("tax_office"),
        "region": addr.get("region_with_type"),
        "city": addr.get("city"),
        "address": (d.get("address") or {}).get("value"),
        "revenue": fin.get("income"),
        "fin_year": fin.get("year"),
        "tax_system": fin.get("tax_system"),
        "employees": d.get("employee_count"),
        "director": (d.get("management") or {}).get("name"),
        "phones": ",".join(p.get("value", "") for p in (d.get("phones") or [])),
        "emails": ",".join(e.get("value", "") for e in (d.get("emails") or [])),
    }


def cmd_enrich(args):
    token = os.environ.get("DADATA_TOKEN")
    if not token:
        sys.exit("Задайте токен: export DADATA_TOKEN=...")
    con = db()
    rows = con.execute(
        """SELECT c.inn FROM companies c
           LEFT JOIN enrichment e ON e.inn = c.inn
           WHERE e.inn IS NULL OR (e.error IS NOT NULL AND ?)
           LIMIT ?""",
        (1 if args.retry_errors else 0, args.limit),
    ).fetchall()
    print(f"к обогащению: {len(rows)}")
    for i, row in enumerate(rows, 1):
        inn = row["inn"]
        try:
            d = dadata_find(inn, token)
            if d is None:
                con.execute("INSERT OR REPLACE INTO enrichment (inn, error, fetched_at) VALUES (?,?,?)",
                            (inn, "not_found", now()))
            else:
                f = flatten(d)
                cols = ["inn"] + list(f.keys()) + ["raw_json", "error", "fetched_at"]
                vals = [inn] + list(f.values()) + [json.dumps(d, ensure_ascii=False), None, now()]
                con.execute(
                    f"INSERT OR REPLACE INTO enrichment ({','.join(cols)}) "
                    f"VALUES ({','.join('?' * len(cols))})", vals)
        except Exception as e:  # сетевые ошибки не должны ронять весь прогон
            print(f"  {inn}: {e}")
            if "429" in str(e) or "лимит" in str(e):
                con.commit()
                sys.exit("Лимит DaData исчерпан. Запустите enrich позже, прогресс сохранён.")
            con.execute("INSERT OR REPLACE INTO enrichment (inn, error, fetched_at) VALUES (?,?,?)",
                        (inn, str(e)[:200], now()))
        if i % 50 == 0:
            con.commit()
            print(f"  {i}/{len(rows)}")
        time.sleep(0.12)  # ~8 запросов/сек
    con.commit()
    print("готово")


# ---------------- Классификация ----------------
def cmd_classify(args):
    con = db()
    con.execute("DELETE FROM leads")
    rows = con.execute(
        """SELECT e.*, c.msp_category AS cat FROM enrichment e
           LEFT JOIN companies c ON c.inn = e.inn
           WHERE e.error IS NULL""").fetchall()
    kept = 0
    for e in rows:
        okved = e["okved"] or ""
        if e["type"] != "LEGAL" or e["status"] != "ACTIVE":
            continue
        if starts(okved, EXCLUDE_PREFIXES):
            continue
        rev = e["revenue"]
        tags, review, reason = [], 0, ""
        if rev is None:
            # оборот недоступен: сегменты по ОКВЭД и категории МСП
            review = 1
            reason = f"оборот не проверен, категория МСП={e['cat']}"
            tags = ["review"]
            if e["cat"] == "1":          # микропредприятие
                tags += ["bukh", "it"]
            if starts(okved, KASSA_PREFIXES):
                tags.append("kassa")
            if starts(okved, RKEEPER_PREFIXES):
                tags.append("rkeeper")
        else:
            if rev <= LIMIT_BUKH:
                tags.append("bukh")
            if rev <= LIMIT_OTHER:
                tags.append("it")
                if starts(okved, KASSA_PREFIXES):
                    tags.append("kassa")
                if starts(okved, RKEEPER_PREFIXES):
                    tags.append("rkeeper")
        if tags:
            con.execute("INSERT INTO leads VALUES (?,?,?,?,?)",
                        (e["inn"], ",".join(tags), review, reason, now()))
            kept += 1
    con.commit()
    print(f"лидов в таблице leads: {kept}")


# ---------------- Экспорт и статистика ----------------
def cmd_export(args):
    con = db()
    q = """SELECT l.inn, l.segments, l.needs_review, l.reason,
                  e.name_short AS name, e.okved, e.okveds, e.ifns, e.city, e.address,
                  e.revenue, e.fin_year, e.tax_system, e.employees,
                  e.director, e.phones, e.emails, c.msp_category
           FROM leads l
           JOIN enrichment e ON e.inn = l.inn
           LEFT JOIN companies c ON c.inn = l.inn"""
    rows = con.execute(q).fetchall()
    if args.segment:
        rows = [r for r in rows if args.segment in r["segments"].split(",")]
    if not rows:
        print("нет данных для экспорта")
        return
    with open(args.out, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f, delimiter=";")
        w.writerow(rows[0].keys())
        for r in rows:
            w.writerow(list(r))
    print(f"записано строк: {len(rows)} -> {args.out}")


def cmd_stats(args):
    con = db()
    for label, q in [
        ("компаний в базе", "SELECT COUNT(*) FROM companies"),
        ("обогащено", "SELECT COUNT(*) FROM enrichment WHERE error IS NULL"),
        ("ошибок обогащения", "SELECT COUNT(*) FROM enrichment WHERE error IS NOT NULL"),
        ("лидов", "SELECT COUNT(*) FROM leads"),
        ("на ручную проверку", "SELECT COUNT(*) FROM leads WHERE needs_review=1"),
    ]:
        print(f"{label}: {con.execute(q).fetchone()[0]}")
    for seg in ("bukh", "it", "kassa", "rkeeper"):
        n = con.execute("SELECT COUNT(*) FROM leads WHERE ',' || segments || ',' LIKE ?",
                        (f"%,{seg},%",)).fetchone()[0]
        print(f"  сегмент {seg}: {n}")


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="cmd", required=True)

    a = sub.add_parser("import-msp")
    a.add_argument("path")
    a.add_argument("--city", default=DEFAULT_CITY, help="город (пусто = вся область)")
    a.set_defaults(fn=cmd_import_msp)

    a = sub.add_parser("import-inns")
    a.add_argument("path")
    a.set_defaults(fn=cmd_import_inns)

    a = sub.add_parser("enrich")
    a.add_argument("--limit", type=int, default=500)
    a.add_argument("--retry-errors", action="store_true")
    a.set_defaults(fn=cmd_enrich)

    sub.add_parser("classify").set_defaults(fn=cmd_classify)

    a = sub.add_parser("export")
    a.add_argument("--segment", choices=["bukh", "it", "kassa", "rkeeper", "review"])
    a.add_argument("--out", default="leads.csv")
    a.set_defaults(fn=cmd_export)

    sub.add_parser("stats").set_defaults(fn=cmd_stats)

    args = p.parse_args()
    args.fn(args)


if __name__ == "__main__":
    main()
