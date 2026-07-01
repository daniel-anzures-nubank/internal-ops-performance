"""Ad-hoc: generate INSERT SQL for the Feb-2026 DIME slots that are missing from
etl.mx__series_contract.agent_dimensioned_activities for the two Content agents
alejandra.erazo and eva.triay.

One-off deliverable (June 2026; not part of the pipeline). Kept under
scripts/adhoc/ for the record.

Source of truth: the Content Template W6-W9 Google-Drive DIME workbooks
(read via the google-workspace MCP). The ETL has zero rows for either agent in
Feb 2026, so every scheduled (working-day) slot is "missing" and is appended to
usr.danielanzures.missing_agents_dime_slots_content_h1.

Each working day is the agent's 18 half-hour slots 09:00-17:30 (Inicio 09:00).
Day strings below encode one char per slot, position 0 = 09:00 ... 17 = 17:30:
  C = productive content OOS   -> OOS_CONT / oos / oos
  L = lunch                    -> <week lunch token> / lunch_break / pause
  S = Sync (team sync meeting)  -> Sync / shrinkage / meeting
  O = 1:1                      -> 1:1 / shrinkage / meeting

Lunch token casing is per-week (mirrors the grid token, matching the existing
March rows): W6 -> 'lunch' (lowercase); W7/W8/W9 -> 'LUNCH' (uppercase).
Weekends (and Feb 1 Sun) are DayOFF for both agents and are excluded.
"""

# (date, week)  -- week drives the lunch-token casing
WEEK_OF = {
    # W6 (Feb 2-6): grid tokens 'Cont'/'lunch'
    "2026-02-02": 6, "2026-02-03": 6, "2026-02-04": 6, "2026-02-05": 6, "2026-02-06": 6,
    # W7 (Feb 9-13): grid tokens 'OOS_CONT'/'LUNCH'
    "2026-02-09": 7, "2026-02-10": 7, "2026-02-11": 7, "2026-02-12": 7, "2026-02-13": 7,
    # W8 (Feb 16-20)
    "2026-02-16": 8, "2026-02-17": 8, "2026-02-18": 8, "2026-02-19": 8, "2026-02-20": 8,
    # W9 (Feb 23-27)
    "2026-02-23": 9, "2026-02-24": 9, "2026-02-25": 9, "2026-02-26": 9, "2026-02-27": 9,
}

DAYS = {
    "eva.triay": {
        "2026-02-02": "CCCCCCCCCCCCLLCCCC",
        "2026-02-03": "CCCCCCCCCCCCLLCCCC",
        "2026-02-04": "CCCCCCCCCCCCLLCCCC",
        "2026-02-05": "CCCCCSSCCCCCLLCCCC",
        "2026-02-06": "CCCCCCCCCCCCLLCCCC",
        "2026-02-09": "CCCCSSCCCCCCLLCCCC",
        "2026-02-10": "CCCCOOCCCCCCLLCCCC",
        "2026-02-11": "CCCCCCCCCCCCLLCCCC",
        "2026-02-12": "CCCCCSSCCCCCLLCCCC",
        "2026-02-13": "CCCCCCCCCCCCLLCCCC",
        "2026-02-16": "CCCCCCSSCCCCLLCCCC",
        "2026-02-17": "CCCCCOCCCCCCLLCCCC",
        "2026-02-18": "CCCCSSCCCCCCLLCCCC",
        "2026-02-19": "CCSCCSSCCCCCLLCCCC",
        "2026-02-20": "CCCCCCCCCCCCLLCCCC",
        "2026-02-23": "CCCCCCSSCCCCLLCCCC",
        "2026-02-24": "CCCCCCOOCCCCLLCCCC",
        "2026-02-25": "CCCCCCCCCCCCLLCCCC",
        "2026-02-26": "CCCCCCCCCCCCLLCCCC",
        "2026-02-27": "CCSSCCSCCCCCLLCCCC",
    },
    "alejandra.erazo": {
        "2026-02-02": "CCCCCCCCCCCCLLCCCC",
        "2026-02-03": "CCCCCCCCCCCCLLSSCC",
        "2026-02-04": "CCCCSSSSCCCCLLCSCC",
        "2026-02-05": "SSSSCCCCSSCCLLCCCC",
        "2026-02-06": "CCCCCCCCCCCCLLCCCC",
        "2026-02-09": "CCCCCCCCSSCCLLSSCC",
        "2026-02-10": "COOCCCCCCCCCLLCCCC",
        "2026-02-11": "CCCCCSSCCCCCLLCCCC",
        "2026-02-12": "CCCCCCCCCCCCLLSCCC",
        "2026-02-13": "CCSCCCCCCCCCLLCCCC",
        "2026-02-16": "CCCCCCSSCCCCLLCCCC",
        "2026-02-17": "COCCCCCCCCCCLLCCCC",
        "2026-02-18": "CCCCSSCCCCCCLLCCCC",
        "2026-02-19": "CCSCCCCCCCCCLLCCCC",
        "2026-02-20": "CCCCCCCCCCCCLLCCCC",
        "2026-02-23": "CCCCCCSSCCCCLLCCCC",
        "2026-02-24": "COOCCCCCCCCCLLCCCC",
        "2026-02-25": "CCCCCCCCCCCCLLCCCC",
        "2026-02-26": "CCCCCCCCCCCCLLCCCC",
        "2026-02-27": "CCCCCCSCCCCCLLCCCC",
    },
}

TABLE = "usr.danielanzures.missing_agents_dime_slots_content_h1"
DIME_SQUAD = "content_content"

# 18 slot start times, 09:00 .. 17:30
SLOT_TIMES = [f"{9 + (i // 2)}:{'30' if i % 2 else '00'}" for i in range(18)]


def slot_hhmm(i):
    h = 9 + (i // 2)
    m = "30" if i % 2 else "00"
    return f"{h:02d}:{m}:00"


def triple(token, week):
    if token == "C":
        return ("OOS_CONT", "oos", "oos")
    if token == "L":
        lunch_da = "lunch" if week == 6 else "LUNCH"
        return (lunch_da, "lunch_break", "pause")
    if token == "S":
        return ("Sync", "shrinkage", "meeting")
    if token == "O":
        return ("1:1", "shrinkage", "meeting")
    raise ValueError(f"unknown token {token!r}")


def build_rows():
    rows = []
    for agent, days in DAYS.items():
        for date, daystr in sorted(days.items()):
            assert len(daystr) == 18, f"{agent} {date}: len {len(daystr)} != 18"
            week = WEEK_OF[date]
            for i, tok in enumerate(daystr):
                da, atr, ssr = triple(tok, week)
                ts = f"{date} {slot_hhmm(i)}"
                rows.append((agent, date, ts, DIME_SQUAD, atr, ssr, da))
    return rows


def _lit(s):
    return "'" + s.replace("'", "''") + "'"


def value_tuple(row):
    agent, date, ts, sq, atr, ssr, da = row
    return (
        f"({_lit(agent)}, DATE'{date}', TIMESTAMP'{ts}', {_lit(sq)}, "
        f"{_lit(atr)}, {_lit(ssr)}, {_lit(da)}, CAST(NULL AS STRING))"
    )


INSERT_HEAD = (
    f"INSERT INTO {TABLE}\n"
    f"  (agent, date, slot_start_local, dime_squad, "
    f"activity_type_required, shuffle_status_required, dimensioned_activity, _rescued_data)\n"
    "VALUES\n"
)


def write_chunks(rows, chunk_size=120, out_dir="output"):
    import os

    os.makedirs(out_dir, exist_ok=True)
    paths = []
    for i in range(0, len(rows), chunk_size):
        chunk = rows[i : i + chunk_size]
        sql = INSERT_HEAD + ",\n".join(value_tuple(r) for r in chunk) + ";\n"
        path = f"{out_dir}/insert_feb_chunk_{i // chunk_size + 1}.sql"
        with open(path, "w") as f:
            f.write(sql)
        paths.append((path, len(chunk)))
    return paths


if __name__ == "__main__":
    rows = build_rows()
    paths = write_chunks(rows)

    from collections import Counter

    per_agent = Counter(r[0] for r in rows)
    per_da = Counter(r[6] for r in rows)
    print(f"total rows: {len(rows)}")
    print("per agent:", dict(per_agent))
    print("per dimensioned_activity:", dict(per_da))
    for p, n in paths:
        print(f"  {p}: {n} rows")
