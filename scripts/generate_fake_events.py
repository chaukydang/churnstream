"""Generate eventsim-like JSON lines for local runs (no Kafka / eventsim needed).

Output layout:  <out>/<topic>/part-N.json   (one JSON object per line)

Unlike a purely random generator, users here have a hidden "churn propensity":
churners listen less over time, thumbs-down / hit errors more, visit Downgrade
and Help pages more, fail logins more, and finally hit "Cancellation
Confirmation" inside the last `label_days` days. This gives the ML part of the
project (ml/) a real, learnable signal. The data is SYNTHETIC - model metrics on
it only demonstrate that the pipeline works, not real-world performance.

Usage:
    python scripts/generate_fake_events.py [out_dir] [users] [days] [seed]
"""
import json
import os
import random
import sys
import time

OUT, N_USERS, DAYS, SEED = "sample_input", 300, 14, 42  # CLI args override in main
LABEL_DAYS = 4          # last N days = label window (cancellations happen here)
FILES_PER_TOPIC = 4
CHURN_RATE = 0.3

SONGS = [("Daft Punk", "One More Time"), ("Björk", "Jóga"),
         ("Sigur Rós", "Hoppípolla"), ("Adele", "Hello"),
         ("Coldplay", "Yellow"), ("Radiohead", "Creep"),
         ("Beyoncé", "Halo"), ("Daft Punk", "Get Lucky"),
         ("Adele", "Skyfall"), ("Coldplay", "Fix You")]
CITIES = [("Boston", "MA", 42.36, -71.06), ("Austin", "TX", 30.27, -97.74),
          ("Seattle", "WA", 47.61, -122.33), ("Denver", "CO", 39.74, -104.99)]
AGENTS = ["Mozilla/5.0 (Macintosh)", "Mozilla/5.0 (Windows NT 10.0)", "Mozilla/5.0 (iPhone)"]


def esc(text):
    """eventsim writes non-ASCII as literal \\xNN escapes wrapped in quotes."""
    body = "".join(c if ord(c) < 128 else "".join("\\x%02x" % b for b in c.encode())
                   for c in text)
    return '"' + body + '"'


def build(seed=SEED, n_users=N_USERS, days=DAYS, now_ms=None):
    rnd = random.Random(seed)
    now_ms = now_ms or int(time.time() * 1000)
    day_ms = 86_400_000
    start = now_ms - days * day_ms
    cancel_from = now_ms - LABEL_DAYS * day_ms
    topics = {"listen_events": [], "page_view_events": [], "auth_events": []}
    session_counter = 0

    for uid in range(1, n_users + 1):
        churner = rnd.random() < CHURN_RATE
        city = rnd.choice(CITIES)
        user = dict(userId=uid, firstName=f"User{uid}", lastName="Test",
                    gender=rnd.choice(["M", "F"]), city=city[0], state=city[1],
                    lat=city[2], lon=city[3], zip=rnd.randint(10000, 99999),
                    userAgent=rnd.choice(AGENTS),
                    registration=start - rnd.randint(1, 200) * day_ms)
        level = "paid" if rnd.random() < (0.45 if churner else 0.35) else "free"
        favourites = rnd.sample(SONGS, rnd.randint(2, 6))

        engagement = rnd.lognormvariate(0, 0.45)  # heavy vs light listeners, any user
        cancel_day = rnd.randint(days - LABEL_DAYS, days - 1) if churner else None
        for d in range(days):
            is_cancel_day = d == cancel_day
            # engagement: churners fade out before they cancel
            fade = max(0.6, 1 - 0.4 * d / days) if churner else 1.0
            if not is_cancel_day and rnd.random() > min(0.95, 0.7 * fade * engagement) + 0.05:
                continue
            day_start = start + d * day_ms
            if churner and d > cancel_day:
                break  # user already left
            session_counter += 1
            sid = session_counter
            t = day_start + rnd.randint(8, 22) * 3_600_000
            item = 0

            def base(ts):
                nonlocal item
                item += 1
                return dict(user, ts=ts, level=level, itemInSession=item)

            # login (churners fail more often)
            ok = is_cancel_day or rnd.random() > (0.10 if churner else 0.04)
            topics["auth_events"].append(dict(base(t), sessionId=sid, success=ok))
            if not ok:
                continue

            n_songs = max(1, int(rnd.gauss(10 * fade * engagement, 3)))
            for _ in range(n_songs):
                t += rnd.randint(120, 300) * 1000
                artist, song = rnd.choice(favourites if rnd.random() < 0.7 else SONGS)
                duration = rnd.uniform(120, 300)
                row = dict(base(t), artist=esc(artist), song=esc(song), duration=duration)
                topics["listen_events"].append(
                    dict(row, sessionid=sid, auth="Logged In"))
                topics["page_view_events"].append(
                    dict(row, sessionId=sid, page="NextSong", auth="Logged In",
                         method="PUT", status=200))
                # side pages
                for page, p_norm, p_churn in [("Thumbs Up", 0.13, 0.09),
                                              ("Thumbs Down", 0.04, 0.07),
                                              ("Help", 0.01, 0.025),
                                              ("Downgrade", 0.005, 0.02),
                                              ("Error", 0.005, 0.02)]:
                    if rnd.random() < (p_churn if churner else p_norm):
                        status = 404 if page == "Error" else 200
                        topics["page_view_events"].append(
                            dict(base(t + 1000), sessionId=sid, page=page,
                                 auth="Logged In", method="GET", status=status))

            if is_cancel_day:
                cancel_ts = max(t + 2000, cancel_from + 1)
                for page in ("Cancel", "Cancellation Confirmation"):
                    topics["page_view_events"].append(
                        dict(base(cancel_ts), sessionId=sid, page=page,
                             auth="Logged In" if page == "Cancel" else "Cancelled",
                             method="PUT", status=200 if page == "Cancel" else 307))
                    cancel_ts += 1000
                break  # user is gone
    return topics


def write(topics, out=OUT, files=FILES_PER_TOPIC):
    for topic, rows in topics.items():
        rows.sort(key=lambda r: r["ts"])
        os.makedirs(os.path.join(out, topic), exist_ok=True)
        size = -(-len(rows) // files)
        for f in range(files):
            with open(os.path.join(out, topic, f"part-{f}.json"), "w") as fh:
                for r in rows[f * size:(f + 1) * size]:
                    fh.write(json.dumps(r) + "\n")
        print(f"{topic}: {len(rows)} events")


def main(argv):
    out = argv[1] if len(argv) > 1 else OUT
    n_users = int(argv[2]) if len(argv) > 2 else N_USERS
    days = int(argv[3]) if len(argv) > 3 else DAYS
    seed = int(argv[4]) if len(argv) > 4 else SEED
    write(build(seed=seed, n_users=n_users, days=days), out)
    print("done", out)


if __name__ == "__main__":
    main(sys.argv)
