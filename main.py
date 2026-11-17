"""2v2 Clash Royale draft server (FastAPI + WebSockets, in-memory rooms).

Run:  CR_API_TOKEN=xxxx uvicorn main:app --host 0.0.0.0 --port 8000
Token comes from https://developer.clashroyale.com (key is bound to your server IP).
"""
import asyncio
import os
import random
import time
from pathlib import Path

import httpx
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.staticfiles import StaticFiles
from dotenv import load_dotenv


PICK_SECONDS, POOL_SIZE, DECK_SIZE = 20, 48, 8

app = FastAPI()
ROOMS: dict[str, "Room"] = {}
_cache = {"at": 0.0, "cards": []}


# ---------- cards ----------
async def get_cards() -> list[dict]:
    if _cache["cards"] and time.time() - _cache["at"] < 3600:
        return _cache["cards"]
    token = os.getenv("CR_API_TOKEN")
    if not token:
        raise RuntimeError(
            "Set CR_API_TOKEN (https://developer.clashroyale.com)")
    async with httpx.AsyncClient(timeout=15) as c:
        # or https://proxy.royaleapi.dev/v1
        base = os.getenv("CR_API_BASE", "https://api.clashroyale.com/v1")
        r = await c.get(f"{base}/cards", headers={"Authorization": f"Bearer {token}"})
        r.raise_for_status()
        cards = [{"id": i["id"], "name": i["name"], "img": i["iconUrls"]["medium"]}
                 for i in r.json()["items"]]
    _cache.update(at=time.time(), cards=cards)
    return cards


# ---------- draft order ----------
def build_order(teams: dict[str, list[str]], start: str, s_idx: dict[str, int]) -> list[str]:
    """Return the player id for each of the 32 picks, in order."""
    other = "B" if start == "R" else "R"
    to = [start, other]                                   # team order
    S = [teams[t][s_idx[t]] for t in to]                  # starters
    NS = [teams[t][1 - s_idx[t]] for t in to]             # non-starters
    order = list(S)                                       # 3. S pick 1 each
    for p in NS:                                          # 4. NS pick 2 each
        order += [p, p]
    have = {**{p: 1 for p in S}, **{p: 2 for p in NS}}
    goal = {**{p: 7 for p in S}, **{p: 6 for p in NS}}
    while any(have[p] < goal[p] for p in have):           # 5. 2 each until 7 / 6
        for p in S + NS:
            if have[p] < goal[p]:
                order += [p, p]
                have[p] += 2
    order += S + NS                                       # 7. everyone 1
    order += NS                                           # 8. NS 1 each
    assert len(order) == 4 * DECK_SIZE
    return order


# ---------- rooms ----------
class Player:
    def __init__(self, name):
        self.name, self.team, self.ws, self.picks = name, None, None, []


class Room:
    def __init__(self, code, host):
        self.code, self.host, self.phase = code, host, "lobby"
        self.players: dict[str, Player] = {}
        self.pool, self.taken, self.order = [], {}, []
        self.idx, self.deadline, self.timer, self.start_team = 0, 0.0, None, None

    def state(self):
        left = max(0, self.deadline - time.time()
                   ) if self.phase == "draft" else 0
        return {
            "t": "state", "code": self.code, "host": self.host, "phase": self.phase,
            "players": [{"id": i, "name": p.name, "team": p.team, "on": p.ws is not None}
                        for i, p in self.players.items()],
            "pool": [{**c, "by": self.taken.get(c["id"])} for c in self.pool],
            "picks": {i: p.picks for i, p in self.players.items()},
            "order": self.order, "idx": self.idx, "remaining": left,
            "seconds": PICK_SECONDS, "start": self.start_team,
        }


async def broadcast(room: Room):
    msg = room.state()
    for p in list(room.players.values()):
        if p.ws:
            try:
                await p.ws.send_json(msg)
            except Exception:
                p.ws = None


async def begin_turn(room: Room):
    room.deadline = time.time() + PICK_SECONDS
    room.timer = asyncio.create_task(expire(room, room.idx))
    await broadcast(room)


async def expire(room: Room, idx: int):
    await asyncio.sleep(PICK_SECONDS)
    if room.phase == "draft" and room.idx == idx:
        free = [c["id"] for c in room.pool if c["id"] not in room.taken]
        await do_pick(room, room.order[idx], random.choice(free))


async def do_pick(room: Room, pid: str, card_id: int):
    if room.timer and room.timer is not asyncio.current_task():
        room.timer.cancel()
    room.taken[card_id] = pid
    room.players[pid].picks.append(card_id)
    room.idx += 1
    if room.idx >= len(room.order):
        room.phase = "done"
        await broadcast(room)
    else:
        await begin_turn(room)


async def start_draft(room: Room):
    teams = {t: [i for i, p in room.players.items() if p.team == t]
             for t in "RB"}
    room.pool = random.sample(await get_cards(), POOL_SIZE)
    room.start_team = random.choice("RB")
    room.order = build_order(teams, room.start_team, {
                             t: random.randint(0, 1) for t in "RB"})
    room.phase, room.idx = "draft", 0
    await begin_turn(room)


# ---------- websocket ----------
@app.websocket("/ws")
async def ws_endpoint(ws: WebSocket):
    await ws.accept()
    room, pid = None, None

    async def err(text, fatal=False):
        await ws.send_json({"t": "error", "msg": text, "fatal": fatal})

    try:
        while True:
            m = await ws.receive_json()
            t = m.get("t")
            if t in ("create", "join"):
                pid, name = str(m.get("pid"))[:40], (str(
                    m.get("name") or "Player"))[:16]
                if t == "create":
                    code = next(c for c in (f"{random.randint(0, 9999):04d}" for _ in iter(int, 1))
                                if c not in ROOMS)
                    room = ROOMS[code] = Room(code, pid)
                else:
                    room = ROOMS.get(str(m.get("room")))
                    if not room:
                        await err("Room not found", fatal=True)
                        continue
                if pid not in room.players:
                    if room.phase != "lobby" or len(room.players) >= 4:
                        room = None
                        await err("Room is full or already drafting", fatal=True)
                        continue
                    room.players[pid] = Player(name)
                room.players[pid].ws = ws
                await broadcast(room)
            elif not room or pid not in room.players:
                continue
            elif t == "team" and room.phase == "lobby":
                team = m.get("team")
                if team in ("R", "B") and sum(p.team == team for p in room.players.values()) < 2:
                    room.players[pid].team = team
                    await broadcast(room)
                else:
                    await err("Team is full")
            elif t == "start" and room.phase == "lobby" and pid == room.host:
                if all(sum(p.team == x for p in room.players.values()) == 2 for x in "RB"):
                    await start_draft(room)
                else:
                    await err("Need 2 players on each team")
            elif t == "pick" and room.phase == "draft" and room.order[room.idx] == pid:
                cid = m.get("card")
                if any(c["id"] == cid for c in room.pool) and cid not in room.taken:
                    await do_pick(room, pid, cid)
    except WebSocketDisconnect:
        pass
    finally:
        p = room.players.get(pid) if room else None
        if p and p.ws is ws:
            p.ws = None
            if room.phase == "lobby":
                del room.players[pid]
                if room.host == pid and room.players:
                    room.host = next(iter(room.players))
            if not any(x.ws for x in room.players.values()):
                if room.timer:
                    room.timer.cancel()
                ROOMS.pop(room.code, None)
            else:
                await broadcast(room)

load_dotenv()
app.mount("/", StaticFiles(directory=Path(__file__).parent / "static", html=True))
