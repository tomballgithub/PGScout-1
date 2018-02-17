import json
import logging
import os
from base64 import b64decode
from cHaversine import haversine

import psutil
import requests

from pgscout.AppState import AppState
from pgscout.config import cfg_get

log = logging.getLogger(__name__)

app_state = AppState()

PRIO_HIGH = 0
PRIO_NORMAL = 1
PRIO_LOW = 2
PRIO_NAMES = {
    PRIO_HIGH: "High",
    PRIO_NORMAL: "Normal",
    PRIO_LOW: "Low",
}


def rss_mem_size():
    process = psutil.Process(os.getpid())
    mem = process.memory_info().rss
    unit = 'bytes'
    if mem > 1024:
        unit = 'KB'
        mem /= 1024
    if mem > 1024:
        unit = 'MB'
        mem /= 1024
    if mem > 1024:
        unit = 'GB'
        mem /= 1024
    return "{:>4.1f} {}".format(mem, unit)


def normalize_encounter_id(eid):
    if not eid:
        return eid
    try:
        return long(eid)
    except:
        return long(b64decode(eid))


def get_move_name(move_id):
    if not hasattr(get_move_name, 'mapping'):
        with open("pokemon_moves.json", 'r') as f:
            get_move_name.mapping = json.loads(f.read())
    return get_move_name.mapping.get(str(move_id))


def calc_pokemon_level(cp_multiplier):
    if cp_multiplier < 0.734:
        level = 58.35178527 * cp_multiplier * cp_multiplier - 2.838007664 * cp_multiplier + 0.8539209906
    else:
        level = 171.0112688 * cp_multiplier - 95.20425243
    level = (round(level) * 2) / 2.0
    return int(level)


def calc_iv(at, df, st):
    return float(at + df + st) / 45 * 100


def load_pgpool_accounts(count, reuse=False):
    addl_text = " Reusing previous accounts." if reuse else ""
    log.info("Trying to load {} accounts from PGPool.{}".format(count, addl_text))
    request = {
        'system_id': cfg_get('pgpool_system_id'),
        'count': count,
        'min_level': cfg_get('level'),
        'reuse': reuse
    }
    try:
        r = requests.get("{}/account/request".format(cfg_get('pgpool_url')), params=request)
        return r.json()
    except Exception as ex:
        log.info("PGPool account fetch error: {}".format(ex))
    return {}

def distance(pos1, pos2):
    return haversine((tuple(pos1))[0:2], (tuple(pos2))[0:2])


def get_pokemon_prio(pokemon_id):
    low_prio_pokemon = cfg_get('low_prio_pokemon')
    return PRIO_NORMAL if low_prio_pokemon and int(pokemon_id) not in low_prio_pokemon else PRIO_LOW
