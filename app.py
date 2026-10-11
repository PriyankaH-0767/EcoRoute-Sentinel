"""
EcoRoute Sentinel: AI-Predictive School Transport Pollution Mitigation Engine
=============================================================================
AWS Bharat Builds Tour: Environmental Hacks | Track 01: Air (School Safety / Pollution Exposure)

Run:      streamlit run app.py
Optional: docker run --rm -p 4566:4566 localstack/localstack   (live AWS plumbing mode)
Deps:     streamlit, pandas, boto3  (everything else is standard library)

ARCHITECTURE (logical view)
---------------------------
                 +-------------------+     +------------------------------+
  CPCB / wind    |  Environment      |     |  Domain engine (pure Python) |
  feeds (sim) -> |  snapshot (frozen)| --> |  1. Multi-factor risk matrix |
                 +-------------------+     |  2. 3h hazard forecast       |
                                           |  3. Mitigation optimiser     |
                                           +--------------+---------------+
                                                          |
        +-------------------------+     +-----------------v----------------+
        | EventBridge cron rule   | --> | Lambda "hazard-inference"        | --> SNS topic
        | (T-45 min per route)    |     | (scores fleet, flags > threshold)|     (SMS/Email/Push)
        +-------------------------+     +----------------------------------+

LAYERS IN THIS FILE
  A. Domain model + physics-inspired risk maths       (no I/O, fully deterministic, unit-testable)
  B. Decision engine (reroute / delay optimiser)      (no I/O)
  C. AWS plumbing: emulated trace + optional LocalStack execution via boto3
  D. Presentation: HTML/CSS cards, map, dataframes, tabs

HONESTY NOTE
  Air-quality inputs are simulated (sidebar). The "SageMaker" model is an analytic
  inference function packaged as a Lambda handler. Every infrastructure record is
  labelled `localstack` (a real API call returned it) or `emulated` (synthesised).
  SMS/push delivery receipts are always synthesised because LocalStack does not deliver them.
"""

from __future__ import annotations

import html
import io
import json
import math
import random
import re
import time
import uuid
import zipfile
import hashlib
import base64
from dataclasses import dataclass, field, replace
from datetime import datetime, timedelta, timezone, time as dtime
from typing import Optional

import boto3
import pandas as pd
import streamlit as st
from botocore.config import Config

st.set_page_config(
    page_title="EcoRoute Sentinel",
    page_icon="🛰️",
    layout="wide",
    initial_sidebar_state="expanded",
)

# =====================================================================================
# A. DOMAIN MODEL + RISK MATHS
# =====================================================================================

# ---- Tunable model constants (documented in the "Model card" on the AI tab) ----------
SOURCE_BEARING_DEG = 315.0          # Punjab/Haryana burn belt lies north-west of Delhi NCR
W_AQI, W_PM25, W_PLUME, W_TRAFFIC = 0.30, 0.28, 0.24, 0.18
SIGMOID_K, SIGMOID_X0 = 8.0, 0.52   # logistic squash: composite -> 0..100 hazard
PEAK_BLEND = 0.40                   # window hazard = 60% weighted mean + 40% worst moment
LOOKAHEAD_MIN, STEP_MIN = 180, 15   # 3-hour forward window, 15-minute resolution
HALF_LIFE_MIN = 120                 # exponential weighting favours imminent exposure
BREATHING_M3_PER_MIN = 0.0085       # ~8.5 L/min, child at light activity
CABIN_INFILTRATION = 0.85           # fraction of ambient PM2.5 reaching the bus cabin
NAAQS_PM25_24H = 60.0               # India NAAQS 24h limit, ug/m3 (for context only)
DELAY_OPTIONS_MIN = (15, 30, 45, 60, 90)

# Alternate "Low-Exposure Loop": interior roads, fewer arterial kilometres, a bit longer.
ALT_PROFILE = {"openness": 0.30, "cong": 0.80, "hotspot": 0.60, "arterial": 0.25, "time": 1.10}
BASE_PROFILE = {"openness": 1.0, "cong": 1.0, "hotspot": 1.0, "arterial": 1.0, "time": 1.0}

# CPCB India AQI breakpoints for PM2.5: (aqi_lo, aqi_hi, pm_lo, pm_hi)
CPCB_PM25_BREAKPOINTS = (
    (0, 50, 0, 30), (51, 100, 31, 60), (101, 200, 61, 90),
    (201, 300, 91, 120), (301, 400, 121, 250), (401, 500, 251, 380),
)

LOCALSTACK_ENDPOINT = "http://localhost:4566"
AWS_REGION = "ap-south-1"
AWS_ACCOUNT = "000000000000"
IST = timezone(timedelta(hours=5, minutes=30))


@dataclass(frozen=True)
class Route:
    id: str
    name: str
    area: str
    students: int
    depart_min: int          # scheduled departure, minutes after midnight (IST)
    base_min: int            # nominal trip length in free-flow conditions
    openness: float          # exposure to the NW smoke corridor (0..1)
    hotspot: float           # local emission load (industry, junctions) (0..1)
    arterial: float          # share of the trip on high-traffic arterials (0..1)
    cong_bias: float         # local congestion sensitivity
    waypoints: tuple         # ((lat, lon), ...) depot -> school


def _alt_loop(wps: tuple, k: float) -> tuple:
    """Synthetic interior-road geometry: interior vertices shifted inward from the arterial."""
    out = [wps[0]]
    for i, (la, lo) in enumerate(wps[1:-1], start=1):
        sign = 1 if i % 2 else -1
        out.append((la - 0.011 * k, lo + 0.009 * k * sign + 0.006 * k))
    out.append(wps[-1])
    return tuple(out)


FLEET: tuple[Route, ...] = (
    Route("R-01", "Rohini to NH-44 Corridor", "Rohini", 312, 6 * 60 + 55, 58, 0.90, 0.70, 0.85, 1.10,
          ((28.7383, 77.1130), (28.7152, 77.1470), (28.6935, 77.1650), (28.6780, 77.1890), (28.6640, 77.2120))),
    Route("R-02", "Dwarka to Najafgarh Road", "Dwarka", 276, 7 * 60 + 5, 52, 0.75, 0.55, 0.70, 1.00,
          ((28.5921, 77.0460), (28.6050, 77.0700), (28.6130, 77.0980), (28.6200, 77.1300), (28.6250, 77.1650))),
    Route("R-03", "Gurugram to NH-48 Link", "Gurugram", 348, 7 * 60 + 15, 64, 0.80, 0.85, 0.90, 1.20,
          ((28.4595, 77.0266), (28.4800, 77.0600), (28.5050, 77.0900), (28.5300, 77.1400), (28.5500, 77.1850))),
    Route("R-04", "Noida Sec-62 to DND", "Noida", 241, 7 * 60 + 25, 46, 0.35, 0.40, 0.45, 0.85,
          ((28.6270, 77.3650), (28.6130, 77.3400), (28.5950, 77.3100), (28.5800, 77.2850), (28.5700, 77.2500))),
    Route("R-05", "Ghaziabad to NH-9 Approach", "Ghaziabad", 295, 7 * 60 + 35, 55, 0.60, 0.75, 0.75, 1.05,
          ((28.6692, 77.4538), (28.6600, 77.4200), (28.6500, 77.3800), (28.6380, 77.3400), (28.6300, 77.3000))),
    Route("R-06", "Faridabad to Mathura Road", "Faridabad", 203, 7 * 60 + 45, 49, 0.10, 0.20, 0.25, 0.70,
          ((28.4089, 77.3178), (28.4300, 77.3000), (28.4600, 77.2800), (28.5000, 77.2600), (28.5400, 77.2450))),
)


@dataclass(frozen=True)
class Environment:
    now_min: int             # engine clock, minutes after midnight IST
    aqi: int                 # observed CPCB AQI
    wind_speed: float        # km/h
    wind_from: int           # meteorological direction wind blows FROM, degrees
    burn: float              # stubble-burning intensity 0..1


@dataclass(frozen=True)
class Conditions:
    aqi: float
    pm25: float
    plume: float
    cong: float
    wind_speed: float
    wind_from: float


def clamp(x: float, lo: float = 0.0, hi: float = 1.0) -> float:
    return max(lo, min(hi, x))


def sigmoid(x: float) -> float:
    return 1.0 / (1.0 + math.exp(-x))


def gauss(h: float, mu: float, sigma: float) -> float:
    d = (h - mu) / sigma
    return math.exp(-d * d)


def fmt_hm(minutes: float) -> str:
    m = int(round(minutes)) % (24 * 60)
    return f"{m // 60:02d}:{m % 60:02d}"


def aqi_to_pm25(aqi: float) -> float:
    """Invert the CPCB piecewise-linear PM2.5 sub-index to ug/m3."""
    aqi = clamp(aqi, 0, 500)
    for a_lo, a_hi, p_lo, p_hi in CPCB_PM25_BREAKPOINTS:
        if aqi <= a_hi:
            return p_lo + (aqi - a_lo) / max(1, a_hi - a_lo) * (p_hi - p_lo)
    return CPCB_PM25_BREAKPOINTS[-1][3]


def diurnal(h: float) -> float:
    """Boundary-layer cycle: pre-sunrise inversion peak, afternoon mixing dip, evening build-up."""
    return 1 + 0.32 * gauss(h, 7.0, 2.2) + 0.24 * gauss(h, 20.5, 2.0) - 0.30 * gauss(h, 15.0, 2.6)


def congestion(h: float, bias: float) -> float:
    """Time-of-day traffic multiplier (1.0 free flow .. 2.0 gridlock) with school/office peaks."""
    peaks = 0.75 * gauss(h, 8.5, 0.9) + 0.60 * gauss(h, 15.5, 1.1) + 0.35 * gauss(h, 18.5, 1.2)
    return min(2.0, 1.0 + bias * peaks)


def transport_efficiency(ws_kmh: float) -> float:
    """Smoke is carried best at moderate wind: calm air stalls it, gales dilute it."""
    return (1 - math.exp(-ws_kmh / 6.0)) * math.exp(-((ws_kmh / 28.0) ** 2))


def conditions_at(route: Route, env: Environment, t_abs_min: float, variant: str) -> Conditions:
    """Wind-vector plume model + diurnal AQI forecast + congestion, specialised to a route."""
    prof = ALT_PROFILE if variant == "ALT" else BASE_PROFILE
    h, h0 = t_abs_min / 60.0, env.now_min / 60.0

    aqi_f = env.aqi * diurnal(h) / diurnal(h0)
    aqi_loc = clamp(aqi_f * (0.92 + 0.16 * route.hotspot * prof["hotspot"]), 5, 500)

    # Wind strengthens as the mixing layer deepens (after ~08:00) and veers ~4 deg/hour.
    gain = lambda x: 1 + 0.35 * clamp((x - 8.0) / 4.0)
    ws = max(0.5, env.wind_speed * gain(h) / gain(h0))
    wd = (env.wind_from + 4.0 * (t_abs_min - env.now_min) / 60.0) % 360

    # Plume = burn intensity x directional alignment with the NW source x road exposure x transport.
    align = max(0.0, math.cos(math.radians(wd - SOURCE_BEARING_DEG))) ** 2
    plume = clamp(1.2 * env.burn * align * route.openness * prof["openness"] * transport_efficiency(ws))

    cong = congestion(h, route.cong_bias * prof["cong"])

    # Local PM2.5: CPCB inverse + plume loading + roadside street-canyon uplift on arterials.
    roadside = 1 + 0.28 * route.arterial * prof["arterial"] * (0.6 + (cong - 1))
    pm25 = aqi_to_pm25(aqi_loc) * (1 + 0.35 * plume) * roadside
    return Conditions(aqi_loc, pm25, plume, cong, ws, wd)


def factor_scores(c: Conditions) -> dict:
    return {
        "AQI": clamp(c.aqi / 400.0),
        "PM2.5": clamp(c.pm25 / 180.0),
        "Plume": c.plume,
        "Traffic": clamp(c.cong - 1.0),
    }


def composite(f: dict) -> float:
    return W_AQI * f["AQI"] + W_PM25 * f["PM2.5"] + W_PLUME * f["Plume"] + W_TRAFFIC * f["Traffic"]


def instant_hazard(c: Conditions) -> float:
    return 100.0 * sigmoid(SIGMOID_K * (composite(factor_scores(c)) - SIGMOID_X0))


@dataclass
class Evaluation:
    variant: str
    depart_min: int
    delay_min: int
    hazard: float                       # 0..100 3-hour Student Exposure Hazard
    series: list                        # [(offset_min, instantaneous hazard)]
    mean_factors: dict                  # time-weighted factor scores
    cond0: Conditions                   # conditions at departure
    trip_min: float
    dose_ug: float                      # PM2.5 inhaled per student on this trip


def evaluate(route: Route, env: Environment, depart_min: int, variant: str, delay_min: int = 0) -> Evaluation:
    """
    The 'inference' step. Mock-SageMaker contract:
      input  = (route, environment snapshot, planned departure, route variant)
      output = Student Exposure Hazard (0..100) over the next 3 hours + inhaled dose estimate.
    """
    offsets = list(range(0, LOOKAHEAD_MIN + 1, STEP_MIN))
    conds = [conditions_at(route, env, depart_min + o, variant) for o in offsets]
    inst = [instant_hazard(c) for c in conds]
    w = [math.exp(-o / HALF_LIFE_MIN) for o in offsets]
    wmean = sum(wi * hi for wi, hi in zip(w, inst)) / sum(w)
    hazard = (1 - PEAK_BLEND) * wmean + PEAK_BLEND * max(inst)

    fac = [factor_scores(c) for c in conds]
    mean_f = {k: sum(wi * f[k] for wi, f in zip(w, fac)) / sum(w) for k in fac[0]}

    prof = ALT_PROFILE if variant == "ALT" else BASE_PROFILE
    mid = conditions_at(route, env, depart_min + route.base_min / 2, variant)
    trip = route.base_min * prof["time"] * (1 + 0.35 * (mid.cong - 1))
    dose, t = 0.0, 0.0
    while t < trip:
        step = min(5.0, trip - t)
        c = conditions_at(route, env, depart_min + t, variant)
        dose += c.pm25 * CABIN_INFILTRATION * BREATHING_M3_PER_MIN * step
        t += step
    return Evaluation(variant, depart_min, delay_min, hazard, list(zip(offsets, inst)), mean_f, conds[0], trip, dose)


@dataclass
class Decision:
    route: Route
    baseline: Evaluation
    final: Evaluation
    action: str                         # NONE | REROUTE | DELAY | REROUTE+DELAY | ESCALATE
    reduction_pct: float
    candidates: list = field(default_factory=list)
    triggered: bool = False

    @property
    def status(self) -> str:            # computed vs the *unmitigated* threat
        return self._status

    _status: str = "safe"


# =====================================================================================
# B. DECISION ENGINE
# =====================================================================================

def optimise(route: Route, env: Environment, threshold: float, max_delay: int) -> Decision:
    """
    Rerouting engine. If baseline hazard > threshold, enumerate:
        {Low-Exposure Loop, original road} x {0, 15, 30, 45, 60, 90 min hold}  (<= max_delay)
    Pick the plan with the lowest delay-penalised inhaled dose among those that clear the
    threshold; if none clear it, take the least-bad plan. If it barely helps, escalate to a human.
    """
    base = evaluate(route, env, route.depart_min, "BASE", 0)
    status = "crit" if base.hazard > threshold else ("warn" if base.hazard > 0.6 * threshold else "safe")
    if base.hazard <= threshold:
        d = Decision(route, base, base, "NONE", 0.0, [], False)
        d._status = status
        return d

    cands = []
    for variant in ("ALT", "BASE"):
        for delay in (0,) + DELAY_OPTIONS_MIN:
            if delay > max_delay or (variant == "BASE" and delay == 0):
                continue
            cands.append(evaluate(route, env, route.depart_min + delay, variant, delay))

    cost = lambda e: e.dose_ug * (1 + 0.35 * e.delay_min / 60.0)
    clearing = [e for e in cands if e.hazard <= threshold]
    best = min(clearing or cands, key=cost)
    reduction = 100.0 * (1 - best.dose_ug / base.dose_ug)

    if reduction < 3.0:
        action = "ESCALATE"
    elif best.variant == "ALT" and best.delay_min:
        action = "REROUTE+DELAY"
    elif best.variant == "ALT":
        action = "REROUTE"
    else:
        action = "DELAY"
    d = Decision(route, base, best if action != "ESCALATE" else base, action,
                 max(0.0, reduction), cands, True)
    d._status = status
    return d


def plan_text(d: Decision) -> str:
    if d.action == "NONE":
        return "Run as scheduled"
    if d.action == "ESCALATE":
        return "Escalate to transport coordinator"
    parts = []
    if d.final.variant == "ALT":
        parts.append("Low-Exposure Loop")
    if d.final.delay_min:
        parts.append(f"hold {d.final.delay_min} min")
    return " + ".join(parts)


@st.cache_data(show_spinner=False)
def monte_carlo_band(route: Route, env: Environment, variant: str, depart_min: int, n: int = 240) -> pd.DataFrame:
    """Uncertainty band: perturb AQI, wind speed/direction and burn intensity, rescore the window."""
    rng = random.Random(42)
    offsets = list(range(0, LOOKAHEAD_MIN + 1, STEP_MIN))
    cols = [[] for _ in offsets]
    for _ in range(n):
        e = replace(
            env,
            aqi=int(clamp(env.aqi * rng.lognormvariate(0, 0.10), 5, 500)),
            wind_speed=max(0.5, env.wind_speed * rng.lognormvariate(0, 0.25)),
            wind_from=int((env.wind_from + rng.gauss(0, 18)) % 360),
            burn=clamp(env.burn + rng.gauss(0, 0.12)),
        )
        for i, o in enumerate(offsets):
            cols[i].append(instant_hazard(conditions_at(route, e, depart_min + o, variant)))
    pct = lambda xs, q: sorted(xs)[min(len(xs) - 1, int(q * len(xs)))]
    return pd.DataFrame(
        {"P10": [pct(c, .10) for c in cols], "P50 (expected)": [pct(c, .50) for c in cols],
         "P90": [pct(c, .90) for c in cols]},
        index=pd.Index(offsets, name="Minutes after departure"),
    )


# =====================================================================================
# C. AWS PLUMBING: EMULATED TRACE + OPTIONAL LOCALSTACK EXECUTION
# =====================================================================================

TOPIC_NAME = "ecoroute-sentinel-guardian-alerts"
TOPIC_ARN = f"arn:aws:sns:{AWS_REGION}:{AWS_ACCOUNT}:{TOPIC_NAME}"
FUNCTION_NAME = "ecoroute-sentinel-hazard-inference"
FUNCTION_ARN = f"arn:aws:lambda:{AWS_REGION}:{AWS_ACCOUNT}:function:{FUNCTION_NAME}"
LAMBDA_ROLE = f"arn:aws:iam::{AWS_ACCOUNT}:role/ecoroute-sentinel-exec"

# Real handler that gets deployed into LocalStack's Lambda service in live mode.
LAMBDA_HANDLER_SRC = '''\
import json

def handler(event, context):
    routes = event.get("routes", [])
    thr = event.get("threshold", 75)
    flagged = [r["id"] for r in routes if r.get("hazard", 0) > thr]
    print(json.dumps({"level": "INFO", "msg": "hazard_scan", "scored": len(routes), "flagged": flagged}))
    return {"statusCode": 200, "flagged": flagged,
            "function_version": context.function_version, "request_id": context.aws_request_id}
'''


def _client(service: str):
    """boto3 client pinned to LocalStack with dummy credentials and fast-fail networking."""
    return boto3.client(
        service,
        endpoint_url=LOCALSTACK_ENDPOINT,
        region_name=AWS_REGION,
        aws_access_key_id="test",
        aws_secret_access_key="test",
        config=Config(connect_timeout=1, read_timeout=30, retries={"max_attempts": 1}),
    )


@st.cache_data(ttl=10, show_spinner=False)
def probe_localstack() -> dict:
    t0 = time.perf_counter()
    try:
        _client("events").list_rules(Limit=1)
        return {"up": True, "latency_ms": round((time.perf_counter() - t0) * 1000, 1), "detail": "EventBridge reachable"}
    except Exception as exc:  # noqa: BLE001 - any failure means 'offline', by design
        return {"up": False, "latency_ms": round((time.perf_counter() - t0) * 1000, 1), "detail": type(exc).__name__}


def _rng_for(key: str) -> random.Random:
    return random.Random(int(hashlib.sha256(key.encode()).hexdigest()[:12], 16))


def _uuid(rng: random.Random) -> str:
    return str(uuid.UUID(int=rng.getrandbits(128), version=4))


def _mask_phone(rng: random.Random) -> str:
    return f"+9198{rng.randint(10, 99)}XXXX{rng.randint(10, 99)}"


def compose_alert(route: Optional[Route], d: Optional[Decision], fleet_summary: str) -> dict:
    """Customised multi-protocol payloads (SNS MessageStructure=json)."""
    if route is None or d is None:
        sms = f"EcoRoute: {fleet_summary} No schedule changes today."
        subject = "[EcoRoute Sentinel] All clear: buses run as scheduled"
        email = f"Hello,\n\n{fleet_summary}\nNo schedule changes are required.\n\nEcoRoute Sentinel"
        title, body, rid, action = "All clear", fleet_summary, "FLEET", "NONE"
    else:
        old, new = fmt_hm(route.depart_min), fmt_hm(d.final.depart_min)
        way = "via the Low-Exposure Loop" if d.final.variant == "ALT" else "on the usual road"
        if d.action == "ESCALATE":
            sms = f"EcoRoute: Bus {route.id} ({route.area}) has high smoke exposure. Coordinator reviewing; stay tuned."
            subject = f"[EcoRoute Sentinel] Bus {route.id}: schedule under review"
            title, body = f"Bus {route.id} under review", "High smoke exposure forecast. A coordinator is reviewing the plan."
            email = f"Hello,\n\nBus {route.id} ({route.name}) faces a high smoke-exposure forecast.\nA transport coordinator is reviewing options and will confirm shortly.\n\nEcoRoute Sentinel"
        else:
            when = f"now departs {new} (was {old})" if new != old else f"still departs {new}"
            sms = (f"EcoRoute: Bus {route.id} ({route.area}) {when} {way}. "
                   f"Child PM2.5 exposure cut {d.reduction_pct:.0f}%.")
            subject = f"[EcoRoute Sentinel] Bus {route.id} schedule change: departs {new} {way}"
            title = f"Bus {route.id}: departs {new}"
            body = f"Now {way}. Exposure cut {d.reduction_pct:.0f}% (was {old})."
            email = (f"Hello,\n\nBecause of a forecast smoke-exposure hazard of {d.baseline.hazard:.0f}% for "
                     f"{route.name}, we changed today's plan.\n\n"
                     f"  Scheduled departure : {old}\n  New departure       : {new}\n"
                     f"  Route               : {'Low-Exposure Loop (interior roads)' if d.final.variant == 'ALT' else 'Usual road'}\n"
                     f"  Hazard after change : {d.final.hazard:.0f}%\n  Exposure reduction  : {d.reduction_pct:.0f}%\n\n"
                     f"Please have your child ready by {fmt_hm(d.final.depart_min - 10)}.\n\nEcoRoute Sentinel")
        rid, action = route.id, d.action
    sms = sms[:160]
    return {
        "route_id": rid, "action": action, "subject": subject,
        "structure": {
            "default": sms, "sms": sms, "email": email,
            "GCM": json.dumps({"notification": {"title": title, "body": body, "android_channel_id": "bus-alerts"},
                               "data": {"route_id": rid, "action": action}}),
            "APNS": json.dumps({"aps": {"alert": {"title": title, "body": body}, "sound": "default",
                                        "thread-id": rid, "interruption-level": "time-sensitive"}}),
        },
    }


def build_emulated_trace(decisions: list, env: Environment, threshold: float, compute_ms: float,
                         cold_start: bool, seed_key: str) -> dict:
    """Deterministic, realistic AWS records. Replaced field-by-field by real ones in live mode."""
    rng = _rng_for(seed_key)
    now_utc = datetime.now(timezone.utc)
    stamp = lambda dt: dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{dt.microsecond // 1000:03d}Z"

    # ---- EventBridge: one cron rule per route, firing 45 min before scheduled departure ----
    rules = []
    for d in decisions:
        trig_ist = d.route.depart_min - 45
        utc_total = (trig_ist - 330) % (24 * 60)
        name = f"ecoroute-predispatch-{d.route.id.lower()}"
        cron = f"cron({utc_total % 60} {utc_total // 60} ? * MON-SAT *)"
        rules.append({
            "source": "emulated", "route_id": d.route.id,
            "request": {"Name": name, "ScheduleExpression": cron, "State": "ENABLED",
                        "Description": f"Pre-dispatch hazard scoring, {fmt_hm(trig_ist)} IST for {d.route.name}"},
            "response": {"RuleArn": f"arn:aws:events:{AWS_REGION}:{AWS_ACCOUNT}:rule/{name}",
                         "ResponseMetadata": {"RequestId": _uuid(rng), "HTTPStatusCode": 200}},
        })
    first = rules[0]
    event_id = _uuid(rng)
    scheduled_event = {
        "version": "0", "id": event_id, "detail-type": "Scheduled Event", "source": "aws.events",
        "account": AWS_ACCOUNT, "time": now_utc.strftime("%Y-%m-%dT%H:%M:%SZ"), "region": AWS_REGION,
        "resources": [first["response"]["RuleArn"]], "detail": {},
    }

    # ---- Lambda: structured logs + billing report ----
    rid = _uuid(rng)
    version = str(rng.randint(11, 27))
    duration = round(compute_ms + rng.uniform(18, 42), 2)
    billed = math.ceil(duration)
    init = round(rng.uniform(280, 410), 2) if cold_start else None
    mem_used = 74 + len(decisions) * 4 + rng.randint(0, 9)
    flagged = [d.route.id for d in decisions if d.triggered]
    lines = [f"START RequestId: {rid} Version: {version}"]
    ts = now_utc
    lines.append(json.dumps({"level": "INFO", "ts": stamp(ts), "msg": "eventbridge_trigger", "rule": first["request"]["Name"],
                             "event_id": event_id, "requestId": rid}))
    ts += timedelta(milliseconds=6)
    lines.append(json.dumps({"level": "INFO", "ts": stamp(ts), "msg": "inference_complete", "model": "hazard-v2.3.1-analytic",
                             "routes_scored": len(decisions), "flagged": flagged, "latency_ms": round(compute_ms, 2),
                             "threshold": threshold, "requestId": rid}))
    for d in decisions:
        if d.triggered:
            ts += timedelta(milliseconds=3)
            lines.append(json.dumps({"level": "WARN", "ts": stamp(ts), "msg": "mitigation_applied", "route": d.route.id,
                                     "action": d.action, "hazard_before": round(d.baseline.hazard, 1),
                                     "hazard_after": round(d.final.hazard, 1), "exposure_reduction_pct": round(d.reduction_pct, 1),
                                     "new_departure": fmt_hm(d.final.depart_min), "requestId": rid}))
    lines.append(json.dumps({"level": "INFO", "ts": stamp(ts + timedelta(milliseconds=4)), "msg": "sns_publish_enqueued",
                             "topic": TOPIC_ARN, "requestId": rid}))
    lines.append(f"END RequestId: {rid}")
    report = (f"REPORT RequestId: {rid}\tDuration: {duration:.2f} ms\tBilled Duration: {billed} ms\t"
              f"Memory Size: 512 MB\tMax Memory Used: {mem_used} MB" + (f"\tInit Duration: {init:.2f} ms" if init else ""))
    lines.append(report)
    gb_s = (512 / 1024) * (billed + (math.ceil(init) if init else 0)) / 1000
    metrics = {
        "functionName": FUNCTION_NAME, "functionVersion": version,
        "invokedFunctionArn": f"{FUNCTION_ARN}:{version}", "runtime": "python3.12", "architecture": "arm64",
        "memorySizeMB": 512, "maxMemoryUsedMB": mem_used, "durationMs": duration, "billedDurationMs": billed,
        "initDurationMs": init, "coldStart": cold_start, "requestId": rid,
        "estimatedCostUSD": round(gb_s * 0.0000133334 + 0.20 / 1_000_000, 10),
    }

    # ---- SNS: one publish per actioned route (or a single fleet all-clear) ----
    actioned = [d for d in decisions if d.action != "NONE"]
    if actioned:
        alerts = [compose_alert(d.route, d, "") for d in actioned]
    else:
        alerts = [compose_alert(None, None, f"All {len(decisions)} routes are below the {threshold:.0f}% hazard threshold.")]
    sns = []
    for a in alerts:
        mid = _uuid(rng)
        phone = _mask_phone(rng)
        t_pub = now_utc + timedelta(milliseconds=rng.randint(40, 90))
        sns.append({
            "source": "emulated", "route_id": a["route_id"],
            "request": {
                "TopicArn": TOPIC_ARN, "Subject": a["subject"], "MessageStructure": "json", "Message": a["structure"],
                "MessageAttributes": {
                    "AWS.SNS.SMS.SMSType": {"DataType": "String", "StringValue": "Transactional"},
                    "AWS.SNS.SMS.SenderID": {"DataType": "String", "StringValue": "ECOSNTL"},
                    "AWS.MM.SMS.EntityId": {"DataType": "String", "StringValue": "1101000000000000000"},
                    "AWS.MM.SMS.TemplateId": {"DataType": "String", "StringValue": "1107000000000000000"},
                    "route_id": {"DataType": "String", "StringValue": a["route_id"]},
                    "action": {"DataType": "String", "StringValue": a["action"]},
                },
            },
            "response": {"MessageId": mid, "ResponseMetadata": {"RequestId": _uuid(rng), "HTTPStatusCode": 200, "RetryAttempts": 0}},
            "delivery_receipts": {
                "sms (synthesised)": {
                    "notification": {"messageId": mid, "topicArn": TOPIC_ARN, "timestamp": t_pub.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]},
                    "delivery": {"phoneCarrier": rng.choice(["Jio", "Airtel", "Vi India"]), "destination": phone,
                                 "numberOfMessageParts": 1, "smsType": "Transactional", "priceInUSD": 0.00277,
                                 "providerResponse": "Message has been accepted by phone carrier",
                                 "dwellTimeMs": rng.randint(300, 900), "dwellTimeMsUntilDeviceAck": rng.randint(2100, 4800)},
                    "status": "SUCCESS"},
                "email (accepted by SNS)": {"note": "SNS does not emit per-recipient email delivery logs; message queued to endpoint.",
                                            "destination": "guardians+" + a["route_id"].lower() + "@ecoroute.example"},
                "push-gcm (synthesised)": {
                    "notification": {"messageId": mid, "topicArn": TOPIC_ARN},
                    "delivery": {"deliveryId": _uuid(rng), "statusCode": 200, "dwellTimeMs": rng.randint(80, 240),
                                 "destination": f"arn:aws:sns:{AWS_REGION}:{AWS_ACCOUNT}:endpoint/GCM/ecoroute-parent-app/{_uuid(rng)}",
                                 "providerResponse": json.dumps({"multicast_id": rng.getrandbits(60), "success": 1, "failure": 0})},
                    "status": "SUCCESS"},
            },
        })

    return {
        "mode": {"eventbridge": "emulated", "lambda": "emulated", "sns": "emulated"},
        "eventbridge": {"rules": rules, "delivered_event": scheduled_event},
        "lambda": {"log_lines": lines, "metrics": metrics, "response": {"statusCode": 200, "flagged": flagged,
                                                                          "function_version": version, "request_id": rid},
                   "source": "emulated"},
        "sns": sns, "errors": [],
    }


def run_live_pipeline(trace: dict, decisions: list, threshold: float) -> dict:
    """
    Replace emulated sections with real LocalStack API responses where they succeed.
    Every service is attempted independently so a missing Lambda runtime (Docker) never blocks SNS/EventBridge.
    """
    errors = trace["errors"]
    lam_arn = FUNCTION_ARN

    # -- Lambda: deploy real handler then invoke it with a scheduled-event style payload --
    try:
        lam = _client("lambda")
        try:
            cfg = lam.get_function(FunctionName=FUNCTION_NAME)["Configuration"]
        except Exception:  # noqa: BLE001 - not found -> create
            buf = io.BytesIO()
            with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
                z.writestr("handler.py", LAMBDA_HANDLER_SRC)
            cfg = lam.create_function(FunctionName=FUNCTION_NAME, Runtime="python3.12", Role=LAMBDA_ROLE,
                                      Handler="handler.handler", Code={"ZipFile": buf.getvalue()},
                                      Timeout=10, MemorySize=512, Publish=True)
        lam_arn = cfg["FunctionArn"]
        for _ in range(12):  # wait (max ~6s) for the function to become Active
            state = lam.get_function(FunctionName=FUNCTION_NAME)["Configuration"].get("State", "Active")
            if state in ("Active", None):
                break
            time.sleep(0.5)
        payload = {"source": "aws.events", "detail-type": "Scheduled Event", "threshold": threshold,
                   "routes": [{"id": d.route.id, "hazard": round(d.baseline.hazard, 1), "action": d.action} for d in decisions]}
        resp = lam.invoke(FunctionName=FUNCTION_NAME, InvocationType="RequestResponse", LogType="Tail",
                          Payload=json.dumps(payload).encode())
        body = json.loads(resp["Payload"].read() or b"{}")
        tail = base64.b64decode(resp.get("LogResult", "")).decode(errors="replace").strip().splitlines()
        if resp.get("FunctionError"):
            raise RuntimeError(f"Lambda FunctionError: {body}")
        trace["lambda"]["response"] = body
        if tail:
            trace["lambda"]["log_lines"] = tail
        trace["lambda"]["metrics"]["functionVersion"] = resp.get("ExecutedVersion", "$LATEST")
        trace["lambda"]["metrics"]["invokedFunctionArn"] = lam_arn
        m = re.search(r"Duration: ([\d.]+) ms.*?Billed Duration: (\d+) ms.*?Memory Size: (\d+) MB.*?Max Memory Used: (\d+) MB",
                      "\n".join(tail), re.S)
        if m:
            trace["lambda"]["metrics"].update({"durationMs": float(m[1]), "billedDurationMs": int(m[2]),
                                               "memorySizeMB": int(m[3]), "maxMemoryUsedMB": int(m[4])})
        trace["lambda"]["source"] = "localstack"
        trace["mode"]["lambda"] = "localstack"
    except Exception as exc:  # noqa: BLE001
        errors.append(f"Lambda: {type(exc).__name__}: {str(exc)[:160]} (kept emulated record)")

    # -- EventBridge: real put_rule + put_targets --
    try:
        ev = _client("events")
        for r in trace["eventbridge"]["rules"]:
            req = r["request"]
            resp = ev.put_rule(Name=req["Name"], ScheduleExpression=req["ScheduleExpression"], State="ENABLED",
                               Description=req["Description"])
            ev.put_targets(Rule=req["Name"], Targets=[{"Id": "hazard-inference", "Arn": lam_arn,
                                                       "Input": json.dumps({"route_id": r["route_id"], "threshold": threshold})}])
            r["response"] = {"RuleArn": resp["RuleArn"], "ResponseMetadata": {
                "RequestId": resp["ResponseMetadata"].get("RequestId", ""), "HTTPStatusCode": resp["ResponseMetadata"]["HTTPStatusCode"]}}
            r["source"] = "localstack"
        trace["eventbridge"]["rules"][0]["response"]["RuleArn"] and trace["eventbridge"]["delivered_event"].update(
            {"resources": [trace["eventbridge"]["rules"][0]["response"]["RuleArn"]]})
        trace["mode"]["eventbridge"] = "localstack"
    except Exception as exc:  # noqa: BLE001
        errors.append(f"EventBridge: {type(exc).__name__}: {str(exc)[:160]} (kept emulated record)")

    # -- SNS: real topic, subscriptions and publish with per-protocol JSON structure --
    try:
        sns = _client("sns")
        topic_arn = sns.create_topic(Name=TOPIC_NAME)["TopicArn"]
        for proto, ep in (("email", "guardians@ecoroute.example"), ("sms", "+919800000042")):
            try:
                sns.subscribe(TopicArn=topic_arn, Protocol=proto, Endpoint=ep)
            except Exception:  # noqa: BLE001 - duplicate/pending subscription is fine
                pass
        for rec in trace["sns"]:
            req = rec["request"]
            resp = sns.publish(
                TopicArn=topic_arn, Subject=req["Subject"][:99], MessageStructure="json",
                Message=json.dumps(req["Message"]),
                MessageAttributes={k: v for k, v in req["MessageAttributes"].items() if not k.startswith("AWS.")},
            )
            req["TopicArn"] = topic_arn
            rec["response"] = {"MessageId": resp["MessageId"], "ResponseMetadata": {
                "RequestId": resp["ResponseMetadata"].get("RequestId", ""), "HTTPStatusCode": resp["ResponseMetadata"]["HTTPStatusCode"],
                "RetryAttempts": 0}}
            for receipt in rec["delivery_receipts"].values():
                if "notification" in receipt:
                    receipt["notification"]["messageId"] = resp["MessageId"]
            rec["source"] = "localstack"
        trace["mode"]["sns"] = "localstack"
    except Exception as exc:  # noqa: BLE001
        errors.append(f"SNS: {type(exc).__name__}: {str(exc)[:160]} (kept emulated record)")
    return trace


# =====================================================================================
# D. PRESENTATION LAYER
# =====================================================================================

PRESETS = {
    "Stubble plume event (critical)": dict(aqi=318, wind=10, wdir=308, burn=80, hh=6, mm=30),
    "Winter smog surge": dict(aqi=268, wind=5, wdir=290, burn=35, hh=6, mm=30),
    "Clear monsoon-wash day": dict(aqi=88, wind=15, wdir=210, burn=5, hh=6, mm=30),
    "Custom": dict(aqi=300, wind=9, wdir=305, burn=70, hh=6, mm=30),
}

STATUS_META = {
    "safe": ("Safe", "#2ecc8c"),
    "warn": ("Watch", "#ffb020"),
    "crit": ("Critical", "#ff4d4f"),
}

CSS = """
<style>
@import url('https://fonts.googleapis.com/css2?family=Bricolage+Grotesque:opsz,wght@12..96,500;12..96,700;12..96,800&family=Figtree:wght@400;500;600&display=swap');
.eco-banner{border-radius:16px;padding:22px 26px;margin:0 0 18px;color:#e9f4ef;
 background:linear-gradient(115deg,#09201b 0%,#0f3029 52%,#0c2b3b 100%);font-family:'Figtree',system-ui,sans-serif;
 display:flex;flex-wrap:wrap;gap:16px;justify-content:space-between;align-items:center}
.eco-banner h1{font-family:'Bricolage Grotesque',system-ui,sans-serif;font-weight:800;font-size:2.05rem;line-height:1.1;margin:0;color:#f3fbf7;letter-spacing:-.01em}
.eco-banner p{margin:6px 0 0;color:#a9c7bc;font-size:.98rem;max-width:62ch}
.eco-pills{display:flex;flex-wrap:wrap;gap:8px}
.eco-pill{border:1px solid rgba(255,255,255,.2);border-radius:999px;padding:5px 12px;font-size:.82rem;color:#d9ece5;background:rgba(255,255,255,.05)}
.eco-pill b{font-weight:600}
.eco-dot{display:inline-block;width:8px;height:8px;border-radius:50%;margin-right:7px;vertical-align:baseline}
.eco-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(270px,1fr));gap:18px;margin:10px 0 22px}
.eco-card{position:relative;border-radius:14px;padding:16px 18px 14px;background:rgba(127,127,127,.07);
 border:1.5px solid var(--c);box-shadow:0 0 18px var(--glow);font-family:'Figtree',system-ui,sans-serif;color:inherit}
.eco-card.safe{--c:rgba(46,204,140,.70);--glow:rgba(46,204,140,.20)}
.eco-card.warn{--c:rgba(255,176,32,.80);--glow:rgba(255,176,32,.22)}
.eco-card.crit{--c:rgba(255,77,79,.90);--glow:rgba(255,77,79,.35);animation:eco-aura 1.9s ease-in-out infinite}
@keyframes eco-aura{0%,100%{box-shadow:0 0 0 0 rgba(255,77,79,.50),0 0 18px rgba(255,77,79,.30)}
 50%{box-shadow:0 0 0 12px rgba(255,77,79,0),0 0 36px rgba(255,77,79,.65)}}
@media (prefers-reduced-motion:reduce){.eco-card.crit{animation:none}}
.eco-head{display:flex;justify-content:space-between;align-items:flex-start;gap:10px}
.eco-id{font-family:'Bricolage Grotesque',system-ui,sans-serif;font-weight:700;font-size:1.05rem}
.eco-sub{font-size:.83rem;opacity:.7;margin-top:1px}
.eco-tag{font-size:.78rem;font-weight:600;padding:3px 10px;border-radius:999px;border:1px solid var(--c);white-space:nowrap}
.eco-big{font-family:'Bricolage Grotesque',system-ui,sans-serif;font-weight:800;font-size:2.7rem;line-height:1;margin:12px 0 2px;letter-spacing:-.02em}
.eco-big small{font-size:1rem;font-weight:600;opacity:.65;margin-left:4px}
.eco-was{font-size:.82rem;opacity:.75;margin-bottom:8px}
.eco-gauge{height:7px;border-radius:99px;background:rgba(127,127,127,.22);overflow:hidden;margin:6px 0 12px}
.eco-gauge i{display:block;height:100%;border-radius:99px;background:var(--bar)}
.eco-chips{display:grid;grid-template-columns:repeat(4,1fr);gap:6px;margin-bottom:10px}
.eco-chip{border-radius:9px;padding:6px 4px;background:rgba(127,127,127,.10);text-align:center}
.eco-chip b{display:block;font-size:.98rem;font-weight:600}
.eco-chip span{font-size:.72rem;opacity:.68}
.eco-act{border-radius:9px;padding:8px 10px;font-size:.84rem;line-height:1.35;background:rgba(127,127,127,.10);border-left:3px solid var(--c)}
.eco-flow{display:grid;grid-template-columns:repeat(auto-fit,minmax(230px,1fr));gap:12px;margin:6px 0 14px;font-family:'Figtree',system-ui,sans-serif}
.eco-node{border-radius:12px;padding:12px 14px;background:rgba(127,127,127,.08);border:1px solid rgba(127,127,127,.28)}
.eco-node h4{margin:0 0 4px;font-family:'Bricolage Grotesque',system-ui,sans-serif;font-size:1.02rem}
.eco-node p{margin:0;font-size:.85rem;opacity:.78}
</style>
"""


def show_df(df, **kwargs):
    """st.dataframe wrapper that works across Streamlit versions (width API changed in 1.4x)."""
    try:
        st.dataframe(df, width="stretch", **kwargs)
    except Exception:  # noqa: BLE001
        st.dataframe(df, use_container_width=True, **kwargs)


def heat_style(df: pd.DataFrame):
    def paint(v):
        try:
            x = clamp(float(v) / 100.0)
        except (TypeError, ValueError):
            return ""
        r, g, b = (46, 204, 140) if x < .45 else ((255, 176, 32) if x < .75 else (255, 77, 79))
        return f"background-color: rgba({r},{g},{b},{0.18 + 0.35 * x:.2f}); font-weight:600"
    sty = df.style
    return (sty.map if hasattr(sty, "map") else sty.applymap)(paint).format("{:.0f}%")


def route_card_html(d: Decision, threshold: float) -> str:
    label, color = STATUS_META[d.status]
    r, b, f = d.route, d.baseline, d.final
    c = b.cond0
    shown = f.hazard if d.triggered and d.action != "ESCALATE" else b.hazard
    if d.action == "NONE":
        act = f"On schedule at {fmt_hm(r.depart_min)}. Hazard is under the {threshold:.0f}% alert line."
    elif d.action == "ESCALATE":
        act = "No plan cuts exposure by 3% or more. Sent to the transport coordinator."
    else:
        when = (f"Departs {fmt_hm(f.depart_min)} (was {fmt_hm(r.depart_min)})" if f.delay_min
                else f"Departs on time at {fmt_hm(r.depart_min)}")
        act = (f"<b>Mitigated:</b> {html.escape(plan_text(d))}. {when}. Exposure down <b>{d.reduction_pct:.0f}%</b>.")
    was = (f"<div class='eco-was'>Unmitigated forecast {b.hazard:.0f}%</div>" if d.triggered and d.action != "ESCALATE"
           else "<div class='eco-was'>3-hour forecast</div>")
    chips = "".join(
        f"<div class='eco-chip'><b>{v}</b><span>{k}</span></div>"
        for k, v in (("AQI", f"{c.aqi:.0f}"), ("PM2.5", f"{c.pm25:.0f}"),
                     ("Plume", f"{c.plume * 100:.0f}%"), ("Traffic", f"x{c.cong:.2f}"))
    )
    return (
        f"<div class='eco-card {d.status}' role='group' aria-label='Route {r.id} hazard {shown:.0f} percent, {label}'>"
        f"<div class='eco-head'><div><div class='eco-id'>{r.id} {html.escape(r.area)}</div>"
        f"<div class='eco-sub'>{html.escape(r.name)}, {r.students} students</div></div>"
        f"<span class='eco-tag' style='color:{color}'>{label}</span></div>"
        f"<div class='eco-big'>{shown:.0f}<small>% hazard</small></div>{was}"
        f"<div class='eco-gauge'><i style='width:{clamp(shown / 100) * 100:.0f}%;--bar:{color}'></i></div>"
        f"<div class='eco-chips'>{chips}</div><div class='eco-act'>{act}</div></div>"
    )


def position_on_path(wps: tuple, frac: float) -> tuple:
    frac = clamp(frac)
    seg = frac * (len(wps) - 1)
    i = min(int(seg), len(wps) - 2)
    t = seg - i
    return (wps[i][0] + (wps[i + 1][0] - wps[i][0]) * t, wps[i][1] + (wps[i + 1][1] - wps[i][1]) * t)


def build_map_frame(decisions: list, now_min: int) -> pd.DataFrame:
    rows = []
    for d in decisions:
        color = STATUS_META[d.status][1]
        wps = d.route.waypoints
        alt = _alt_loop(wps, 1.0)
        using_alt = d.final.variant == "ALT" and d.triggered
        for la, lo in wps:
            rows.append({"lat": la, "lon": lo, "color": color + ("40" if using_alt else "99"), "size": 55})
        if using_alt:
            for la, lo in alt:
                rows.append({"lat": la, "lon": lo, "color": "#2bb6c9cc", "size": 55})
        path = alt if using_alt else wps
        frac = (now_min - d.final.depart_min) / max(1.0, d.final.trip_min)
        la, lo = position_on_path(path, frac)
        rows.append({"lat": la, "lon": lo, "color": color, "size": 380})
    return pd.DataFrame(rows)


def kpi_block(decisions: list):
    mitigated = [d for d in decisions if d.triggered and d.action not in ("ESCALATE", "NONE")]
    crit = sum(1 for d in decisions if d.status == "crit")
    kids = sum(d.route.students for d in mitigated)
    avg_red = (sum(d.reduction_pct for d in mitigated) / len(mitigated)) if mitigated else 0.0
    avoided_mg = sum(d.route.students * (d.baseline.dose_ug - d.final.dose_ug) for d in mitigated) / 1000.0
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Routes above alert line", f"{crit} of {len(decisions)}")
    c2.metric("Students re-protected", f"{kids:,}")
    c3.metric("Mean exposure reduction", f"{avg_red:.0f}%")
    c4.metric("PM2.5 inhalation avoided", f"{avoided_mg:.1f} mg", help="Sum over students of (unmitigated dose minus mitigated dose) per trip.")


# ----------------------------------- sidebar ------------------------------------------

def sidebar_controls() -> tuple[Environment, float, int]:
    st.sidebar.markdown("### Scenario")
    name = st.sidebar.selectbox("Air-quality scenario", list(PRESETS), index=0,
                                help="Simulated feeds. Pick Custom to set every value yourself.")
    p = PRESETS[name]
    custom = name == "Custom"
    now = st.sidebar.slider("Engine clock (IST)", min_value=dtime(4, 0), max_value=dtime(21, 0),
                            value=dtime(p["hh"], p["mm"]), step=timedelta(minutes=5), format="HH:mm", disabled=not custom)
    aqi = st.sidebar.slider("Live AQI (CPCB)", 0, 500, p["aqi"], disabled=not custom)
    wind = st.sidebar.slider("Wind speed (km/h)", 1, 40, p["wind"], disabled=not custom)
    wdir = st.sidebar.slider("Wind blowing from (degrees)", 0, 359, p["wdir"], disabled=not custom,
                             help="315 is north-west, where the Punjab/Haryana burn belt lies.")
    burn = st.sidebar.slider("Stubble-burning intensity (%)", 0, 100, p["burn"], disabled=not custom)
    st.sidebar.markdown("### Policy")
    threshold = st.sidebar.slider("Alert threshold (hazard %)", 50, 95, 75)
    max_delay = st.sidebar.select_slider("Longest dispatch hold (min)", options=[0, 15, 30, 45, 60, 90], value=45)
    st.sidebar.caption("Hazard above the threshold triggers the rerouting engine.")
    env = Environment(now.hour * 60 + now.minute, aqi, float(wind), wdir, burn / 100.0)
    return env, float(threshold), int(max_delay)


# ------------------------------------- tabs --------------------------------------------

def tab_fleet(decisions: list, env: Environment, threshold: float):
    kpi_block(decisions)
    st.markdown("<div class='eco-grid'>" + "".join(route_card_html(d, threshold) for d in decisions) + "</div>",
                unsafe_allow_html=True)

    left, right = st.columns([1, 1.35], gap="large")
    with left:
        st.markdown("**Fleet positions**")
        st.caption(f"Engine clock {fmt_hm(env.now_min)} IST. Large markers are buses; teal dots mark Low-Exposure Loops.")
        frame = build_map_frame(decisions, env.now_min)
        try:
            st.map(frame, latitude="lat", longitude="lon", color="color", size="size", zoom=9)
        except Exception:  # noqa: BLE001 - older Streamlit without color/size support
            st.map(frame[["lat", "lon"]])
    with right:
        st.markdown("**Dispatch plan**")
        rows = []
        for d in decisions:
            rows.append({
                "Route": d.route.id, "Area": d.route.area, "Students": d.route.students,
                "Scheduled": fmt_hm(d.route.depart_min), "Dispatch": fmt_hm(d.final.depart_min),
                "Hazard now": round(d.baseline.hazard, 1),
                "Hazard after": round(d.final.hazard, 1),
                "Exposure cut": round(d.reduction_pct, 1),
                "Dose (ug/child)": round(d.final.dose_ug, 1),
                "Plan": plan_text(d),
            })
        df = pd.DataFrame(rows)
        show_df(df, hide_index=True, column_config={
            "Hazard now": st.column_config.ProgressColumn("Hazard now", min_value=0, max_value=100, format="%.0f%%"),
            "Hazard after": st.column_config.ProgressColumn("Hazard after", min_value=0, max_value=100, format="%.0f%%"),
            "Exposure cut": st.column_config.NumberColumn("Exposure cut", format="%.0f%%"),
            "Students": st.column_config.NumberColumn("Students", format="%d"),
        })
        st.caption(f"Dose is PM2.5 inhaled per child per trip at {BREATHING_M3_PER_MIN * 1000:.1f} L/min with "
                   f"{CABIN_INFILTRATION:.0%} cabin infiltration. India's 24-hour PM2.5 limit is {NAAQS_PM25_24H:.0f} ug/m3.")


def tab_ai(decisions: list, env: Environment, threshold: float):
    ids = [d.route.id for d in decisions]
    pick = st.selectbox("Route to inspect", ids, index=max(0, next((i for i, d in enumerate(decisions) if d.triggered), 0)))
    d = next(x for x in decisions if x.route.id == pick)

    c1, c2 = st.columns([1.5, 1], gap="large")
    with c1:
        st.markdown("**Three-hour hazard forecast with uncertainty**")
        band = monte_carlo_band(d.route, env, d.baseline.variant, d.baseline.depart_min)
        band = band.assign(**{"Alert threshold": threshold})
        st.line_chart(band, color=["#6fa8dc", "#2ecc8c", "#ff4d4f", "#9a9a9a"], height=300)
        st.caption("240 Monte Carlo draws perturb AQI (10%), wind speed (25%), wind direction (18 degrees) and burn intensity. "
                   "Unmitigated plan shown.")
    with c2:
        st.markdown("**What drives the score**")
        contrib = lambda e: {k: 100 * w * e.mean_factors[k] for k, w in
                             (("AQI", W_AQI), ("PM2.5", W_PM25), ("Plume", W_PLUME), ("Traffic", W_TRAFFIC))}
        cdf = pd.DataFrame({"Unmitigated": contrib(d.baseline), "Mitigated plan": contrib(d.final)})
        cdf.index.name = "Factor"
        st.bar_chart(cdf, color=["#ff4d4f", "#2ecc8c"], height=300, stack=False)
        st.caption("Weighted contribution to the composite risk, in points.")

    st.markdown("**Candidate mitigation plans**")
    if d.candidates:
        rows = [{"Route variant": "Low-Exposure Loop" if e.variant == "ALT" else "Usual road", "Hold (min)": e.delay_min,
                 "Departs": fmt_hm(e.depart_min), "Trip (min)": round(e.trip_min), "Hazard": round(e.hazard, 1),
                 "Dose (ug)": round(e.dose_ug, 1),
                 "Exposure cut": round(100 * (1 - e.dose_ug / d.baseline.dose_ug), 1),
                 "Clears threshold": "Yes" if e.hazard <= threshold else "No",
                 "Chosen": "Selected" if (e.variant == d.final.variant and e.delay_min == d.final.delay_min) else ""}
                for e in sorted(d.candidates, key=lambda e: e.dose_ug * (1 + 0.35 * e.delay_min / 60))]
        show_df(pd.DataFrame(rows), hide_index=True, column_config={
            "Hazard": st.column_config.ProgressColumn("Hazard", min_value=0, max_value=100, format="%.0f%%"),
            "Exposure cut": st.column_config.NumberColumn("Exposure cut", format="%.0f%%")})
    else:
        st.info(f"{d.route.id} is under the {threshold:.0f}% threshold, so the optimiser stays idle. "
                "Switch to the stubble plume scenario to see it work.")

    st.markdown("**Hazard by dispatch time, whole fleet**")
    cols = {"Base +0": ("BASE", 0), "Base +30": ("BASE", 30), "Base +60": ("BASE", 60),
            "Loop +0": ("ALT", 0), "Loop +30": ("ALT", 30), "Loop +60": ("ALT", 60)}
    grid = pd.DataFrame({x.route.id: {k: evaluate(x.route, env, x.route.depart_min + dl, v, dl).hazard
                                      for k, (v, dl) in cols.items()} for x in decisions}).T
    try:
        show_df(heat_style(grid))
    except Exception:  # noqa: BLE001 - Styler needs jinja2; fall back to plain numbers
        show_df(grid.round(0))

    with st.expander("Model card: equations, weights and assumptions"):
        st.latex(r"C_t = 0.30\,A_t + 0.28\,P_t + 0.24\,W_t + 0.18\,T_t")
        st.latex(r"h_t = 100\,\sigma\!\big(8\,(C_t - 0.52)\big)")
        st.latex(r"H = 0.6\,\frac{\sum_t e^{-t/120}\,h_t}{\sum_t e^{-t/120}} + 0.4\,\max_t h_t \qquad t \in [0,180]\ \text{min}")
        st.latex(r"W_t = 1.2\, b\, o\, \cos^2(\theta_t - 315^\circ)\,\big(1-e^{-v_t/6}\big)\,e^{-(v_t/28)^2}")
        show_df(pd.DataFrame([
            {"Symbol": "A", "Factor": "Live AQI, forecast with a boundary-layer diurnal curve", "Normaliser": "AQI / 400"},
            {"Symbol": "P", "Factor": "PM2.5 from the CPCB breakpoint inverse, plume and roadside uplift", "Normaliser": "ug/m3 / 180"},
            {"Symbol": "W", "Factor": "Smoke plume from wind vector against the north-west burn belt", "Normaliser": "0 to 1"},
            {"Symbol": "T", "Factor": "Time-of-day congestion multiplier (school and office peaks)", "Normaliser": "multiplier minus 1"},
        ]), hide_index=True)
        st.caption("b is burn intensity, o is the route's exposure to the smoke corridor, v is wind speed and theta is the wind "
                   "direction. Weights and the logistic centre are hand-calibrated for this demo, not fitted to measured data.")


def tab_aws(decisions: list, env: Environment, threshold: float, compute_ms: float, sig: str):
    probe = probe_localstack()
    up = probe["up"]

    if up:
        st.success(f"LocalStack is reachable at {LOCALSTACK_ENDPOINT} ({probe['latency_ms']} ms). "
                   "Press the button to deploy and invoke the pipeline for real.")
    else:
        st.info(f"LocalStack not detected at {LOCALSTACK_ENDPOINT} ({probe['detail']}). "
                "Everything below is a deterministic emulation, labelled as such. Start LocalStack to go live.")
        st.code("docker run --rm -p 4566:4566 -e SERVICES=lambda,sns,events localstack/localstack", language="bash")

    st.session_state.setdefault("invocations", 0)
    trace = build_emulated_trace(decisions, env, threshold, compute_ms, st.session_state["invocations"] == 0, sig)

    clicked = st.button("Fire EventBridge trigger against LocalStack", type="primary", disabled=not up)
    if clicked:
        with st.spinner("Deploying handler, creating rules, publishing to SNS..."):
            live = run_live_pipeline(trace, decisions, threshold)
        st.session_state["invocations"] += 1
        st.session_state["live"] = {"sig": sig, "trace": live}
    saved = st.session_state.get("live")
    if saved and saved["sig"] == sig:
        trace = saved["trace"]
    elif saved:
        st.caption("The last live run used a different scenario, so the current records are emulated.")

    for e in trace["errors"]:
        st.warning(e)

    badge = lambda svc: ("<span class='eco-dot' style='background:#2ecc8c'></span>real LocalStack response"
                         if trace["mode"][svc] == "localstack" else "<span class='eco-dot' style='background:#ffb020'></span>emulated record")
    st.markdown(
        "<div class='eco-flow'>"
        f"<div class='eco-node'><h4>Amazon EventBridge</h4><p>{len(trace['eventbridge']['rules'])} cron rules, 45 min before each departure.</p><p>{badge('eventbridge')}</p></div>"
        f"<div class='eco-node'><h4>AWS Lambda</h4><p>{FUNCTION_NAME}, python3.12, 512 MB.</p><p>{badge('lambda')}</p></div>"
        f"<div class='eco-node'><h4>Amazon SNS</h4><p>{len(trace['sns'])} publish call(s) with SMS, email and push bodies.</p><p>{badge('sns')}</p></div>"
        "</div>", unsafe_allow_html=True)

    t1, t2, t3 = st.tabs(["EventBridge", "Lambda", "SNS"])
    with t1:
        st.markdown("**Event delivered to the target**")
        st.json(trace["eventbridge"]["delivered_event"])
        st.markdown("**PutRule calls**")
        for r in trace["eventbridge"]["rules"]:
            with st.expander(f"{r['request']['Name']}   [{r['source']}]"):
                st.json({"PutRule.request": r["request"], "PutRule.response": r["response"]})
    with t2:
        m = trace["lambda"]["metrics"]
        a, b, c, d_ = st.columns(4)
        a.metric("Function version", m["functionVersion"])
        b.metric("Billed duration", f"{m['billedDurationMs']} ms", f"{m['durationMs']:.2f} ms actual", delta_color="off")
        c.metric("Memory used", f"{m['maxMemoryUsedMB']} / {m['memorySizeMB']} MB")
        d_.metric("Est. cost", f"${m['estimatedCostUSD']:.8f}", "cold start" if m.get("coldStart") else "warm", delta_color="off")
        st.markdown(f"**CloudWatch log stream** `[{trace['lambda']['source']}]`")
        st.code("\n".join(trace["lambda"]["log_lines"]), language="text")
        l, r_ = st.columns(2)
        with l:
            st.markdown("**Invocation response**")
            st.json(trace["lambda"]["response"])
        with r_:
            st.markdown("**Execution metrics**")
            st.json(m)
        with st.expander("Handler deployed to LocalStack (live mode)"):
            st.code(LAMBDA_HANDLER_SRC, language="python")
    with t3:
        st.caption(f"Topic {TOPIC_ARN}. Bodies use MessageStructure=json so each protocol receives its own text.")
        for rec in trace["sns"]:
            with st.expander(f"Publish for {rec['route_id']}   [{rec['source']}]", expanded=rec is trace["sns"][0]):
                msg = rec["request"]["Message"]
                s1, s2, s3 = st.columns(3)
                s1.markdown("**SMS**")
                s1.code(msg["sms"], language="text")
                s2.markdown("**Email**")
                s2.code(f"Subject: {rec['request']['Subject']}\n\n{msg['email']}", language="text")
                s3.markdown("**Push (FCM and APNs)**")
                s3.code(json.dumps({"GCM": json.loads(msg["GCM"]), "APNS": json.loads(msg["APNS"])}, indent=2), language="json")
                st.markdown("**Publish request and response**")
                st.json({"Publish.request": rec["request"], "Publish.response": rec["response"]}, expanded=False)
                st.markdown("**Delivery receipts**")
                st.json(rec["delivery_receipts"], expanded=False)


# ------------------------------------- main -------------------------------------------

def main():
    st.markdown(CSS, unsafe_allow_html=True)
    env, threshold, max_delay = sidebar_controls()

    t0 = time.perf_counter()
    decisions = [optimise(r, env, threshold, max_delay) for r in FLEET]
    compute_ms = (time.perf_counter() - t0) * 1000.0
    sig = json.dumps([env.__dict__, threshold, max_delay], sort_keys=True)

    probe = probe_localstack()
    dot = "#2ecc8c" if probe["up"] else "#ffb020"
    status = "LocalStack connected" if probe["up"] else "LocalStack offline, emulation mode"
    st.markdown(
        "<div class='eco-banner'><div><h1>EcoRoute Sentinel</h1>"
        "<p>Scores the next three hours of smoke, wind and traffic for every school bus route, "
        "then reroutes or holds buses before children breathe the worst of it.</p></div>"
        "<div class='eco-pills'>"
        f"<span class='eco-pill'><span class='eco-dot' style='background:{dot}'></span><b>{status}</b></span>"
        f"<span class='eco-pill'>Region <b>{AWS_REGION}</b></span>"
        f"<span class='eco-pill'>Engine clock <b>{fmt_hm(env.now_min)} IST</b></span>"
        "<span class='eco-pill'>Track 01: Air</span></div></div>", unsafe_allow_html=True)

    tab1, tab2, tab3 = st.tabs(["🗺️ Active Fleet Ops", "🔬 Predictive AI Model Insights", "🛡️ AWS Cloud Infrastructure Plumbing"])
    with tab1:
        tab_fleet(decisions, env, threshold)
    with tab2:
        tab_ai(decisions, env, threshold)
    with tab3:
        tab_aws(decisions, env, threshold, compute_ms, sig)


main()
