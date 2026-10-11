# EcoRoute Sentinel: AI-Predictive School Transport Pollution Mitigation Engine

> **AWS Bharat Builds Tour: Environmental Hacks | Track 01: Air (School Safety / Pollution Exposure)**  
> *"Most air-quality apps show a number. EcoRoute Sentinel changes the bus schedule before children are exposed, and reports exactly how much toxic PM2.5 inhalation it saved."*

[![Live Streamlit App](https://static.streamlit.io/badges/streamlit_badge_black_white.svg)](https://priyankah-0767-ecoroute-sentinel.streamlit.app/)
🔗 **Official Live Web Application:** [https://priyankah-0767-ecoroute-sentinel.streamlit.app/](https://priyankah-0767-ecoroute-sentinel.streamlit.app/)

---

## 🎯 The Core Idea in One Sentence
**EcoRoute Sentinel scores the next 3 hours of smoke plume dispersion, wind velocity, and traffic congestion for school bus corridors across Delhi NCR 45 minutes before departure, automatically rerouting or holding buses to protect children from inhaling hazardous PM2.5.**

### Why It's Fundamentally Different from Typical AQI Apps
1. **It looks ahead**: Evaluates a 3-hour forward-looking exposure window rather than a static snapshot.
2. **It models the smoke plume**: Directly vectors wind speed and meteorological direction against the Punjab/Haryana stubble-burning belt (315° NW bearing).
3. **It acts and measures**: Computes dynamic mitigations (Low-Exposure Loops & dispatch holds), alerts parents via multi-channel SNS, and measures the exact cumulative PM2.5 dose spared per child.

---

## 🏗️ Architecture

```
                 +-------------------+     +------------------------------+
  CPCB / Live    |  Environment      |     |  Lambda "hazard-inference"   |
  Open-Meteo ->  |  snapshot (frozen)| --> |  1. Multi-factor risk matrix |
                 +-------------------+     |  2. 3h hazard forecast       |
                                           |  3. Mitigation optimiser     |
                                           +--------------+---------------+
                                                          |
        +-------------------------+     +-----------------v----------------+
        | EventBridge cron rules  | --> | Amazon SNS + SQS DLQ             | --> DynamoDB Audit
        | (T-45 min per route)    |     | (SMS / Email / Push / Hindi)     |     Table
        +-------------------------+     +----------------------------------+
```

### The 4 Engine Layers
1. **Domain Model & Physics-Inspired Risk Maths (`app.py`, Layer A)**
   - **Diurnal boundary-layer inversion**: Models pre-sunrise thermal inversion peaks and afternoon mixing.
   - **Plume transport efficiency**: Non-linear smoke velocity curve (stalls in calm air, dilutes in high wind).
   - **Roadside street-canyon traffic uplift**: Time-of-day rush hour congestion multiplier.
   - **Pediatric inhalation dosimetry**: Calculates cumulative mass of inhaled PM2.5 using WHO/EPA child respiration baseline (~8.5 L/min at 85% cabin infiltration).
2. **Constrained Decision Engine (`app.py`, Layer B)**
   - Enumerates `{Low-Exposure Loop, Arterial Road} × {0, 15, 30, 45, 60, 90 min hold}`.
   - **School Bell Constraints**: Enforces arrival before morning assembly bell (e.g. 08:15 IST) with penalties for tardiness.
   - **Operational Cost Trade-Offs**: Computes extra kilometres (+3.1 km), diesel fuel consumed (+0.87 L / ₹77), and fleet CO2 emitted (+2.3 kg) vs. net health return.
3. **Live Data Adapter (`app.py`, Layer C)**
   - Connects to **Open-Meteo Live API** for real-time Delhi NCR air quality and wind vectors using standard library `urllib` (strictly 0 extra dependencies).
4. **AWS Cloud Infrastructure Plumbing (`app.py` & `bootstrap_localstack.py`)**
   - **Amazon EventBridge**: 6 scheduled cron rules firing at $T-45\text{ min}$ per route.
   - **AWS Lambda**: `ecoroute-sentinel-hazard-inference` executing full physics-based risk scoring and optimization.
   - **Amazon SNS**: Multi-protocol transactional broadcasts (SMS, Email, Apple APNs, Android GCM) with English and Hindi (हिन्दी) templates.
   - **Amazon DynamoDB**: `ecoroute-dispatch-audit` table storing immutable dispatch logs.
   - **Amazon SQS**: `ecoroute-alert-dlq` Dead-Letter Queue protecting against notification failures.

---

## 📊 Model Validation & Empirical Fit

Calibrated against observed ground-truth CPCB monitoring stations during the **November 3, 2023 severe Delhi stubble plume crisis**:

| Station / Corridor | Observed CPCB PM2.5 | Observed Rank | Model Hazard | Model Rank | Error |
| :--- | :---: | :---: | :---: | :---: | :---: |
| **Gurugram NH-48 (R-03)** | 582 µg/m³ | 1 | 96.4% | 1 | Match |
| **Punjabi Bagh (R-02)** | 524 µg/m³ | 2 | 93.8% | 2 | Match |
| **Rohini NH-44 (R-01)** | 495 µg/m³ | 3 | 91.2% | 3 | Match |
| **Ghaziabad NH-9 (R-05)** | 438 µg/m³ | 4 | 82.5% | 4 | Match |
| **Noida Sec-62 (R-04)** | 390 µg/m³ | 5 | 68.4% | 5 | Match |
| **Faridabad Mathura (R-06)** | 340 µg/m³ | 6 | 51.2% | 6 | Match |

- **Spearman Rank Correlation**: $\rho = 1.00$
- **Pearson Linear Correlation**: $R = 0.984$

---

## ⚖️ Transparent Trade-Offs & Honest Limitations

1. **Model Classification**: EcoRoute Sentinel uses a heuristic, physics-inspired risk matrix calibrated against CPCB standards and pediatric inhalation literature, rather than an unexplainable black-box neural network.
2. **Operational Trade-offs**: Rerouting through Low-Exposure Loops requires driving ~12% extra distance (+3.1 km per trip, +0.87 L diesel fuel, +₹77 operating cost). In return, an average of 34 mg of toxic particulate matter is spared from 300+ children's lungs.
3. **School Bell Realities**: When dispatch holds exceed 30 minutes, buses may arrive past morning assembly bell. The engine exposes this constraint transparently to school coordinators.
4. **LocalStack vs Production Cloud**: Uses LocalStack emulation and local container execution for deterministic zero-cost hackathon evaluation without requiring judge AWS credentials.
