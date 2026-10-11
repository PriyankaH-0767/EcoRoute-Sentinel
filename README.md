# EcoRoute Sentinel: AI-Predictive School Transport Pollution Mitigation Engine

> **AWS Bharat Builds Tour: Environmental Hacks | Track 01: Air (School Safety / Pollution Exposure)**

EcoRoute Sentinel scores the next three hours of smoke plume dispersion, wind velocity, and traffic congestion for school bus corridors across Delhi NCR, dynamically rerouting or holding buses to protect children from inhaling hazardous PM2.5 concentrations.

---

## 🏗️ Architecture

```
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
```

### Core Components
1. **Domain Model & Physics-Inspired Risk Calculations**
   - Diurnal boundary-layer AQI cycle modeling inversion peaks and afternoon mixing.
   - Wind-vector plume transport model capturing north-west stubble-burning smoke corridors.
   - Roadside street-canyon traffic multipliers.
   - Cumulative PM2.5 inhaled dose estimations per child.
2. **Decision Engine**
   - Dynamic enumeration of route variants ("Low-Exposure Loop" vs base roads) combined with dispatch hold options (15–90 mins).
   - Cost-benefit optimization minimizing delay-penalized student inhalation.
3. **AWS Cloud Infrastructure Plumbing**
   - **Amazon EventBridge**: Pre-dispatch scheduled cron rules (T-45 min).
   - **AWS Lambda**: `ecoroute-sentinel-hazard-inference` handler.
   - **Amazon SNS**: Multi-protocol transactional notifications (SMS, Email, APNs, FCM/GCM).
   - Dual-mode support: High-fidelity deterministic emulation or live LocalStack execution.
4. **Interactive Streamlit Interface**
   - Active Fleet Operations with live map & KPIs.
   - Predictive AI Model Insights with Monte Carlo uncertainty bands & risk decomposition.
   - AWS Cloud Infrastructure Plumbing trace inspector.

---

## 🚀 Quickstart

### 1. Requirements & Setup
Ensure Python 3.11+ is installed.

```bash
# Create virtual environment
python -m venv .venv
# Activate virtual environment:
# Windows (PowerShell):
.venv\Scripts\Activate.ps1
# Linux/macOS:
source .venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

### 2. Run the Streamlit Dashboard
```bash
streamlit run app.py
```

### 3. Optional: Live AWS LocalStack Plumbing Mode
To run against a live LocalStack instance:
```bash
docker run --rm -p 4566:4566 -e SERVICES=lambda,sns,events localstack/localstack
```
EcoRoute Sentinel automatically detects running LocalStack instances and enables direct deployment and execution of EventBridge rules, Lambda functions, and SNS topics.
