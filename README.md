# ANVAYA
### AI-Powered Pre-Delinquency Early Warning System for Retail Banking

[![Python 3.10](https://img.shields.io/badge/Python-3.10-blue.svg)](https://www.python.org/)
[![XGBoost](https://img.shields.io/badge/Model-XGBoost-orange.svg)](https://xgboost.readthedocs.io/)
[![LightGBM](https://img.shields.io/badge/Model-LightGBM-green.svg)](https://lightgbm.readthedocs.io/)
[![Apache Kafka](https://img.shields.io/badge/Streaming-Apache%20Kafka-black.svg)](https://kafka.apache.org/)
[![LangGraph](https://img.shields.io/badge/Agents-LangGraph-purple.svg)](https://www.langchain.com/langgraph)
[![React 19](https://img.shields.io/badge/Frontend-React%2019-cyan.svg)](https://react.dev/)
[![FastAPI](https://img.shields.io/badge/Backend-FastAPI-teal.svg)](https://fastapi.tiangolo.com/)

<p align="center">
  Predict financial stress 14–30 days before EMI default using Explainable AI, Behavioral Analytics, Real-Time Streaming, and Agentic Interventions.
</p>

---
## 🚀 Live Demo

### 🌐 Frontend (Vercel)
**https://predeliquencysystem.vercel.app/**

### ⚡ Backend API (Render)
**https://pre-deliquency.onrender.com/**

### 📖 API Documentation (Swagger UI)
**https://pre-deliquency.onrender.com/docs**

## Overview

ANVAYA is an enterprise-grade pre-delinquency early warning system for retail banking. By analyzing high-frequency cash flow and behavioral data, the system detects financial stress in loan customers **2 to 4 weeks before they miss their first EMI payment**. Rather than waiting for a default to trigger expensive recovery processes, ANVAYA scores customer risk, leverages SHAP explainability to explain the stress drivers, automatically maps them to tailored interventions, and drafts empathetic, agent-personalized messages — all in a fully auditable, compliance-aligned workflow.

---

## The Problem

Traditional retail credit collection is inherently reactive. Banks typically initiate outreach only *after* an EMI is missed, incurring significant recovery costs, damaging customer relationships, and suffering high attrition. Conventional credit scoring systems (FICO, CIBIL) are static, updated only monthly or quarterly — this lag makes them blind to rapid cash-flow deterioration, particularly for gig workers, freelancers, farmers, and contractors who do not fit traditional salary-based credit templates.

---

## The Solution

ANVAYA changes the paradigm from collections to proactive assistance:

- **Catching Early Stress Signals** — Analyzes transaction-level and repayment-timing patterns to flag financial distress 14–30 days prior to payment due dates
- **Individualized Baselines** — Employs a Personal Rhythm Engine that measures deviations from each customer's own historical baseline, bypassing population-level bias
- **Regulated Explainability** — Utilizes Weight of Evidence (WoE) transformation with capped Information Value (IV) to prevent data leakage and maintain model transparency
- **Dual-Track Gating** — Runs a high-speed XGBoost screening layer and routes borderline cases dynamically to a deep-analysis LightGBM layer
- **Empathetic GenAI Outreach** — Automates case summaries and drafts personalized, compliance-aligned customer communications in under 2 seconds via a LangGraph 2-agent workflow

---

## System Architecture

```
                                  [ REAL-TIME EVENT STREAMING (KAFKA) ]
                                 ┌─────────────────────────────────────┐
                                 │       anvaya-transactions           │
                                 └──────────────────┬──────────────────┘
                                                    │
                                                    ▼
                                 ┌─────────────────────────────────────┐
                                 │          Kafka Consumer             │
                                 └──────────────────┬──────────────────┘
                                                    │ (Real-time scoring)
                                                    ▼
 [ BATCH TRAINING PIPELINE ]     ┌─────────────────────────────────────┐
┌───────────────────────────┐    │      Personal Rhythm Engine         │
│  Home Credit Kaggle Data  │    │  (Z-Score Normalization vs. Self)   │
│  (50k Customers, 4 CSVs)  │    └──────────────────┬──────────────────┘
└─────────────┬─────────────┘                       │
              │                                     ▼
              ▼                  ┌─────────────────────────────────────┐
┌───────────────────────────┐    │         WoE Transformer             │
│  Preprocessing & Merge    │    │      (Log-odds calculation)         │
└─────────────┬─────────────┘    └──────────────────┬──────────────────┘
              │                                     │
              ▼                                     ▼
┌───────────────────────────┐    ┌─────────────────────────────────────┐
│    Feature Engineering    ├───>│         Dual-Track ML Model         │
│  (Custom Features F1-F13) │    │  XGBoost (Fast) / LightGBM (Deep)   │
└───────────────────────────┘    └──────────────────┬──────────────────┘
                                                    │
                                                    ▼
                                 ┌─────────────────────────────────────┐
                                 │        Isotonic Calibration         │
                                 │   (Calculates probability of def)   │
                                 └──────────────────┬──────────────────┘
                                                    │
                                                    ▼
                                 ┌─────────────────────────────────────┐
                                 │        SHAP TreeExplainer           │
                                 │     (Extracts Top 3 Drivers)        │
                                 └──────────┬───────────────┬──────────┘
                                            │               │
                                            ▼               ▼
      [ STRESS CONTAGION DETECTOR ]         │     [ LANGGRAPH AGENTS ]
     ┌─────────────────────────────┐        │    ┌──────────────────┐
     │ Detects 5+ employer-linked  │<───────┘    │ Agent 1: Analyzer│
     │  cases in Yellow/Red band   │             │ (RM Summary)     │
     │        within 7 days        │             └──────────┬───────┘
     └──────────────┬──────────────┘                        │
                    │ (Trigger Band Elevation)               ▼
                    ▼                            ┌──────────────────┐
     ┌─────────────────────────────┐             │ Agent 2: Action  │
     │      Alert Generation       │             │ (Draft Message)  │
     │  (Kafka anvaya-alerts topic)│             └──────────┬───────┘
     └──────────────┬──────────────┘                        │
                    └───────────────────┬────────────────────┘
                                        │
                                        ▼
                       ┌─────────────────────────────────┐
                       │     FASTAPI BACKEND ROUTER      │
                       └────────────────┬────────────────┘
                                        │ (JSON REST API)
                                        ▼
                       ┌─────────────────────────────────┐
                       │   REACT 19 / VITE DASHBOARD     │
                       └─────────────────────────────────┘
```

---

## Key Features

1. **Robust Data Pipeline** — Ingests and processes 4 core files (`application_train.csv`, `installments_payments.csv`, `bureau.csv`, `credit_card_balance.csv`) from the Home Credit Default Risk Kaggle dataset. Handles missing values, clips outliers, encodes labels, and aggregates satellite tables to generate a 50,000-row × 88-column master dataframe.
2. **13 Custom Risk Features (F1–F13)** — Domain-specific features capturing high-frequency behavioral shifts like savings drawdown rates, income arrival delays, transaction frequencies with lending apps, and payment timing entropy.
3. **Personal Rhythm Engine** — Normalizes feature distributions per customer using rolling Z-scores. Measures deviation against each customer's own historical baseline rather than population averages — robust to gig workers, freelancers, and salaried employees alike.
4. **WoE Transformation & IV Capping** — Performs Weight of Evidence (WoE) conversion to generate model-ready log-odds values. Caps Information Value (IV) at 0.5 to prevent data leakage and guarantee regulatory compliance.
5. **Dual-Track Gating Model** — Blends a fast 7-feature XGBoost screener with a comprehensive LightGBM analyzer. Borderline probability zones (15%–25% PD) are dynamically routed to LightGBM. Outputs blended via Meta-Logistic Regression and calibrated using Isotonic Regression into 4 risk bands.
6. **SHAP Explainability** — Runs a SHAP `TreeExplainer` on the LightGBM track to extract the top 3 drivers of risk per customer with plain-English explanations and contribution percentages, creating full regulatory audit trails.
7. **Rule-Based Intervention Engine** — Translates top SHAP risk drivers into viable restructuring actions (e.g., High EMI Burden → tenure extension, Late Income Arrival → EMI date shift).
8. **Kafka Streaming & Contagion Detector** — Processes real-time transactions via Apache Kafka. The Stress Contagion Detector triggers when 5+ customers from the same employer enter Yellow/Red band within 7 days, auto-escalating all connected profiles by one risk band.
9. **LangGraph 2-Agent Workflow** — Sequential LLM agents in under 2 seconds. *Agent 1 (Analyzer)* writes a concise RM case summary from SHAP drivers. *Agent 2 (Action)* drafts personalized customer communications with exact restructuring figures.
10. **Interactive React Dashboard** — Built with React 19 and Vite, featuring 5 functional views: Portfolio Overview, Customer Profile, Alerts Feed, Interventions Tracker, and Live Scoring.

---

## The 13 Risk Features

| Code | Feature Name | Description |
| :--- | :--- | :--- |
| **F1** | EMI to Income Ratio | Ratio of monthly debt obligations (EMI) to total monthly income |
| **F2** | Savings Drawdown Percentage | Percentage change and depletion rate of savings reserves over a rolling 60-day window |
| **F3** | Income Arrival Irregularity | Standard deviation of income arrival dates compared to typical scheduled arrival |
| **F4** | Spending Pattern Shift | Cosine similarity between current-month category spending vector and historical baseline |
| **F5** | Auto Debit Failure Count | Count of failed automated clearing/NACH transactions due to insufficient funds |
| **F6** | Lending App Transaction Count | Number of transfers or repayments to short-term, high-interest digital lending applications |
| **F7** | Cash Hoarding Ratio | Proportion of total monthly debits executed as cash withdrawals versus digital transactions |
| **F8** | Stress Velocity | Rate of change in predicted risk and stress indicators over a rolling 90-day window |
| **F9** | Payment Timing Entropy | Shannon entropy of installment repayment timestamps (higher = more erratic repayment dates) |
| **F10** | Peer Cohort Stress Index | Aggregated stress and delinquency flags calculated within the customer's employer or peer group |
| **F11** | Overdraft Frequency | Count of occurrences where account balance fell below zero or breached overdraft limits |
| **F12** | Cross Loan Payment Consistency | Variance in repayment timeliness and behavior across multiple active credit facilities |
| **F13** | Secondary Income Index | Stability, consistency, and volume of auxiliary cash inflows outside of primary salary |

---

## Personal Rhythm Engine

Traditional models classify customers into static demographic cohorts, creating high false-positive rates for gig workers, freelancers, farmers, and pensioners whose cash flows are naturally volatile.

ANVAYA's Personal Rhythm Engine computes rolling Z-score normalizations at the individual customer level:

$$\text{Normalized Feature} = \frac{\text{Current Value} - \mu_{\text{Customer Historical}}}{\sigma_{\text{Customer Historical}}}$$

If a freelancer regularly receives income on irregular dates, the model learns this as normal. If a traditionally stable salaried worker suddenly exhibits a minor income delay, the system flags it immediately as a critical deviation.

---

## Risk Bands

| Band | Probability of Default |
| :--- | :--- |
| **GREEN** | < 5% |
| **YELLOW** | 5% – 15% |
| **HIGH** | 15% – 25% |
| **RED** | > 25% |

---

## Model Performance

Trained and evaluated on **50,000 customers** using a stratified **70/15/15 train/validation/holdout** split:

| Metric | Score |
| :--- | :--- |
| AUC-ROC | 0.7005 |
| KS Statistic | 29.95 |
| Gini Coefficient | 0.4009 |
| Precision | 0.2007 |
| Recall | 0.2901 |
| F1 Score | 0.2372 |

---

## Tech Stack

| Category | Technologies |
| :--- | :--- |
| Core Language | Python 3.10 |
| Machine Learning | XGBoost, LightGBM, Scikit-learn, SHAP |
| Data Science | Pandas, NumPy |
| Event Streaming | Apache Kafka, kafka-python, Docker Compose |
| AI Agent Workflows | LangGraph, LangChain, Groq (LLM Interface) |
| Backend | FastAPI, Uvicorn |
| Frontend | React 19, Vite, Recharts, Tailwind CSS, Lucide React |
| Containerization | Docker |

---

## Project Structure

```
ANVAYA/
├── src/
│   ├── data_loader.py
│   ├── preprocessor.py
│   ├── synthetic_generator.py
│   ├── feature_engineering.py
│   ├── personal_rhythm_engine.py
│   ├── woe_transformer.py
│   ├── feature_selector.py
│   ├── model_trainer.py
│   ├── shap_explainer.py
│   ├── kafka_producer.py
│   ├── kafka_consumer.py
│   ├── contagion_detector.py
│   ├── intervention_engine.py
│   ├── Langgraph_agents.py
│   └── main.py
├── backend/
│   ├── api.py
│   ├── models/
│   │   ├── xgb_model.pkl
│   │   ├── lgb_model.pkl
│   │   ├── meta_model.pkl
│   │   ├── calibrated_model.pkl
│   │   └── woe_mappings.json
│   ├── data/
│   │   ├── raw/
│   │   ├── synthetic/
│   │   └── processed/
│   └── requirements.txt
├── frontend/
│   ├── src/
│   │   ├── App.jsx
│   │   ├── main.jsx
│   │   ├── index.css
│   │   └── pages/
│   │       ├── Portfolio.jsx
│   │       ├── CustomerProfile.jsx
│   │       ├── Alerts.jsx
│   │       ├── Interventions.jsx
│   │       └── LiveScoring.jsx
│   └── package.json
├── notebooks/
├── docker-compose.yml
└── README.md
```

---

## How to Run

### Prerequisites

- Python 3.10+
- Node.js v18+
- Docker and Docker Compose

### 1. Setup Virtual Environment

```bash
git clone https://github.com/yourusername/ANVAYA.git
cd ANVAYA

python -m venv .venv
# Windows
.venv\Scripts\activate
# Linux/Mac
source .venv/bin/activate

pip install -r backend/requirements.txt
```

### 2. Start Apache Kafka

```bash
docker-compose up -d
```

### 3. Run the Data & Model Pipeline

```bash
python src/main.py
```

This runs the full sequence: data loading → preprocessing → feature engineering → personal rhythm normalization → WoE transformation → model training → SHAP explanation. Calibrated models are saved to `backend/models/` and outputs to `backend/data/processed/`.

### 4. Run the Real-Time Streaming Engine

Open two separate terminals:

```bash
# Terminal 1 — Kafka consumer (streaming risk scoring + contagion detector)
python src/kafka_consumer.py

# Terminal 2 — Transaction event simulator (producer)
python src/kafka_producer.py
```

### 5. Launch the Dashboard

```bash
# Start the FastAPI backend
cd backend
uvicorn api:app --reload --port 8000
```

```bash
# Start the React frontend
cd frontend
npm install
npm run dev
```

Open [http://localhost:5173](http://localhost:5173) in your browser.

---

## Dashboard Views

- **Portfolio Overview** — Global health metrics, cohort distribution, risk band breakdown, and customer search/filter interface
- **Customer Profile** — Individual gauge, Personal Rhythm deviations, stress velocity direction, 6-month payment delays, and SHAP driver cards
- **Alerts Feed** — Live-updating transaction alerts and active Stress Contagion warnings
- **Interventions Tracker** — Accepted restructuring proposals and RM decision logs
- **Live Scoring** — Interactive form to run manual inputs through the real-time scoring and SHAP explanation pipeline

---

## Business Impact

For a mid-sized retail bank managing a representative portfolio:

| Metric | Value |
| :--- | :--- |
| Active Portfolio Size | 1,00,000 customers |
| Baseline Default Rate | 3% annually (3,000 defaulting customers) |
| Delinquency Mitigation Rate | 20% defaults prevented via early interventions |
| Avoided Delinquencies | ~600 saved accounts annually |
| Net Loan Value Protected | ~₹30 Crores (₹300,000,000) per year |
| Annual Infrastructure Cost | ~₹70 Lakhs (₹7,000,000) |
| ROI | Highly positive within the first 12 months |

---

## Resume Highlights

✔ Built a real-time banking risk prediction platform.

✔ Processed 50,000+ customer records.

✔ Developed 13 domain-specific risk features.

✔ Implemented explainable AI using SHAP.

✔ Integrated Kafka event streaming.

✔ Developed LangGraph multi-agent workflows.

✔ Built React + FastAPI production dashboard.

---

## Future Work

- **Core Banking System (CBS) Webhooks** — Native connectors to stream transaction data from legacy systems (Temenos T24, Finacle)
- **Continuous Active Learning** — Automated retraining pipelines triggered when actual defaults deviate from predictions
- **Multi-Tenant Cloud Deployments** — Containerized deployments on AWS/Azure using Kubernetes
- **Interactive Agent Workspace** — Conversational query-and-response interfaces for RMs via expanded LangGraph workflow

---

## License

This project is licensed under the MIT License.

---

*ANVAYA — Predicting financial stress before delinquency occurs.*
