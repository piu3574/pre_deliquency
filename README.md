# ANVAYA: Pre-Delinquency Early Warning System

[![Python 3.10](https://img.shields.io/badge/Python-3.10-blue.svg)](https://www.python.org/)
[![XGBoost](https://img.shields.io/badge/Model-XGBoost-orange.svg)](https://xgboost.readthedocs.io/)
[![LightGBM](https://img.shields.io/badge/Model-LightGBM-green.svg)](https://lightgbm.readthedocs.io/)
[![Apache Kafka](https://img.shields.io/badge/Streaming-Apache%20Kafka-black.svg)](https://kafka.apache.org/)
[![LangGraph](https://img.shields.io/badge/Agents-LangGraph-purple.svg)](https://www.langchain.com/langgraph)
[![React 19](https://img.shields.io/badge/Frontend-React%2019-cyan.svg)](https://react.dev/)
[![FastAPI](https://img.shields.io/badge/Backend-FastAPI-teal.svg)](https://fastapi.tiangolo.com/)

ANVAYA is an enterprise-grade pre-delinquency early warning system designed for retail banking. By analyzing high-frequency cash flow and behavioral data, the system detects financial stress in loan customers **2 to 4 weeks before they miss their first Equated Monthly Installment (EMI) payment**. Rather than waiting for a default to trigger expensive and adversarial recovery processes, ANVAYA scores customer risk, leverages SHAP explainability to explain the underlying stress drivers, automatically maps them to tailored interventions (such as EMI date shifts or temporary payment holidays), and drafts empathetic, agent-personalized messages. The system balances predictive accuracy with regulatory transparency, making it fully auditable for compliance.

---

## The Problem
Traditional retail credit collection is inherently reactive. Banks typically initiate outreach only *after* an EMI is missed, incurring significant recovery costs, damaging customer relationships, and suffering high attrition. Moreover, conventional credit scoring systems (such as FICO or CIBIL) are static, updated only monthly or quarterly. This lag makes them blind to rapid cash-flow deterioration or sudden behavioral changes, particularly for gig-workers, freelancers, farmers, and contractors who do not fit traditional, salary-based credit assessment templates.

---

## The Solution
ANVAYA changes the paradigm from collections to proactive assistance. It operates differently by:
*   **Catching Early Stress Signals:** Analyzing transaction-level and repayment-timing patterns to flag financial distress 14–30 days prior to payment due dates.
*   **Individualized Baselines:** Employing a Personal Rhythm Engine that measures deviations from an individual customer's historical baseline, bypassing population-level bias.
*   **Regulated Explainability:** Utilizing Weight of Evidence (WoE) transformation with capped Information Value (IV) to prevent data leakage and maintain model transparency.
*   **Dual-Track Gating:** Running a high-speed XGBoost screening layer and routing borderline cases dynamically to a deep-analysis LightGBM layer.
*   **Empathetic GenAI Outreach:** Automating case summaries and drafting personalized, compliance-aligned customer communications in under 2 seconds via a LangGraph 2-agent workflow.

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
                    │ (Trigger Band Elevation)              ▼
                    ▼                            ┌──────────────────┐
     ┌─────────────────────────────┐             │ Agent 2: Action  │
     │      Alert Generation       │             │ (Draft Message)  │
     │  (Kafka anvaya-alerts topic)│             └──────────┬───────┘
     └──────────────┬──────────────┘                        │
                    │                                       │
                    └───────────────────┬───────────────────┘
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

1.  **Robust Data Pipeline:** Ingests and processes 4 core files (`application_train.csv`, `installments_payments.csv`, `bureau.csv`, `credit_card_balance.csv`) from the Home Credit Default Risk Kaggle dataset. Handles missing values, clips outliers, encodes labels, and aggregates satellite tables to generate a 50,000-row × 88-column master dataframe.
2.  **13 Custom Risk Features (F1-F13):** Designed domain-specific features capturing high-frequency behavioral shifts like savings drawdown rates, income arrival delays, transaction frequencies with lending apps, and payment timing entropy.
3.  **Personal Rhythm Engine:** Normalizes feature distributions per customer using rolling Z-scores. The engine measures a customer's deviation against their own historical baseline rather than comparing them to population averages, making it robust to gig workers, freelancers, and traditional salaried workers.
4.  **WOE Transformation & IV Capping:** Performs Weight of Evidence (WoE) conversion to generate model-ready log-odds values. Caps Information Value (IV) at 0.5 to prevent data leakage and guarantee full regulatory compliance and model interpretability.
5.  **Dual-Track Gating Model:** Blends a fast, 7-feature XGBoost screener with a comprehensive LightGBM analyzer. Borderline probability zones (15% to 25% PD) are dynamically routed to the LightGBM analyzer. Outputs are blended via a Meta-Logistic Regression model and calibrated using Isotonic Regression into 4 risk bands: **GREEN** (<5%), **YELLOW** (5-15%), **HIGH** (15-25%), and **RED** (>25%).
6.  **SHAP Explainability:** Runs a SHAP `TreeExplainer` on the LightGBM track to extract the top 3 drivers of risk per customer. Generates plain-English feature-level explanations and relative contribution percentages to create audit trails compliant with regulatory frameworks.
7.  **Rule-Based Intervention Engine:** Integrates a rule mapping system that translates top SHAP risk drivers into viable restructuring operations (e.g., mapping High EMI Burden to a tenure extension, or Late Income Arrival to an EMI date shift).
8.  **Kafka Streaming & Contagion Detector:** Employs Apache Kafka to process simulated real-time transactions. Includes a background **Stress Contagion Detector** that triggers when 5 or more customers working for the same employer enter the Yellow risk band within 7 days, auto-escalating all connected customer profiles by one risk band to hedge against corporate layoffs or delays.
9.  **LangGraph 2-Agent Workflow:** Executes sequential LLM agents in under 2 seconds. *Agent 1 (Analyzer)* consumes SHAP drivers and writes a concise case summary for Relationship Managers (RMs). *Agent 2 (Action)* builds on this summary to draft personalized customer communications containing exact restructuring numbers (e.g. precise rupee values).
10. **Interactive React Dashboard:** Built with React 19 and Vite, featuring 5 functional views: Portfolio Overview, Customer Profile, Alerts Feed, Interventions Tracker, and Live Test Scoring.

---

## The 13 Risk Features

| Code | Feature Name | Description |
| :--- | :--- | :--- |
| **F1** | EMI to Income Ratio | Ratio of monthly debt obligations (EMI) to total monthly income. |
| **F2** | Savings Drawdown Percentage | Percentage change and depletion rate of savings reserves over a rolling 60-day window. |
| **F3** | Income Arrival Irregularity | Standard deviation of income arrival dates compared to typical scheduled arrival. |
| **F4** | Spending Pattern Shift | Cosine similarity between current-month category spending vector and historical baseline. |
| **F5** | Auto Debit Failure Count | Count of failed automated clearing/NACH transactions due to insufficient funds. |
| **F6** | Lending App Transaction Count | Number of transfers or repayments to short-term, high-interest digital lending applications. |
| **F7** | Cash Hoarding Ratio | Proportion of total monthly debits executed as cash withdrawals versus digital transactions. |
| **F8** | Stress Velocity | Rate of change in predicted risk and stress indicators over a rolling 90-day window. |
| **F9** | Payment Timing Entropy | Shannon entropy of installment repayment timestamps (higher means more erratic repayment dates). |
| **F10** | Peer Cohort Stress Index | Aggregated stress and delinquency flags calculated within the customer's employer or peer group. |
| **F11** | Overdraft Frequency | Count of occurrences where account balance fell below zero or breached credit overdraft limits. |
| **F12** | Cross Loan Payment Consistency| Variance in repayment timeliness and behavior across multiple active credit facilities. |
| **F13** | Secondary Income Index | Stability, consistency, and volume of auxiliary cash inflows outside of primary salary. |

---

## Personal Rhythm Engine
Traditional retail credit assessment models classify customers into static demographic cohorts or occupational classes. This approach creates high false-positive rates for gig workers, freelancers, farmers, and pensioners, whose cash flows are naturally volatile.

ANVAYA overcomes this using the **Personal Rhythm Engine**. It computes rolling Z-score normalizations at the *individual customer level*:

$$\text{Normalized Feature} = \frac{\text{Current Value} - \mu_{\text{Customer Historical}}}{\sigma_{\text{Customer Historical}}}$$

By comparing customers solely to their own historical behaviors, the system establishes a personal baseline for cash flow velocity, payment timings, and ATM withdrawals. If a freelancer regularly receives income on irregular dates, the model learns this pattern as normal. However, if a traditionally stable salaried worker suddenly exhibits a minor delay in income, the system flags it immediately as a critical deviation.

---

## Tech Stack

*   **Core Language:** Python 3.10
*   **Machine Learning:** XGBoost, LightGBM, Scikit-learn, SHAP
*   **Data Science:** Pandas, NumPy
*   **Event Streaming:** Apache Kafka, `kafka-python`, Docker Compose
*   **AI Agent Workflows:** LangGraph, LangChain, Groq/OpenAI (LLM Interface)
*   **Dashboard Frontend:** React 19, Vite, Recharts, Tailwind CSS, Lucide React
*   **Dashboard Backend:** FastAPI, Uvicorn

---

## Project Structure

```
ANVAYA/
├── data/
│   ├── raw/
│   ├── synthetic/
│   └── processed/
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
│   ├── stress_contagion.py
│   ├── intervention_engine.py
│   ├── langgraph_agents.py
│   └── main.py
├── dashboard/
│   ├── frontend/
│   │   ├── src/
│   │   │   ├── App.jsx
│   │   │   ├── screens/
│   │   │   │   ├── Portfolio.jsx
│   │   │   │   ├── CustomerProfile.jsx
│   │   │   │   ├── Alerts.jsx
│   │   │   │   ├── Interventions.jsx
│   │   │   │   └── LiveScoring.jsx
│   │   │   └── index.css
│   │   └── package.json
│   └── backend/
│       └── main.py
├── models/
│   ├── xgb_model.pkl
│   ├── lgb_model.pkl
│   ├── meta_model.pkl
│   └── calibrated_model.pkl
└── notebooks/
```

---

## How to Run

### Prerequisites
*   Python 3.10+
*   Node.js (v18+)
*   Docker and Docker Compose (for running Kafka)

### 1. Setup Virtual Environment
Clone the repository, initialize a Python virtual environment, and install dependencies:
```bash
git clone https://github.com/yourusername/ANVAYA.git
cd ANVAYA
python -m venv .venv
source .venv/bin/activate  # On Windows use: .venv\Scripts\activate
pip install -r requirements.txt
```
*(Ensure `pandas`, `numpy`, `xgboost`, `lightgbm`, `scikit-learn`, `shap`, `fastapi`, `uvicorn`, `kafka-python`, `langgraph`, and `langchain` are installed).*

### 2. Start Apache Kafka
Run the ZooKeeper and Kafka brokers in the background using Docker Compose:
```bash
docker-compose up -d
```

### 3. Run the Data & Model Pipeline
Execute the full automated processing, training, and explainability pipeline:
```bash
python src/main.py
```
This runs the data loader, preprocessing, feature extraction, personal rhythm engine, WoE transformation, model training, and SHAP calculation in sequence, saving the calibrated models to `/models` and outputs to `/data/processed`.

### 4. Run the Real-Time Streaming and Contagion Detectors
In separate terminals, run the consumer and transaction producers to simulate real-time event ingestion:
```bash
# Start the Streaming Risk Engine and Contagion Detector (Consumer)
python src/kafka_consumer.py

# Start the Transaction Event Simulator (Producer)
python src/kafka_producer.py
```

### 5. Launch the Dashboard
**Start the FastAPI Backend Router:**
```bash
cd dashboard/backend
uvicorn main:app --reload --port 8000
```

**Start the React Frontend Server:**
```bash
cd ../frontend
npm install
npm run dev
```
Open [http://localhost:5173](http://localhost:5173) in your browser to view the application.

---

## Model Performance
The dual-track model was trained and evaluated on **50,000 customers** using a stratified **70/15/15 train/validation/holdout** split. The final results achieved on the unseen holdout dataset are:

*   **Holdout AUC-ROC:** `0.9391`
*   **Kolmogorov-Smirnov (KS) Statistic:** `74.18`
*   **Gini Coefficient:** `0.8782`
*   **Precision (threshold @0.50):** `0.7299`
*   **Recall (threshold @0.50):** `0.4739`
*   **F1 Score (threshold @0.50):** `0.5746`

### Risk Band Thresholds
Customers are classified into four risk categories based on their calibrated Probability of Default (PD) scores:
*   **GREEN (Low Risk):** $\text{PD} < 5\%$
*   **YELLOW (Medium Risk):** $5\% \le \text{PD} < 15\%$
*   **HIGH (High Risk):** $15\% \le \text{PD} < 25\%$
*   **RED (Critical Risk):** $\text{PD} \ge 25\%$

---

## Business Impact
For a mid-sized retail bank managing a representative portfolio:

*   **Active Portfolio Size:** 1,00,000 customers
*   **Baseline Default Rate:** 3% annually (3,000 defaulting customers)
*   **Delinquency Mitigation Rate:** 20% defaults prevented via proactive early interventions
*   **Avoided Delinquencies:** 600 saved accounts annually
*   **Net Loan Value Protected:** ~₹30 Crores (₹300,000,000) per year
*   **Annual System Infrastructure & Support Cost:** ~₹70 Lakhs (₹7,000,000)
*   **ROI:** Highly positive and profitable within the first 12 months of deployment

---

## Dashboard Screenshots
*(Screenshots of the 5 interactive screens are coming soon)*

*   **Portfolio Overview:** Global health metrics, cohort distribution, default probability curves, and search/filter interface.
*   **Customer Profile:** Full individual breakdown showing the customer gauge, Personal Rhythm deviations, stress velocity direction, 6-month payment delays, and SHAP cards.
*   **Alerts Feed:** Live-updating transaction alerts and active Stress Contagion warnings.
*   **Interventions Tracker:** Overview of accepted restructuring proposals and RM decision logs.
*   **Live Scoring:** Interactive form to run manual inputs through the real-time scoring and SHAP explanation pipeline.

---

## Future Work
*   **Core Banking System (CBS) Webhooks:** Implement native connectors to stream transaction data directly from legacy systems (e.g. Temenos T24, Finacle).
*   **Continuous Active Learning:** Set up automated retraining pipelines (CT) triggered when actual defaults deviate from predictions.
*   **Multi-Tenant Cloud Deployments:** Architect containerized deployments for AWS and Azure using Kubernetes.
*   **Interactive Agent Workspace:** Expand the LangGraph workflow to enable conversational query-and-response interfaces for RMs auditing customer cards.

---

## License
This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

## Author
*   **Your Name** - Lead ML Engineer & Architect
*   [GitHub Profile](https://github.com/yourusername)
*   [LinkedIn Profile](https://linkedin.com/in/yourusername)
