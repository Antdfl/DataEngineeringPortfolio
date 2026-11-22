# 👋 Hi, I'm Antonio Di Felice

**Senior Database Architect → Data Engineer | 25+ Years Oracle Expertise | Databricks Certified (Associate and Professional)**

📍 Based in Italy | 🌐 Remote-first | 💬 English (C1), Spanish (C1)

---

## 🎯 About This Repository

This repository collects my **Data Engineering projects**: data pipelines, workflows and architectures built on the modern data stack, designed and documented the way I would do it in a team.

My Python projects are in **[MyProfessionalPortfolio](https://github.com/Antdfl/MyProfessionalPortfolio)** and my learning log in **[LearningPython3](https://github.com/Antdfl/LearningPython3)**.

**Why public?** Because building in the open creates accountability and invites feedback.

---

## 📂 Projects

| # | Project | In one line | Status |
|---|---|---|---|
| 01 | **[UK Carbon Intensity Lakehouse](uk-carbon-intensity-lakehouse/)** | A medallion lakehouse on Databricks that answers a simple question: in which hour is the electricity of a region in Great Britain cleanest? | 🚧 In progress, target December 2026 |

### 01 · UK Carbon Intensity Lakehouse

- **Data:** carbon intensity and generation mix for the 14 regions of Great Britain, every half hour, from November 2024 onwards. Over 500,000 rows of carbon intensity (one per region and half hour) and over 4.5 million rows of generation mix, refreshed every day.
- **Architecture:** a Python notebook calls the API and lands the raw JSON files in a volume; one Lakeflow Spark Declarative Pipeline builds bronze (Auto Loader), silver (expectations and AUTO CDC) and gold (materialized views); Unity Catalog metric views hold every formula once, so the dashboard and any other tool read the same numbers.
- **Engineering choices:** idempotent ingestion with a completeness flag for every call and a guard against oversized backfills; timestamps stored in UTC and shown in London time; 26 automated checks after every daily refresh, tested against deliberately broken data.
- **Stack:** Databricks Free Edition, Python, SQL, Delta Lake, Auto Loader, Lakeflow Spark Declarative Pipelines, Lakeflow Jobs, Unity Catalog, AI/BI Dashboard.
- **Done so far:** ingestion, bronze, silver and gold. **Under way:** metric views, automated checks as a job task, dashboard.
- **Detailed design:** [docs/design.md](uk-carbon-intensity-lakehouse/docs/design.md).
- **Data source:** Carbon Intensity API, National Energy System Operator (NESO), CC BY 4.0. This project is not endorsed by NESO.

---

## 🤝 How I Work on These Projects

I do the **analysis, the design and the end to end testing**. The code is developed with an AI coding assistant (Claude Code), working from my analysis and design documents. I state this openly: what I bring is the judgement around the code, meaning what to build, how to model the data and how to verify that it works.

Each layer goes through its own pull request, with its tests and its documentation.

---

## 💼 Professional Background

**25 years** designing, implementing and optimizing **mission-critical database systems** for Public Utilities (energy, gas, water) serving 500k+ customers:

- **Database Architecture:** Oracle 8i through 19c (production), complete lifecycle management
- **Performance Engineering:** Query optimization (70% improvement achieved), execution plan analysis, indexing strategies
- **Data Modeling:** E-R diagrams, normalization, physical database design for enterprise scale
- **Data Integration:** High-volume ETL processes (10M+ records/day), system integration, data quality frameworks
- **Mission-Critical Systems:** 99.9% uptime, <5% defect rate, regulatory compliance (ARERA)

### Current Tech Stack

- **Databases:** Oracle 19c (expert), PL/SQL, SQL optimization, PostgreSQL
- **Data Engineering:** Databricks and Apache Spark (certified at Associate and Professional level), Lakeflow Spark Declarative Pipelines, Lakeflow Jobs, Delta Lake, Unity Catalog, Python (intermediate) with Pandas
- **In progress:** dbt and Airflow
- **Tools:** Git, GitHub Copilot, Claude Code
- **Methodologies:** Agile/Scrum (Professional Scrum Developer I certified)

### Certifications

- 🏅 **Databricks Certified Professional Data Engineer** (September 2026)
- 🏅 **Databricks Certified Data Engineer Associate** (July 2026)
- 🏅 **Oracle Cloud Infrastructure 2025 Certified AI Foundations Associate** (July 2025)
- 🏅 **GitHub Copilot Certification** (July 2025)
- 🏅 **Oracle APEX Professional** (2024)
- 🏅 **Professional Scrum Developer I** (2019)
- 🏅 **100 Days of Code: The Complete Python Pro Bootcamp** (Udemy, April 2026)
- 🏅 **Cambridge Advanced (CAE), C1 English**
- 🏅 **DELE C1, Instituto Cervantes, C1 Spanish** (February 2026)

---

## 📁 Repository Structure

```
DataEngineeringPortfolio/
├── README.md                          # this page: overview of all projects
├── LICENSE
└── uk-carbon-intensity-lakehouse/     # project 01, in progress
```

### Project Folder Convention

| File | Purpose |
|---|---|
| `README.md` | **Project overview**: the problem, the architecture at a glance, what is in the folder, how to reproduce it, known limits |
| `docs/design.md` | **Detailed design**: source behaviour, decisions layer by layer, data quality rules, orchestration |
| `retrospective.md` | **Lessons learned**: open tasks, next steps and what I would do differently (written when a project is complete) |

---

## 🌱 Open to Opportunities

I'm seeking **Data Engineer**, **Senior Database Developer** or **Data Architect** roles in **remote-first** companies.

- Full remote or hybrid (maximum 1 office visit per month)
- Teams that value both depth of experience and continuous learning
- Opportunities to apply database expertise while growing modern data engineering skills
- Professional development support (certifications, training, conferences)
- International environment (to use English C1 and Spanish C1)

**Availability:** Negotiable notice period

---

## 📫 Let's Connect

- 💼 [LinkedIn](https://linkedin.com/in/antonio-c-di-felice)
- 📧 antonio.difelice@tiscali.it
- 🐙 [GitHub](https://github.com/Antdfl) (you're already here!)
- 📘 [MyProfessionalPortfolio](https://github.com/Antdfl/MyProfessionalPortfolio) | [LearningPython3](https://github.com/Antdfl/LearningPython3)

---

## 📄 License

Released under the **GNU General Public License v3.0** (see `LICENSE`). Starter code, templates and other third party material from courses keep their original licenses. Data used by each project keeps its own licence, stated in the project.

---

**Last Updated:** November 2026
