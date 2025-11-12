# SOEN363 Phase 2 Project - Team Implementation Plan

**Project Goal**: Migrate hospital database from PostgreSQL (Phase 1) to MongoDB (Phase 2) with 400MB+ data
**Team Size**: 5 people
**Total Project Time**: ~20-24 hours distributed (4-5 hours per person)
**Deadline**: [Insert your deadline here]

---

## 📋 Overview

This document outlines the complete Phase 2 implementation plan with:
- Clear task breakdown for 5 team members
- Collaborative report & presentation development
- Sequential workflow with proof-reading gates
- All deliverables and their specifications

**Key Principle**: Everyone works on their task AND contributes to the final report/presentation as they go.

---

## 👥 Team Task Assignments

---

### **PERSON 1: Setup NoSQL DB (MongoDB)**

**Code:**
- docker-compose.yml (MongoDB + PostgreSQL services)
- connection_script.py (basic connections to both databases)

**Doc:**
- README with setup instructions

**Report Section:**
- Setup & Docker Configuration (2 pages)

**Presentation:**
- 1-2 slides on MongoDB setup and architecture

**Demo Role:**
- Show Docker setup, both databases running, demonstrate basic connections

---

### **PERSON 2: Design NoSQL Structure**

**Code:**
- None (documentation-focused task)

**Doc:**
- NOSQL_DESIGN.md with mapping document "Table A → Collection B with embedded document C"
- Clearly document: which relationships to flatten, which weak entities to embed, which tables to denormalize/nest

**Report Section:**
- NoSQL Schema Design & Transformation Strategy (2-3 pages)

**Presentation:**
- 1-2 slides showing schema design, embed vs. reference decisions, mapping table

**Demo Role:**
- Explain design decisions, show mapping document, justify architectural choices

---

### **PERSON 3: Data Scaling + Data Extraction (SQL)**

**Code:**
- Data scaling script (expand Phase 1 PostgreSQL data to reach ~500MB - add more MIMIC-III tables or duplicate/expand existing data)
- extract_data.py (PostgreSQL → JSON intermediate format)

**Doc:**
- Data scaling report (which tables added, size progression, final size achieved)
- Extraction quality report (NULL values, foreign key issues, data anomalies)

**Report Section:**
- Data Scaling & Extraction Process (2-3 pages)

**Presentation:**
- 1-2 slides on data scaling approach + extraction methodology, type conversions, quality checks

**Demo Role:**
- Show extraction script running, demonstrate extracted JSON output, explain how 500MB was achieved

---

### **PERSON 4: Data Loading (NoSQL)**

**Code:**
- load_to_mongodb.py (transform JSON and load into MongoDB following Person 2's design, handle embedding and indexes)

**Doc:**
- Loading execution report (insertion statistics, index creation, final MongoDB stats)

**Report Section:**
- Data Loading & Transformation (2-3 pages)

**Presentation:**
- 1-2 slides on transformation logic, denormalization, batch loading, index strategy

**Demo Role:**
- Show MongoDB with loaded data, demonstrate database size/collections, show index structure

---

### **PERSON 5: Testing, Validation & Performance**

**Code:**
- validate_migration.py (verify completeness, row count matching, data integrity)
- performance_test.py (run a couple of queries on both PostgreSQL and MongoDB, compare execution times)

**Doc:**
- Validation report (all tables verified, no data loss)
- Performance comparison results

**Report Section:**
- Data Validation & Performance Analysis (2-3 pages)

**Presentation:**
- 1-2 slides on validation results, performance comparison charts/insights

**Demo Role:**
- Run validation queries, show performance comparison, explain findings

---

## 📅 Project Timeline

| Phase | Person 1 | Person 2 | Person 3 | Person 4 | Person 5 |
|-------|----------|----------|----------|----------|----------|
| **Implementation** | Docker setup + Connection script | NoSQL Design document | Data scaling + extract_data.py | load_to_mongodb.py | validate + performance scripts |
| **Documentation** | Report section (2 pages) | Report section (2-3 pages) | Report section (2-3 pages) | Report section (2-3 pages) | Report section (2-3 pages) |
| **Presentation** | 1-2 slides | 1-2 slides | 1-2 slides | 1-2 slides | 1-2 slides |
| **Demo** | Show Docker setup | Explain design | Show scaling + extraction | Show loaded MongoDB | Show validation & performance |

**Total time**: ~2-2.5 hours per person per task (implementation + documentation + presentation)

---

## 📁 Project Structure

```
/SOEN363-HospitalDB/
├── docker-compose.yml           (Person 1)
├── NOSQL_DESIGN.md              (Person 2)
├── scripts/
│   ├── connection_script.py      (Person 1)
│   ├── data_scaling.py          (Person 3 - expand PostgreSQL data)
│   ├── extract_data.py          (Person 3 - extract to JSON)
│   ├── load_to_mongodb.py       (Person 4)
│   ├── validate_migration.py    (Person 5)
│   └── performance_test.py      (Person 5)
├── reports/
│   ├── data_scaling_report.txt          (Person 3)
│   ├── extraction_quality_report.txt    (Person 3)
│   ├── loading_execution_report.txt     (Person 4)
│   ├── validation_report.txt            (Person 5)
│   └── performance_results.txt          (Person 5)
├── SOEN363_Project_Phase1_Phase2_Report.pdf (Final - compiled by team)
├── SOEN363_Project_Phase1_Phase2_Presentation.pptx (Final - compiled by team)
└── README.md
```

---

## ✅ Grading Rubric (from Assignment)

| Task | Points | Requirements |
|------|--------|--------------|
| **Task 1: Conversion to NoSQL** | 50 | All entities, attributes, relationships migrated. Design changes documented. 500MB database. |
| **Task 2: Documentation (PDF Report)** | 10 | Phase 1 & 2 process, ER diagram, design changes, migration steps, scripts, challenges. |
| **Task 3: Presentation (PPT)** | 10 | Main aspects of project, design rationale, migration methodology, performance findings. |
| **Task 4: Demo & Q&A** | 30 | 10-min demo of working system + 10-min Q&A. Clear presentation, technical accuracy. |
| **TOTAL** | **100** | - |

---

## 🎯 Next Steps

1. **Person 1**: Create docker-compose.yml and connection_script.py
2. **Person 2**: Write NOSQL_DESIGN.md with mapping document
3. **Person 3**: Write data_scaling.py (expand PostgreSQL to ~500MB) + extract_data.py (PostgreSQL → JSON)
4. **Person 4**: Write load_to_mongodb.py to load JSON into MongoDB
5. **Person 5**: Write validate_migration.py and performance_test.py

Each person writes their report section and slides while completing their implementation task.

Final deliverables:
- **PDF Report**: All 5 report sections compiled (Phase 1 recap + all 5 implementation sections)
- **PPT Presentation**: All 5 slide sets compiled (12-15 slides total)
- **Demo**: 10 minutes showing the working system (scaling, extraction, loading, validation, performance)
- **Q&A**: 10 minutes answering questions
