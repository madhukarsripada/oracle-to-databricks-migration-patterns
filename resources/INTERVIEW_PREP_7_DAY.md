# 7-Day Interview Readiness Sprint

> For Oracle-to-Databricks data engineering / data architect roles
> Companion to the patterns in [/README.md](../README.md)

This is a structured 7-day plan to be **ready** for technical screens that may come in 5-14 days from the outreach batch. **~90 minutes per day, no more.** The goal is fluency on what you already know, not learning new things.

---

## Day 1: Crystallize the 5 Most-Asked Patterns

**Goal:** Be able to explain these 5 patterns in 60-90 seconds each, out loud, without notes.

| Pattern | Quick mental anchor |
|---|---|
| Oracle MERGE → Delta MERGE INTO | "Watermark predicate moves from WHERE to the WHEN MATCHED AND clause. Delta rewrites whole files, so Z-ORDER + partition pruning are essential on big tables." |
| SCD Type 2 in Delta | "The two-row UNION ALL with a NULL merge_key trick. One row closes the old version, one opens the new. No SEQUENCE so I use a hash-based surrogate key." |
| Partition Exchange → REPLACE WHERE | "Build the new partition data, then `mode=overwrite` with `replaceWhere` matching the partition boundaries exactly. Atomic, idempotent." |
| Materialized View → DLT | "DLT @dlt.table with streaming source replaces FAST refresh. EXPECT decorators replace ODI CKM checks. No Oracle-style query rewrite — be honest about that." |
| Cursor loop anti-pattern | "Every PL/SQL cursor loop has a set-based equivalent. Translating to a Python for-loop is the #1 migration mistake — it serializes on the driver." |

**Today's deliverable:** Read the README of this repo (your own repo) end to end. For each of the 5 patterns above, close the doc and explain it out loud to your phone's voice memo. Listen back. Where you stalled is where you re-read.

**Time:** 75 minutes.

---

## Day 2: Delta Lake Internals That Screeners Probe

**Goal:** Speak to Delta Lake internals at the level of someone who's debugged production issues.

Topics to lock down, with one-line mental anchors:

- **Transaction log (`_delta_log`)** — "JSON files per commit, periodically compacted to parquet checkpoints. Source of ACID guarantees and time travel."
- **OPTIMIZE vs Z-ORDER** — "OPTIMIZE compacts small files (the small-file problem from streaming/MERGE). Z-ORDER colocates rows by columns you frequently filter on. Together they're the daily housekeeping job."
- **VACUUM and retention** — "VACUUM removes files no longer referenced by the log, default retention 7 days. Lower it and you lose time-travel and break long-running readers."
- **Schema evolution** — "`mergeSchema = true` on write allows new columns. `overwriteSchema = true` allows breaking changes. Auto-merge is unsafe in production unless you have schema contracts upstream."
- **Auto Optimize / Auto Compact** — "Table properties `delta.autoOptimize.optimizeWrite` and `autoCompact`. Reduce small-file pain at write time."
- **Generated columns** — "Compute partition columns from a base column. Lets the optimizer prune even when queries filter the base column."
- **Liquid clustering (DBR 13.3+)** — "Newer alternative to partitioning + Z-ORDER. One key set, no partition skew problems. 2025 interview hot topic — know it exists."

**Today's deliverable:** Write a one-paragraph internal-blog-post-style explanation of OPTIMIZE + Z-ORDER + the small-file problem. If you can't write it in your own words, re-read.

**Time:** 90 minutes.

---

## Day 3: Medallion Architecture and Data Quality

**Goal:** Architect a medallion solution out loud for an unfamiliar use case.

### The 90-second "design a medallion" script

> "Bronze is the immutable landing zone — raw source data with minimal transforms, just lineage columns. Silver is the cleaned, conformed, business-key-resolved layer where I apply DQ rules and joins to reference data. Gold is the aggregated, denormalized layer optimized for specific consumer use cases — BI tools, ML feature tables, executive dashboards.
>
> Data flows are streaming or scheduled batch using DLT or Workflows. DQ checks live as DLT EXPECT decorators — `expect_or_drop` for soft failures, `expect_or_fail` to halt the pipeline. I always have an audit table tracking row counts, rejections, and reconciliation between layers.
>
> Late-arriving data: bronze accepts everything timestamped. Silver uses watermark logic in MERGE to handle out-of-order arrivals. Gold rebuilds from silver on a schedule, so late-arriving silver rows flow through naturally.
>
> Unity Catalog governs the whole thing — three-level namespace, fine-grained access at the column and row level, lineage tracked automatically."

**Today's deliverable:** Pick any of the 24 companies you emailed. Imagine their data — Veeam backs up enterprise systems, so think backup metadata. Sketch the medallion design in 10 minutes. Be ready to do this on a whiteboard if asked.

**Time:** 75 minutes.

---

## Day 4: Behavioral STAR Stories

**Goal:** Six rehearsed STAR stories at 90 seconds each. Six covers most interview scenarios.

Format: **Situation → Task → Action → Result.** Lead with the metric.

### Your 6 stories — fill in details from your actual experience:

**Story 1: Performance crisis under SLA pressure**
- *S:* At ADP, payroll aggregation job had grown to 4 hours, threatening the payroll-by-7am SLA for 10,000+ enterprise clients.
- *T:* I owned diagnosis and fix without business disruption.
- *A:* AWR/ASH analysis pinpointed a poorly-clustered fact table scan. Designed a FAST-refresh materialized view with query rewrite.
- *R:* 4 hours → 12 minutes. SLA preserved. The MV pattern became reusable for 3 other aggregations.

**Story 2: Migration program leadership**
- *S:* ADP needed 100+ SSIS packages migrated to ODI as part of platform modernization.
- *T:* Lead the technical design and offshore-onshore delivery team of 8.
- *A:* Designed target ODI Knowledge Module framework. Standardized restart/recovery patterns. Mentored team weekly through GitHub code reviews.
- *R:* 40% data refresh time reduction, zero production incidents during cutover, 8 engineers up-skilled.

**Story 3: Federal compliance under audit**
- *S:* FDA data migration with strict federal compliance requirements — zero-data-loss, full audit trails.
- *T:* Migration architect for consolidation of multiple child applications into a single parent Oracle system.
- *A:* Designed end-to-end migration with reconciliation framework (row counts, hash totals, business key matching). PII masking via PL/SQL automation during refresh. ServiceNow change management with CAB approval gates.
- *R:* Zero-data-loss cutover. Clean federal audit. Reusable reconciliation framework adopted across the program.

**Story 4: Production incident — high-pressure debugging**
- *S:* [Pick a real P1/P2 incident from ADP or Credit Acceptance — a deadlock, a runaway query, a failed batch]
- *T:* Triage, root cause, mitigate, communicate.
- *A:* [Be specific: AWR snapshot analysis, transaction order analysis, identified the conflicting transaction, deployed code fix via emergency change]
- *R:* MTTR, business impact avoided, postmortem actions.

**Story 5: Stakeholder conflict / disagreement**
- *S:* [Pick a real case — a business stakeholder wanted X, engineering knew Y was better. Or an offshore-onshore alignment issue.]
- *T:* Build alignment without slowing delivery.
- *A:* [Specific listening, specific compromise, specific data brought to the conversation]
- *R:* [Outcome + relationship preserved]

**Story 6: The Databricks pivot story (essential for these interviews)**
- *S:* "I'd spent 16 years deep in Oracle. By 2023, every enterprise client conversation kept surfacing one question — when and how do we move off Oracle?"
- *T:* "Reposition my skills so I could lead these migrations, not be threatened by them."
- *A:* "Started hands-on with Databricks at Credit Acceptance — led the migration readiness program for 500K+ lines of PL/SQL. Built oracletospark.io as a live conversion tool. Active on Databricks Data Engineer Associate cert. Published patterns at github.com/[your-handle]/oracle-to-databricks-migration-patterns."
- *R:* "Now positioning as a migration specialist — rare combination of deep legacy Oracle knowledge and modern lakehouse skills. That bridge is what my next role needs."

**Today's deliverable:** Voice memo each of the 6 stories. Listen back. Trim each to under 90 seconds. The "Databricks pivot" story is the most important — it's the answer to "why are you a fit for this Databricks role with so much Oracle on your resume?"

**Time:** 90 minutes.

---

## Day 5: Mock Technical Interview, Out Loud

**Goal:** Stress-test fluency under pressure.

Run through these 10 questions out loud, timed. 2-3 minutes max per answer. Record yourself.

1. Walk me through how you'd migrate a 500-table Oracle warehouse to Databricks. What's the sequencing?
2. A junior engineer wrote a PL/SQL cursor loop and ported it to Spark as a Python for-loop. It's slow. What do you tell them?
3. Design a Medallion architecture for ingesting GoldenGate CDC from an Oracle OLTP source.
4. How does Delta MERGE work internally? Why is it slow on large tables, and how do you mitigate?
5. Explain SCD Type 2 implementation in Delta Lake. What's the two-row UNION pattern?
6. What's the difference between OPTIMIZE and Z-ORDER? When do you use each?
7. How do you handle late-arriving data in a streaming Medallion pipeline?
8. What's the equivalent of an Oracle materialized view with FAST refresh in Databricks?
9. How would you reconcile data after migrating an Oracle fact table to Delta? Walk me through your strategy.
10. Tell me about a time a migration went wrong in production. What did you learn?

**Today's deliverable:** Self-score after listening back. For each question, rate yourself 1-5. Anything below a 3, mark for re-prep. Send yourself a calendar reminder for Day 7 to re-do those.

**Time:** 90 minutes.

---

## Day 6: Build the GitHub Presence

**Goal:** Push the patterns repo, polish your LinkedIn, write a short post.

**Morning (45 min):**

1. Create a GitHub account if you don't have one (or use existing). Public.
2. Create a new public repo: `oracle-to-databricks-migration-patterns`
3. Push the contents of this repo to it (instructions below)
4. Update the repo description: "Production-grade translation patterns for migrating Oracle DW workloads to Databricks Lakehouse"
5. Add topics: `databricks`, `delta-lake`, `oracle`, `migration`, `data-engineering`, `pyspark`, `lakehouse`
6. Pin it on your GitHub profile

**Afternoon (45 min):**

1. LinkedIn About section — rewrite to lead with the migration specialty
2. LinkedIn Featured section — pin the new GitHub repo + oracletospark.io
3. Write one LinkedIn post: "I've open-sourced 12 Oracle-to-Databricks migration patterns I've used in production. [Link]. Most data engineers in migration projects struggle with the same translation problems — SCD Type 2 without sequences, MERGE performance on big tables, ODI Knowledge Module equivalents in Workflows. This is the reference I wish I had three years ago."

**Today's deliverable:** Live GitHub URL, updated LinkedIn, published post. The post will land in the feeds of recruiters at the 24 companies you emailed — reinforces the cold email.

**Time:** 90 minutes.

---

## Day 7: Buffer, Review, Polish

**Goal:** Address weaknesses, prepare for the calls coming in.

1. Re-do the Day 5 questions you scored below 3 on. Score yourself again.
2. Re-read your resume out loud. Anything you can't speak to with confidence — remove it or rewrite it. A common trap: the resume claims something you did briefly 5 years ago and an interviewer drills into it.
3. Pull recent (last 6 months) data engineering blog posts from 5 of your target companies. Note 2-3 things from each to drop into interviews — "I saw your team blogged about X — how does that interact with Y?"
4. Set up your interview environment: clean shirt visible on camera, quiet space, good lighting, water glass, notebook for note-taking, second monitor with this repo open for reference.
5. Practice the "tell me about yourself" 90-second opener until it's natural. This is the one question you'll be asked in every single call.

**The "tell me about yourself" opener template:**

> "I'm a data engineer with 18 years of Oracle DW experience — built and ran platforms for MasterCard's global payments network, ADP's HR/payroll platform serving 10,000+ clients, and a federal healthcare data migration for the FDA. The last 18 months I've been leading Oracle-to-Databricks migration programs, including a 500K-line PL/SQL assessment for Credit Acceptance. I built oracletospark.io as a live conversion tool, and I've open-sourced my migration patterns at [github link]. I'm looking for the next role where that bridge skill — deep Oracle legacy plus modern lakehouse — is what the team needs."

**Time:** 90 minutes.

---

## Pushing the Repo to GitHub

Once you have a GitHub account, from your local terminal:

```bash
cd /path/to/oracle-to-databricks-migration-patterns
git init
git add .
git commit -m "Initial release: 12 Oracle-to-Databricks migration patterns + production reconciliation framework"
git branch -M main
git remote add origin https://github.com/YOUR_USERNAME/oracle-to-databricks-migration-patterns.git
git push -u origin main
```

Then in the GitHub web UI:
1. Settings → Pages → enable GitHub Pages on main branch — gives you a public README rendered as a site
2. Add the repo URL to your resume header (it can replace one of the other links if space is tight)
3. Add the repo URL to your LinkedIn Featured section
4. Add the repo URL to your email signature

---

## The honest finish line

You will not be perfect on Day 7. The point is fluent enough to perform in a first screen. The interviews themselves will teach you the rest.

The pattern to avoid: spending Day 8, Day 9, Day 10 doing "one more day of prep" when calls are coming in. **Once the first call lands, the prep continues by *doing* the interviews, not by avoiding them.**
