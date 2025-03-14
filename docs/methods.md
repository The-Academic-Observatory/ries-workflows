# Methods

- [Methods](#methods)
- [Overview](#overview)
- [ETL / Generated Inputs](#etl--generated-inputs)
  - [ISSNs](#issns)
  - [FoRs](#fors)
  - [RoRs](#rors)
  - [HEPs](#heps)
  - [Journals](#journals)
  - [Papers](#papers)
- [Generated Outputs](#generated-outputs)
  - [Core Outputs](#core-outputs)
  - [Research Outputs](#research-outputs)
  - [Low Volume Threshold](#low-volume-threshold)
  - [Interdisciplinary Profiles](#interdisciplinary-profiles)
  - [Citations Per Paper (CPP) Benchmark](#citations-per-paper-cpp-benchmark)
  - [High Performance Indicator (HPI) Benchmark](#high-performance-indicator-hpi-benchmark)
  - [Relative Citation Impact (RCI)](#relative-citation-impact-rci)
  - [ERA 2018 RCI Class (static)](#era-2018-rci-class-static)
  - [ERA 2023 RCI Class (dynamic)](#era-2023-rci-class-dynamic)
  - [Centile Analysis (Distribution of Papers)](#centile-analysis-distribution-of-papers)
  - [Performance Ratings](#performance-ratings)
    - [ERA 2018 Ratings](#era-2018-ratings)
    - [ERA 2023 Option A Ratings](#era-2023-option-a-ratings)
    - [ERA 2023 Option B Ratings](#era-2023-option-b-ratings)
- [Known Issues](#known-issues)

# Overview

This project implements a subset of ERA indicators and the methods used to build them. This document describes the sequence of methods required to replicate the project's workflow. Provided that the [required inputs](#required-inputs) are available, then the whole workflow can be (re)run at any time. 


# ETL / Generated Inputs

A series of ETL scripts (referred to internally as `telescopes`) are used to prepare the primary datasets. The ETL scripts may be re-run at any time. Some need only run once, others should be re-run regularly. ETL scripts are named as `telescope_*.js` and are found in the `code/telescopes` directory.

- **ISSNS:** International Standard Serial Number. Sourced from [issn.org](https://www.issn.org/)
- **FORS:** ANZSRC Field of Research codes. Sourced from [abs.gov.au](https://www.abs.gov.au/statistics/classifications/australian-and-new-zealand-standard-research-classification-anzsrc/latest-release)
- **RORS:** Research Organisation Registry codes. Sourced from [ror.org](https://ror.org/)
- **HEPS:** Australian Higher Education Providers. Sourced from [arc.gov.au](https://www.arc.gov.au/excellence-research-australia)
- **JOURNALS:** Subset of journals (id by ISSN) used for ERA analysis. Sourced from [arc.gov.au](https://www.arc.gov.au/excellence-research-australia)
- **PAPERS:** Subset of papers (id by DOI (digital object identifier)). Sourced from [COKI](https://openknowledge.community/)

## ISSNs

- this method is implemented by `node ./code/telescopes/telescope_issns.js` and may be re-run at any time.
- [international standard serial numbers](https://en.wikipedia.org/wiki/International_Standard_Serial_Number) are used to connect papers with the journals that publish them
- an authoritative ISSN <-> ISSNL mapping is downloaded from [issn.org](https://issn.org)
- the mapping is then loaded into BigQuery

## FoRs

- this method is implemented by `node ./code/telescopes/telescope_fors.js` and may be re-run at any time.
- authoritative lists of ANZSIC(https://www.abs.gov.au/statistics/classifications/australian-and-new-zealand-standard-research-classification-anzsrc) Field of Research codes are downloaded
- ERA18 analysis uses the v2008 codes. ERA23 uses v2020 codes.
- codes are then transformed and loaded into BigQuery for downstream use.

## RoRs

- this method is implemented by `node ./code/telescopes/telescope_rors.js` and may be re-run at any time.
- an authoritative list of RoR codes is downloaded from the [Research Organisation Registry](https://ror.org)
- the list is then loaded into BigQuery and is used later to connect organisations to publications.

## HEPs

- this method is implemented by `node ./code/telescopes/telescope_heps.js` and may be re-run at any time.
- the ERA process selectes a subset of institutions as Australian Higher Education Providers (HEPs)
- this set is extracted from the ERA 2018 documentation and may be found in `./docs/era_2018_heps.json`.
- the set of RoRs is loaded into BigQuery and cross-referenced against the `era.rors` table to assure correctness and extract titles.

## Journals

- this method is implemented by `node ./code/telescopes/telescope_journals.js` and may be re-run at any time.
- the ERA process defines a [select set of approved journals](https://www.arc.gov.au/evaluating-research/excellence-research-australia/past-era-evaluation), used during ERA analysis
- The 2018 set is [here](https://web.archive.org.au/awa/20220309020544mp_/https://www.arc.gov.au/file/10549/download?token=Sbfb2a9n)
- The 2023 set is [here](https://www.arc.gov.au/sites/default/files/2022-07/ERA2023%20Submission%20Journal%20List.xlsx)
- records are extracted from the Excel files, converted to JSONL and uploaded to BigQuery
- ISSN values, in the ERA data, are cross-referenced against the official ISSN set and checked for possible duplicates.

## Papers

- this method is implemented by `node ./code/telescopes/telescope_papers.js` and may be re-run at any time.
- COKI hosts an aggregated dataset of publications (identified by DOI) that draws primarily from [Crossref](https://www.crossref.org/), [OpenAlex](https://openalex.org/), and [unpaywall](https://unpaywall.org/)
- a subset of papers is extracted from the most recent `observatory.doi` table. 

[top]

---

# Generated Outputs

Once the ETL scripts (telescopes) have all been run and the raw data are loaded, the workflow then prepares a set of secondary tables before proceeding to analysis.

The analysis phase builds a subset of ERA indicators (from ERA 2018 and ERA 2023). The ERA process aims to apply a qualitative activity rating to select institutions (HEPs) for each field of research (FoR) in which the institution is active. It additionally reports aggregate statistics for institutions and for fields of research.


## Core outputs

- these methods are implemented by the `./code/queries/core_*.js` scripts `core_rors.js`, `core_heps.js`, `cor_fors.js`, `core_journals.js`, `core_papers.js` and create secondary tables
- ISSN values for papers are converted to ISSNL values (using the ISSNL table) and de-duplicated.
- papers are then linked to ERA journals via shared ISSNLs, creating an XREF table between DOI (paper) and ERA_ID (journal)
- where RoR codes are available, institutions (and HEPs) are linked to papers via the RoR codes
- as FoR codes (with apportionment) are not assigned in the DOI records, papers inherit codes from the linked journal, with weighting being evenly split between the number of codes
- a subset of papers is restricted to journal-articles that were published within the ERA analysis window, that can be linked to journals in the ERA journal list (via ISSN) and that have been linked to at least one recognised research institution.
- the final table that the rest of the analyses are built upon is implemented by `core_assignments.js`
- Fields of education are mapped to fields of research codes by `./code/queries/translate_fors_to_foes.js`


## Research Outputs

- this method is implemented by `./code/queries/benchmark_outputs.js`
- a tally of research outputs (papers) is computed, by institution, field of research and year (RoR,FoR,year)
- metrics include:
  - sum_papers       - total number of publications
  - sum_citations    - total number of citations (not weighted / adjusted)
  - sum_portions     - sump of FoR apportionment
  - avg_citations    - citations per output (using totals)
  - cent_papers      - total outputs adjusted by FoR apportionment (sum of the fractional assignment to an FoR)
  - cent_outputs     - percentile based on net outputs
  - cent_portions    - percentile based on weighted outputs
  - cent_citations   - percentile based on net citations
  - cent_cpp         - percentile based on citations per output
  - rank_papers      - rank based on net outputs
  - rank_citations   - rank based on net citations
  - rank_portions    - rank based on weighted outputs
  - rank_cpp         - rank based on citations per output

- weighted outputs tally the fraction that each paper is assigned to a particular FoR (apportionment)
- 2-digit FoRs are computed separately to 4-digit FoRs
- 2-digit analysis aggregates numbers from the 4-digit FoRs based on the first two digits
- from the primary table (RoR,FoR,year), aggregates are created as SQL views for:
  - (RoR)      - total outputs for each institution over the ERA period(s)
  - (FoR)      - total outputs for each field of research over the ERA period(s)
  - (year)     - total outputs per year over the ERA period(s)
  - (RoR,FoR)  - total outputs per institution and field of research over the ERA period
  - (RoR,year) - total outputs per institution per year of the ERA period
  - (FoR,year) - total outputs per discipline per year of the ERA period
- the entire set is then repeated (again as SQL views), but this time only for Australian HEPs
  - HEP(RoR,FoR,year) - total outputs for each institution over the ERA period(s)
  - HEP(RoR)      - total outputs for each institution over the ERA period(s)
  - HEP(FoR)      - total outputs for each field of research over the ERA period(s)
  - HEP(year)     - total outputs per year over the ERA period(s)
  - HEP(RoR,FoR)  - total outputs per institution and field of research over the ERA period
  - HEP(RoR,year) - total outputs per institution per year of the ERA period
  - HEP(FoR,year) - total outputs per discipline per year of the ERA period
- NOTE: in ERA, outputs are sub-divided by output type (book, book-chapter, journal article, etc). In this analysis, only journal articles are considered.
- NOTE: in ERA, analysis is limited to Australian HEPs and the ERA timeframe. In this analysis, these constraints can be removed if desired.
- cumulative percentages may also be provided if desired

[top]

---

## Low Volume Threshold

- this method is implemented by `ind_low_volume.js` 
- ERA ratings are assigned to an each (RoR,FoR) pair only if the institution meets a minimum activity threshold
- the threshold is defined as >= 50 weighted outputs per FoR over the ERA period
- the low-volume flag does not take into account individual years within the ERA period. It is all-or-nothing
- the low-volume table is a sub-query of the research outputs table and may be implemented as a view
- the analysis can be extended to also apply to Journals although the threshold of 50 is unlikely to be ideal -->

[top]

---

## Interdisciplinary Profiles

- this method is implemented by `ind_interdisc.js` 
- for each field of research, the analysis shows how often the field is linked to other fields. This is useful for highlighting interdisciplinary activity / crossver. Linkage is identified by two FoR codes being assigned to a given research output (publication / paper)
- analysis is separated into 2-digit and 4-digit coding
- the base table is produced for each (insitution,field,year) and provides:
  - num               - total number of times the two fields are listed together
  - weight            - total weighted number of times the two fields are listed together
  - pct_num           - rank (percentile) based on total co-occurrence
  - pct_weight        - rank (percentile) based on weighted co-occurrence
- from the primary table, aggregates are created as SQL views for:
  - (insitution,field) - co-occurrence between fields, per insitution, over the whole ERA period
  - (field)            - co-occurrence between fields over the whole ERA period
  - (field,year)       - co-occurrence between fields, per year of the ERA period

[top]


---

## Citations Per Paper (CPP) Benchmark

- this method is implemented by `./code/queries/benchmarks_cpp.sql`
- ERA performence ratings are assigned by comparing citation counts to benchmark levels
- benchmarks include all journal articles, even if they were in groups that failed to meet the low volume threshold
- for each (FoR,year), a global CPP benchmark is defined as:

```sql
benchmark(FoR,year) = SUM(num_citations(FoR,year)) / SUM(num_articles(FoR,year))
```

- the benchmarks table is a subquery from the research outputs tables
- apportionment is not used in this process for either citation counts of output counts
- a set of **global CPP benchmarks** is created by using all papers published during the ERA period
- a set of **local CPP benchmarks** is created by filtering only for papers published by HEPs during the ERA period
- care must be exercised when computing the benchmarks to not double-count papers that have cross-institutional authorship
- NOTE: in ERA, if different institutions conflict on the assignment of FoR codes for the same paper, this is flagged and resolved manually.
- benchmarks are computed for:
  - (field,year) - average citations per paper, per field, per year
  - (field) - average citations per paper per field over the entire ERA period (note that this is sensitive to time bias as older papers accumulate more citations)
  - (year) - average citations per paper per year over all fields (note that this is sensitive to differences in activity between fields)
- two sets are computed, one for the global set of institutions, the other for the local set of HEPs

[top]

---

## Centile Analysis (Distribution of Papers)

- this method is implemented by `./code/queries/benchmark_centiles.js`
- centile boundaries are tabulated based on unapportioned citation counts
- a second centile boundary tabulation is based on apportioned output counts
- articles are grouped by (FoR,year) and sorted by citation count
- centile boundaries are defined at 1%, 5%, 10%, 25%, 50% (median) and 100% (total) of papers
- if a centile band lower threshold is 0, then uncited papers will be included in the band 
- Example (not all fields shown):
  | `centile` | `papers_net` | `papers_pct` | `local_pct` |
  | --- | --- | --- | --- |
  | `c1`   | float | float | float |
  | `c5`   | float | float | float |
  | `c10`  | float | float | float |
  | `c25`  | float | float | float |
  | `c50`  | float | float | float |
  | `num_outputs`   | float | float | float |
- Where:  
  | field        | description |
  | ------------ | ----------- |
  | `c1`      | first percentile (top 1%), sorted by number of citations descending |
  | `c5`      | ...top 5% |
  | `c10`     | ...top 10% |
  | `c25`     | ...top 25% |
  | `c50`     | ...median (ie, top 50%) |
  | `num_outputs`      | ...total number of outputs |

[top]

## High Performance Indicator (HPI) Benchmark

- this method is implemented by `./code/queries/benchmark_hpi.js`
- it assigns a benchmark for each (FoR,year) based on the highest performing insitutions in that field
- all global institutions are included (not just HEPs)
- for each (FoR), institutions are dropped from the analysis if they fall below the [low-volume threshold](#low-volume-threshold)
- a (FoR) is dropped from the analysis if < 10 insitutions are above the low volume threshold
- for each (FoR,year) the remaining institutions are sorted by CPP (descending)
- the CPP is then recalculated for the top 10% and this becomes the HPI benchmark for that (FoR,year).

```sql
CPP(RoR,FoR,year) = sum citations(RoR,FoR,year) / num papers(RoR,FoR,year)
-- where papers are per institution (RoR)
HPI_CPP (FoR, year) = sum citations(FoR,year) / num papers(FoR,year)
-- where papers is the top 10% subset
```

- as with the global benchmark, a high-performance benchmark can be computed for:
  - (field,year) - high performance benchmark per field, per year
  - (field) - high performance benchmark per field over the entire ERA period (noting bias)
  - (year) - high performance benchmark per year over all fields of research (noting bias)

[top]

---

## Relative Citation Impact (RCI)

- this method is implemented by `./code/queries/benchmark_rci_papers.js` and depends on the [CPP benchmarks](#citations-per-paper-cpp-benchmark) and [HPI benchmark](#high-performance-indicator-hpi-benchmark)
- Relative Citation Impact (RCI) scores are calculated by dividing the number of citations by the various benchmarks (local, world, high-performing)
- RCIs are calculated for each (FoR,year) and are calculated for individual papers, then as an aggregation for an FoR.
- when aggregating, RCIs are reported at the institutional and world level
- apportionment is not involved in the RCI calculation for an individual paper
- a warning should be triggered if an RCI is >= 8 (percentile analysis may be more appropriate).

```sql
  rci_world(article,FoR) = article_num_citations / benchmark_world(year,FoR);
  rci_local(article,FoR) = article_num_citations / benchmark_local(year,FoR);
  hpi_world(article,FoR) = article_num_citations / benchmark_high_performance(year,FoR);
```

- NOTE: although not done in ERA, apportionment could be used to calculate a weighted-average RCI for a paper (across the various fields of research it is associated with), as follows:

```sql
  -- note that the denominator is always 1.0 because it's the sum of portions which always sum to 1
  rci_world(article) = (for1_portion * rci_world(article,FoR_1) + for2_portion * rci_world(article,FoR_2) ... ) / 1.0
  rci_local(article) = (for1_portion * rci_local(article,FoR_1) + for2_portion * rci_local(article,FoR_2) ... ) / 1.0
  hpi_world(article) = (for1_portion * hpi_world(article,FoR_1) + for2_portion * hpi_world(article,FoR_2) ... ) / 1.0
```

- unlike for papers, the RCI for an entire FoR does use apportionment
- when aggregating RCIs (from papers), the summary statistic is a weighted average of the RCIs from individual papers, weighted by the fractional assignment of the paper to a given FoR
- this can be computed for any time range as RCI values are time-normalised.
- NOTE: in ERA, if a FoR has less than 75 indexed papers, then a warning is shown to look at centiles and RCI class analysis
- NOTE: in ERA, where a 4-digit FoR has < 250 articles, between all HEPs combined, then a low-volume warning is triggered
- If a benchmark value is zero, the paper's RCI will not be included in the calculation.

```sql
  -- note that the denominator is always 1.0 because it's the sum of portions which always sum to 1
  rci_world(RoR,FoR) = (for1_portion * rci_world(article,FoR_1) + for2_portion * rci_world(article,FoR_2) ... ) / 1.0
  rci_local(RoR,FoR) = (for1_portion * rci_local(article,FoR_1) + for2_portion * rci_local(article,FoR_2) ... ) / 1.0
  hpi_world(RoR,FoR) = (for1_portion *  hpi_world(article,FoR_1) + for2_portion *  hpi_world(article,FoR_2) ... ) / 1.0
```

- aggregated RCIs are computed for:
  - (institution,field,year)
  - (institution,field)
  - (institution,year)
  - (institution)
  - (field,year)
  - (field)
  - (year)
- a secondary set of tables is also produced that focuses only on the HEPs

[top]

---

## ERA 2018 RCI Class (static)

- this method is implemented by `./code/queries/benchmark_rci_classes.js` and is specific to ERA 2018
- After each paper has been assigned a set of RCI scores (a global:local pair for each FoR), then it is assigned a matching set of RCI classes.
- Scores are binned into 7 RCI classes
  - class 0: no citations
  - class 1: RCI < 0.8
  - class 2: RCI < 1.2
  - class 3: RCI < 2
  - class 4: RCI < 4
  - class 5: RCI < 8
  - class 6: RCI unlimited
- After all RCI classes have been assigned to papers, tallies for each class are aggregated using the FoR fractions for each paper.
- Tallies combine all years. This is acceptable because RCIs are year-normalised by comparing to year-specific benchmarks.

[top]

---


## ERA 2023 RCI Class (dynamic)

- this method is implemented by `./code/queries/benchmark_rci.js` and is specific to ERA 2023
- the static ERA 2018 method assigns 7 RCI classes with the same fixed boundaries used for all FoRs
- the dynamic ERA 2023 method computes class boundaries individually for each FoR
- the new method has 6 RCI classes (0-5) with 0 indicating uncited
- 2-digit FoR statistics will be determined from the 4-digit data
- papers are assigned RCI scores (previously) based on the (FoR,year) benchmarks
- for each (FoR,year) a series of RCI class boundaries are then recursively calculated as:

| class | metric |
| --- | --- |
| 0 | uncited papers (RCI 0) |
| 1 | RCI > 0, <= 1.00 (ie, at or below average RCI) |
| 2 | RCI(rank 1) to <= mean RCI(rank > 1) |
| 3 | RCI(rank 2) to <= mean RCI(rank > 2) |
| 4 | RCI(rank 3) to <= mean RCI(rank > 3) |
| 5 | RCI(rank 4) to <= mean RCI(rank > 4) |

- class tallies are then calculated for institutional comparisons

[top]

---

## Performance Ratings

The final product of ERA analysis is to assign a relative performance rating to each institition. This is a qualitative assessment made by ERA committee members and, although guided by RCI and centile metrics, is not dictated by them. There isn't a strict formula for assigning ratings.

Nevertheless, this workflow does attempt to assign a rating to all (institution,FoR) pairs (not just HEPs). The method is implemented in `./code/queries/ind_ratings.js`.

ERA provides ratings for each (HEP,FoR) pairing. A key change between ERA 2018 and ERA 2023 is in the assignment of ratings to units of evaluation. Due to changes in the methodology, ERA 2023 ratings will not be directly comparable with ratings from ERA 2018.

The various ratings scales for ERA 2018 and ERA 2023 are described below

[top]

---

### ERA 2018 Ratings

- this method is implemented in `./code/queries/ind_ratings.js`
- the `rci band` is our own definition and is not an official ERA method

| rating | assessment | approximate 2023 equivalents | rci band |
| - | - | - | - |
| 5 | well above world standard | A:5,4 B:6,5,4 | >= 1.6          |
| 4 | above world standard      | A:3 B:3       | >= 1.2 .. < 1.6 |
| 3 | at world standard         | A:2 B:2       | >= 0.8 .. < 1.2 |
| 2 | below world standard      | A:1 B:1       | >= 0.4 .. < 0.8 |
| 1 | well below world standard | A:1 B:1       |           < 0.4 |

- n/a: does not meet the low volume threshold
- n/r: not rated due to quality control concerns in the data

[top]

---

## ERA 2023 Option A Ratings

The high performer and the world benchmarks are used for the top three ratings groups. Only the world benchmark is used for the lower ratings. The rating represents a comparison of an Australian higher education provider (HEP) to other global institutions.

- this method is implemented in `./code/queries/ind_ratings.js`
- the `(rci|hpi) band` is our own definition and is not an official ERA method

| rating | similar to | rci band | hpi band |
| --- | --- | --- | --- |
| world leading             | 2018:5   | >= 1.6          | >= 1.2          |
| well above world standard | 2018:5   | >= 1.6          | >= 0.8 .. < 1.2 |
| above world standard      | 2018:4   | >= 1.2 .. < 1.6 |           < 0.8 |
| at world standard            | 2018:3   | >= 0.8 .. < 1.2 |                 |
| not at world standard     | 2018:2,1 |           < 0.8 |                 |

[top]

---

## ERA 2023 Option B Ratings

Under option B, for assigning ERA 2023 ratings, the HPI is used exclusively for the upper three ratings and the RCI is used exclusively for the lower three ratings. The rating represents a comparison of an Australian higher education provider (HEP) to other global institutions.

- this method is implemented in `./code/queries/ind_ratings.js`
- the `(rci|hpi) band` is our own definition and is not an official ERA method

| rating | similar to | rci band | hpi band |
| --- | --- | --- | --- |
| AAA | 2018:5   |                 | >= 1.6          |
| AA  | 2018:5   |                 | >= 1.2 .. < 1.6 |
| A   | 2018:5   |                 | >= 0.8 .. < 1.2 |
| B   | 2018:4   | >= 1.2          |                 |
| C   | 2018:3   | >= 0.8 .. < 1.2 |                 |
| D   | 2018:2,1 |           < 0.8 |                 |

[top]

---

# Known Issues

- **Lack of RoR mapping** There is a known lack of RoR (research organisation registry) data associated with publications. For some papers, insufficient author metadata is available to link each author to a RoR-listed institution. This results data loss as papers with no RoR data are dropped from analysis and institutional outputs are under-counted where authors cannot be linked.
- **Lack of FoR mapping**. Although the pipeline is designed to handle journal-article apportionment between up to three field-of-research (FoR codes), this information is not available in the current input data. Instead, each paper inherits FoR weightings from the encompassing journal. At the journal level, FoR weighting is uniformly apportioned to codes based on the *ERA Journal List*). This results in FoR analysis that is overly generic.
- **Ambiguous ISSNs**. Following sanitation of ISSN values (and conversion to linking ISSNs), bad input data may result in some papers linking to more than one journal. These papers are excluded from analysis pending manual correction, resulting in some data loss.
- **No valid ISSN**. The workflow validates ISSN values (for journals and papers) by mapping against an official ISSN <-> ISSNL dataset from issn.org. Following sanitation, some journals or papers may have no remaining ISSN-L value assigned. These records are excluded from analysis pending manual correction, resulting in some data loss.
- **Author Weighting**. Like the ERA process, no attempt is made to assign portional weighting based on authorship (and institutional affiliation). For example, if a paper has 9 authors from university A and 1 author from university B, the citation metrics will be assigned equally to A and B. The RoR code for institution A will not be counted 9 times, it will be only counted once.


[top]:#methods
