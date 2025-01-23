# COKI Research Impact Evaluation System

A BigQuery based system for evaluating the impact of research publications. Based on Excellence in Research for Australia (ERA).

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
![Google Cloud](https://img.shields.io/badge/GoogleCloud-%234285F4.svg?style=flat-square&logo=google-cloud)
![JavaScript](https://img.shields.io/badge/javascript-%23323330.svg?style=flat-square&logo=javascript)

Contact / Enquiries: [coki@curtin.edu.au][contact]

## Quickstart

For a more detailed explanation, see [Install][install].

```bash
# run the install script
bash install.sh 

# Create the keyfile symlink. The keyfile is your service account access to Google Cloud Platform services
ln -s /path/to/your/keyfile.json .keyfile.json

# Set up various GCP permissions and resources
bash gcp_setup.sh my_project coki_project service_account_principal

# Set up any institutional projects
bash inst_gcp_setup.sh inst_project service_account_principal
```

See [configure] for more information on the config file contents

```bash
# If this is the first run for this project:
node telescope/index.js

# Run the workflow
node ries.js
```


## Background

[Excellence in Research for Australia][ERA] (ERA) was a periodic assessment conducted by the [Australian Research Council][ARC] (ARC). The assessment focused on the activity of 42 Australian higher education providers (HEPs) across [236 ANZSRC fields of research][ANZSRC] (FoR). Performance was assessed (per HEP and FoR) by comparing research outputs to local and world benchmarks. Analysis has a citation-focus and draws from publication metadata provided by the participating HEPs.

The [Curtin Open Knowledge Initiative][COKI] (COKI) aggregates bibliometric and bibliographic data from publicly available sources such as [Crossref], [Unpaywall], [OpenCitations], [Microsoft Academic Graph], and [OpenAlex]. The resultant [BigQuery] database contains metadata for over 120 million research publications and forms the foundation for further analysis by the [COKI] team.

The RIES workflows have been developed to demonstrate how the [COKI] database may be used to run an [ERA]-like analysis. The methodology is guided by published [ERA methods] and makes use of journal-level metadata from the [ERA 2023 Journal List]. The workflows are amenable to extension, outside of the ERA scope, to include any institution (with a [ROR] identifier) and any research-topic vocabulary that has been assigned to research articles (eg, via machine-learning classifiers).

## Demo System

This codebase is free and open source ([FOSS]), however access to the COKI database is limited. 

A previous version of the codebase is available for demonstration purposes [COKI Research Impact Evaluation System Demonstration], with this repository containing the latest updates.


## Documentation

- [Installation][install] - system requirements and installation (Docker, OS X or Linux)
- [Configuration][configure] - description of configuration options
- [Method][methods] - description of the methods used to build benchmarks & indicators

## Structure

Within this code repository, a `README.md` file in each directory provides context. At this level (the top level):

| directory | description |
| - | - |
| [./code](./code)   | Application code including libraries, SQL templates, ETL scripts and workflows. |
| [./data](./data)   | Scratch area for working data, caches and temp files. Not under version control. |
| [./docs](./docs)   | System and method documentation. |

## Full Access

The full COKI dataset is recompiled weekly by the [Academic Observatory Workflows], running on the [Academic Observatory Platform]. The underlying infrastructure requires significant resourcing and we do not currently make the data resource freely available (whereas the codebases are [FOSS]).

For sustainable development and continuation of this project, our medium-term goal is to establish an institutional membership model. We are [seeking expressions of interest][contact] from institutions that would benefit from further development of an on-demand ERA-like analytical system. The system will aim to provide value to institutions by simplifying curation of research-output metadata and facilitating exploration of alternative analytical methods. For example, reporting on how Australian HEPs perform against other institutions with a focus on [Open Access] publication.

We are also happy to [discuss][contact] possible collaboration opportunities, analysis services or access models with interested individuals and institutions.

## License

[Apache 2.0](./LICENSE)

```text
Copyright 2022 Curtin University

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
```

## Contributors

**Conceptualization**: Julian Tonti-Filippini and Cameron Neylon.  
**Data curation**: Julian Tonti-Filippini,  Keegan Smith and Kathryn Napier.  
**Formal analysis**: Julian Tonti-Filippini, Kathryn Napier and Keegan Smith.  
**Funding acquisition**: Cameron Neylon.  
**Investigation**: Julian Tonti-Filippini, Keegan Smith and Kathryn Napier.  
**Methodology**: Julian Tonti-Filippini and Cameron Neylon.  
**Project administration**: Kathryn Napier and Cameron Neylon.  
**Resources**: Cameron Neylon.  
**Software**: Julian Tonti-Filippini, Keegan Smith and Cameron Neylon.  
**Supervision**: Kathryn Napier and Cameron Neylon.  
**Validation**: Julian Tonti-Filippini, Keegan Smith and Keegan Smith.  
**Visualisation**: Julian Tonti-Filippini.  
**Writing - original draft**: Julian Tonti-Filippini and Cameron Neylon.  
**Writing - review & editing**: Julian Tonti-Filippini, Keegan Smith, Kathryn Napier and Cameron Neylon.  

<!-- links -->
[ARC]: <https://www.arc.gov.au/>
[ERA]: <https://www.arc.gov.au/evaluating-research/excellence-research-australia>
[COKI]: <https://openknowledge.community/>
[ANZSRC]: <https://www.abs.gov.au/statistics/classifications/australian-and-new-zealand-standard-research-classification-anzsrc/latest-release>
[ROR]: <https://ror.org/about/>
[FOSS]: <https://en.wikipedia.org/wiki/Free_and_open-source_software>

[Crossref]: <https://www.crossref.org/>
[Unpaywall]: <https://unpaywall.org/>
[OpenCitations]: <https://opencitations.net/>
[Microsoft Academic Graph]: <https://www.microsoft.com/en-us/research/project/microsoft-academic-graph/>
[OpenAlex]: <https://openalex.org/>
[Open Access]: <https://en.wikipedia.org/wiki/Open_access>
[ISSN]: <https://www.issn.org/>

[ERA methods]: <https://web.archive.org.au/awa/20220302235108mp_/https://www.arc.gov.au/file/10668/download?token=V5AKd-29>
[ERA 2023 Journal List]: <https://www.arc.gov.au/sites/default/files/2022-07/ERA2023%20Submission%20Journal%20List.xlsx>

[BigQuery]: <https://cloud.google.com/bigquery/>
[GCS]: <https://cloud.google.com/storage>
[NodeJS]: <https://nodejs.org/en/download/>
[Docker]: <https://www.docker.com/>

<!-- COKI -->
[Academic Observatory Workflows]: <https://github.com/The-Academic-Observatory/academic-observatory-workflows>
[Academic Observatory Platform]: <https://github.com/The-Academic-Observatory/observatory-platform>
[COKI Research Impact Evaluation System Demonstration]: <https://github.com/Curtin-Open-Knowledge-Initiative/coki-ries/tree/main>
[contact]: <mailto:coki@curtin.edu.au>
[install]: <docs/installation.md>
[configure]: <docs/configuration.md>
[usage]: <docs/usage.md>
[roadmap]: <docs/roadmap.md>
[methods]: <docs/methods.md>
[workflow]: <docs/workflow.md>
