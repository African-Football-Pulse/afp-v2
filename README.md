# AFP Jobs – Struktur & Samband

Det här repot innehåller definitioner för tre Azure Container Apps Jobs som kör olika delar av **African Football Pulse**-pipeline.  
För att undvika förvirring används en konsekvent namngivning genom hela kedjan: **Azure-resurs → YAML-fil → JOB_TYPE → Python-modul**.

---

## Översikt

| Azure Job (resursnamn) | YAML-fil                          | JOB_TYPE (env) | Python-modul som körs                          |
|-------------------------|-----------------------------------|----------------|------------------------------------------------|
| `afp-collect-job`       | `.github/workflows/collect-job.yaml`   | `collect`      | `src.collectors.rss_multi`                     |
| `afp-produce-job`       | `.github/workflows/produce-job.yaml`   | `produce`      | `src.sections.s_news_top3_guardian`            |
| `afp-assemble-job`      | `.github/workflows/assemble-job.yaml`  | `assemble`     | `src.assembler.main`                           |

---

## Flöde

```mermaid
flowchart LR
    subgraph GitHub
        A[Repo Secrets\nBLOB_CONTAINER_SAS_URL] --> B[deploy.yml\nworkflow]
    end

    subgraph Azure
        B --> C1[afp-collect-job\nYAML + JOB_TYPE=collect]
        B --> C2[afp-produce-job\nYAML + JOB_TYPE=produce]
        B --> C3[afp-assemble-job\nYAML + JOB_TYPE=assemble]

        C1 --> D[job_entrypoint.py]
        C2 --> D
        C3 --> D
    end

    D --> M1[src.collectors.rss_multi]
    D --> M2[src.sections.s_news_top3_guardian]
    D --> M3[src.assembler.main]

    style A fill:#f6f8fa,stroke:#0366d6,stroke-width:2px
    style B fill:#e6f7ff,stroke:#1890ff,stroke-width:2px
    style C1 fill:#fffbe6,stroke:#faad14,stroke-width:1px
    style C2 fill:#fffbe6,stroke:#faad14,stroke-width:1px
    style C3 fill:#fffbe6,stroke:#faad14,stroke-width:1px
    style D fill:#f9f0ff,stroke:#9254de,stroke-width:2px
    style M1 fill:#f6ffed,stroke:#52c41a
    style M2 fill:#f6ffed,stroke:#52c41a
    style M3 fill:#f6ffed,stroke:#52c41a
