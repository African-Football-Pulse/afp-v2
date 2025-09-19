# Collect / Merge / Scan – Översikt

Detta dokument beskriver hur **Collect-, Merge- och Scan-flödena** i systemet hänger ihop, vilka dataobjekt de producerar, och hur dessa används vidare. Syftet är att skapa en begriplig översikt som kan användas för dokumentation, felsökning och planering av nya funktioner.

---

## 1. Collect – Insamling av rådata

Collect-flödena ansvarar för att hämta in rådata från **SoccerData API**, **Wikipedia**, samt egna whitelist-filer. Dessa flöden producerar grundläggande dataobjekt som sedan används i Merge och Scan.

- `collect_leagues` → skapar **Leagues.json** med en lista över ligor.
- `collect_all_seasons` → skapar **Seasons files** med aktiva och historiska säsonger för respektive liga.
- `collect_extract_fullseason` och `collect_extract_weekly` → skapar **Match files**, som innehåller detaljerade matcher och events.
- `collect_player_history` → skapar **Player history** från matchernas events.
- `collect_player_stats` → skapar **Player stats** (mål, kort, assist, byten).
- `collect_players_africa` → skapar **Players Africa** (identifierade afrikanska spelare).
- `collect_teams` → skapar **Teams** (metadata per lag).
- `collect_transfers` → skapar **Transfers** (per lag och liga).
- `collect_player_profiles` → skapar **Profiles** (Wikipedia + API-baserade spelardata).

**Nyckelobjekt i Collect:**  
Den centrala produkten är **Match files**. Dessa används direkt för att generera player history, player stats, players africa och teams. Utan matchfiler saknas underlag för nästan alla efterföljande steg.

---

## 2. Merge – Konsolidering till masterfiler

Merge-flödena konsoliderar Collect-data till centrala masterfiler som är grunden för vidare analys, publicering och presentation.

- `merge_players_africa` → skapar **Master players** (players_africa_master.json) och basporträtt per spelare.
- `merge_africa_player_history` → skapar **Merged history** (players_africa_history.json) och **Missing history** (spelare utan historik).
- `merge_transfers` → skapar **Transfers draft** (players_africa_master_draft.json) som innehåller uppdaterade klubbar och loan-status. Draften granskas innan den ersätter master.
- `generate_club_index` → skapar **Club index** (spelare grupperade per klubb).

---

## 3. Scan – Kvalitetskontroll och komplettering

Scan-flödena används för att hitta luckor och komplettera masterfilen.

- `scan_missing_players` → tar **Missing history** som input och letar efter potentiella matchningar i samtliga **Match files**. Resultatet sparas i **Scan results**, som visar vilka spelare som möjligen kan fyllas på med historik.

---

## 4. Samlad pipeline

Diagrammet nedan illustrerar hur Collect producerar rådata, Merge konsoliderar dessa till masterfiler, och Scan använder dessa för att fylla luckor.

```mermaid
flowchart TD

  %% Data objects
  L[(Leagues.json)]
  S[(Seasons files)]
  M[(Match files)]
  H[(Player history)]
  PS[(Player stats)]
  PA[(Players Africa)]
  T[(Teams)]
  TR[(Transfers)]
  P[(Profiles)]
  MASTER[(Master players)]
  MH[(Merged history)]
  DRAFT[(Transfers draft)]
  CI[(Club index)]
  MISS[(Missing history)]
  SCAN[(Scan results)]

  %% Collect
  CL[collect_leagues] --> L
  L --> CS[collect_all_seasons]
  CS --> S

  CE1[collect_extract_fullseason] --> M
  CE2[collect_extract_weekly] --> M

  M --> CH[collect_player_history]
  M --> CP[collect_player_stats]
  M --> CA[collect_players_africa]
  M --> CT[collect_teams]

  CH --> H
  CP --> PS
  CA --> PA
  CT --> T

  CTR[collect_transfers] --> TR
  CPROF[collect_player_profiles] --> P

  %% Merge
  M1[merge_players_africa] --> MASTER
  M2[merge_africa_player_history] --> MH
  M2 --> MISS
  M3[merge_transfers] --> DRAFT
  M4[generate_club_index] --> CI

  %% Scan
  S1[scan_missing_players] --> SCAN

  %% Connections
  D1 --> CS
  D2 --> CF
  D3 --> CH
  D3 --> CP
  D3 --> CA
  D3 --> CT
  D8 --> CTR
  D5 --> M2
  D7 --> M1
  D9 --> M3
  D10 --> M4
  D11 --> S1
