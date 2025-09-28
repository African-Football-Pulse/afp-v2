import os
import subprocess
import time
import sys

JOBS_SEQUENCE = [
    "afp-collect-job",
    "afp-warehouse-player-stats-job",
    "afp-warehouse-goals-assists-job",
    "afp-warehouse-cards-job",
    "afp-warehouse-toplists-job",
    "afp-warehouse-performance-job",
    "afp-warehouse-matches-weekly-job",
    "afp-produce-job",
    "afp-assemble-job",
    "afp-render-job",
    "afp-mix-job",
    "afp-publish-job",
]

RESOURCE_GROUP = os.getenv("AZURE_RESOURCE_GROUP", "afp-rg")

def run_cmd(cmd):
    """Kör CLI-kommandon och returnera stdout"""
    print(f"[Orchestrator] Running: {cmd}", flush=True)
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"❌ Error: {result.stderr}", flush=True)
        sys.exit(1)
    return result.stdout.strip()

def wait_for_job(job_name):
    """Starta ett jobb och vänta tills det är klart"""
    # Starta job
    exec_id = run_cmd(
        f"az containerapp job start --name {job_name} --resource-group {RESOURCE_GROUP} --query name -o tsv"
    )
    print(f"[Orchestrator] Started {job_name} (execution {exec_id})")

    # Poll:a tills job klart
    while True:
        status = run_cmd(
            f"az containerapp job execution show --name {job_name} "
            f"--resource-group {RESOURCE_GROUP} --execution {exec_id} --query properties.status -o tsv"
        )
        print(f"[Orchestrator] {job_name} status = {status}")
        if status in ("Succeeded", "Failed"):
            return status
        time.sleep(30)  # vänta 30 sek innan nästa poll

def main():
    for job in JOBS_SEQUENCE:
        print(f"\n=== Kör {job} ===", flush=True)
        status = wait_for_job(job)
        if status != "Succeeded":
            print(f"❌ Job {job} misslyckades, avbryter orchestrator", flush=True)
            sys.exit(1)
    print("🎉 Alla jobb kördes klart i sekvens!", flush=True)

if __name__ == "__main__":
    main()
