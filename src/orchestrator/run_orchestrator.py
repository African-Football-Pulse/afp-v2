import subprocess, time, json

RG = "afp-rg"
jobs = [
    "afp-collect-job",
    "afp-warehouse-job",
    "afp-produce-job",
    "afp-assemble-job",
    "afp-render-job",
    "afp-mix-job",
    "afp-publish-job",
]

def run(cmd):
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(f"Command failed: {cmd}\n{result.stderr}")
    return result.stdout

def wait_for_completion(job_name):
    print(f"▶️ Waiting for {job_name} to finish...")
    while True:
        execs = json.loads(run(
            f"az containerapp job execution list --name {job_name} --resource-group {RG} --output json"
        ))
        if not execs:
            time.sleep(10)
            continue
        latest = sorted(execs, key=lambda x: x["properties"]["startTime"], reverse=True)[0]
        state = latest["properties"]["status"]
        if state in ("Succeeded", "Failed"):
            print(f"✅ {job_name} finished with status: {state}")
            if state == "Failed":
                raise RuntimeError(f"{job_name} failed")
            break
        time.sleep(20)

for job in jobs:
    print(f"🚀 Starting {job}")
    run(f"az containerapp job start --name {job} --resource-group {RG}")
    wait_for_completion(job)
