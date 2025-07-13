import subprocess
import time
import os
import requests

def wait_for_server(url="http://localhost:5000/produse", timeout=3, retries=20):
    for i in range(retries):
        try:
            r = requests.get(url, timeout=timeout)
            if r.status_code in (200, 404):  # Dacă răspunde, e ok
                print("[INFO] Serverul Flask e disponibil.")
                return True
        except requests.exceptions.RequestException:
            print(f"[INFO] Aștept Flask... ({i+1}/{retries})")
            time.sleep(1)
    print("[FATAL] Flask nu a pornit.")
    return False

# Calea către python din .venv
python_path = os.path.join(".venv", "Scripts", "python.exe")

# Environment corect
env = os.environ.copy()
env["PATH"] = os.path.join(os.getcwd(), ".venv", "Scripts") + ";" + env["PATH"]

# Pornește Flask
flask_process = subprocess.Popen(
    [python_path, "app.py"],
    env=env,
    stdout=None,
    stderr=None
)

# Așteptăm Flask să fie online
if not wait_for_server():
    print("[ERROR] Nu putem porni Schedulerul. Flask nu răspunde.")
    flask_process.terminate()
    flask_process.wait()
    exit(1)

# Pornește Scheduler după ce serverul e OK
scheduler_process = subprocess.Popen(
    [python_path, os.path.join("tutorial", "spiders", "Scheduler.py")],
    env=env,
    stdout=None,
    stderr=None
)

print("[INFO] Flask și Scheduler sunt pornite.")

try:
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    print("\n[INFO] Oprire detectată. Închid procesele...")
    flask_process.terminate()
    scheduler_process.terminate()
    flask_process.wait()
    scheduler_process.wait()
    print("[INFO] Toate procesele au fost oprite.")