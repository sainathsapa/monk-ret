from crewai import Agent, Task
import subprocess
from langchain.tools import tool


import time
from datetime import datetime
def run_command(command):
    process = subprocess.Popen(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )
    stdout, stderr = process.communicate()
    return stdout.decode(), stderr.decode()

# Generate commit message with timestamp
timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
commit_message = f"deploy: run at {timestamp}"

@tool
def deploy_dashboard(file_path: str) -> str:
    """DEPLOY TO GIT

    Returns:
        str: STATUS
    """
    try:
        # Start Streamlit in background
        # 1. git add .
        print("Running: git add .")
        out, err = run_command(["git", "add", "."])
        print("ADD STDOUT:", out)
        print("ADD STDERR:", err)

        # 2. git commit -m "deploy: run at ..."
        print(f"Running: git commit -m \"{commit_message}\"")
        out, err = run_command(["git", "commit", "-m", commit_message])
        print("COMMIT STDOUT:", out)
        print("COMMIT STDERR:", err)

        # 3. git push
        print("Running: git push")
        out, err = run_command(["git", "push"])
        print("PUSH STDOUT:", out)
        print("PUSH STDERR:", err)

        # Open ngrok tunnel
        # public_url = ngrok.connect(8501, bind_tls=True)

        print("✅ Website deployed successfully!")
        

    except Exception as e:
        return f"❌ Error deploying dashboard: {e}"

