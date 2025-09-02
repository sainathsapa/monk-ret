from crewai import Agent, Task
import subprocess
# from langchain_community.llms import Ollama
# llm = Ollama(model="ollama/mistral")   # Local, no API key needed
from langchain.tools import tool

@tool
def upload_and_visualize(file_path: str) -> str:
    """Upload the given CSV file to the database and create a simple visualization."""

    try:
        result = subprocess.run(
            ["python", "csv_insertion_batch.py", file_path],
            capture_output=True, text=True
        )
        return result.stdout if result.stdout else "Visualization completed."
    except Exception as e:
        return f"Error in upload & visualization: {e}"


