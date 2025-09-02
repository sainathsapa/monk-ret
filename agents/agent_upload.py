from crewai import Agent, Task
import subprocess
from langchain_community.llms import Ollama
llm = Ollama(model="ollama/mistral")   # Local, no API key needed

def upload_and_visualize(file_path: str) -> str:
    try:
        result = subprocess.run(
            ["python", "csv_insertion_batch.py", file_path],
            capture_output=True, text=True
        )
        return result.stdout if result.stdout else "Visualization completed."
    except Exception as e:
        return f"Error in upload & visualization: {e}"


upload_agent = Agent(
    role="Data Upload & Visualization Agent",
    goal="Upload CSV datasets and generate dashboards.",
    backstory="This agent takes datasets from users, processes them, and generates interactive dashboards.",
    llm=llm,  # <<<< Force local LLM
    verbose=True
)


upload_task = Task(
    description="Upload dataset and generate visualizations.",
    agent=upload_agent,
    expected_output="Processed data and dashboard ready.",
    function=upload_and_visualize
)
