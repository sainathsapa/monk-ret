from crewai import Agent, Task
import subprocess

from langchain.tools import tool

@tool
def generate_insights(file_path: str) -> str:
    """GEN INSIGHTS

    Args:
        file_path (str): _description_

    Returns:
        str: STRING of FUNC
    """
    try:
        result = subprocess.run(
            ["python", "gen_insights_force.py", file_path],
            capture_output=True, text=True
        )
        return result.stdout if result.stdout else "No insights generated."
    except Exception as e:
        return f"Error generating insights: {e}"

