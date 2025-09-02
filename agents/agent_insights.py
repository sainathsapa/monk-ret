from crewai import Agent, Task
import subprocess

from langchain_community.llms import Ollama
llm = Ollama(model="ollama/mistral")   # Local, no API key needed


def generate_insights(file_path: str) -> str:
    try:
        result = subprocess.run(
            ["python", "gen_insights_force.py", file_path],
            capture_output=True, text=True
        )
        return result.stdout if result.stdout else "No insights generated."
    except Exception as e:
        return f"Error generating insights: {e}"

insight_agent = Agent(
    role="Insight Generation Agent",
    goal="Analyze data and generate AI-driven insights.",
    backstory="This agent examines datasets and produces key insights using AI models.",
    tools=[],
    llm=llm,
    verbose=True
)

insight_task = Task(
    description="Analyze dataset and produce insights.",
    agent=insight_agent,
    expected_output="AI-driven insights generated from dataset.",
    function=generate_insights
)
