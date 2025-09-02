from agents.agent_upload import upload_task
from agents.agent_insights import insight_task
from agents.agent_deploy import deploy_task
from crewai import Crew

def main():
    crew = Crew(
        agents=[upload_task.agent, insight_task.agent, deploy_task.agent],
        tasks=[upload_task, insight_task, deploy_task],
        verbose=True
    )

    file_path = "sample.csv"  # Replace with actual file
    print("🚀 Running multi-agent workflow...")
    results = crew.kickoff(inputs={"file_path": file_path})
    print("✅ Results:", results)

if __name__ == "__main__":
    main()

