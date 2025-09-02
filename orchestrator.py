from agents.agent_upload import upload_task, upload_and_visualize
from agents.agent_insights import insight_task, generate_insights
from agents.agent_deploy import deploy_task, deploy_dashboard
# from crewai import Crew

# from 

def main():
    # crew = Crew(
    #     agents=[upload_task.agent, insight_task.agent, deploy_task.agent],
    #     tasks=[upload_task, insight_task, deploy_task],
    #     verbose=True
    # )

    # # file_path = "sample.csv"  # Replace with actual file
    # # print("🚀 Running multi-agent workflow...")
    # results = crew.kickoff()
    # print("✅ Results:", results)
    print("🚀 Running INSERT")
    upload_and_visualize('')
    print("🚀 DEPLOY workflow...")
    deploy_dashboard()
    

if __name__ == "__main__":
    main()

