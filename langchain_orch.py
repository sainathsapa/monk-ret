import argparse
from langchain_ollama import ChatOllama
from langchain.agents import initialize_agent, AgentType
from agents.agent_upload import upload_and_visualize
from agents.agent_insights import generate_insights
from agents.agent_deploy import deploy_dashboard

# 1. Setup the local Mistral model via Ollama
llm = ChatOllama(model="mistral")

# 2. Define 3 separate agents with tool and parsing fixes

# Agent 1: Uploader
upload_tool = [upload_and_visualize]
uploader = initialize_agent(
    tools=upload_tool,
    llm=llm,
    agent=AgentType.ZERO_SHOT_REACT_DESCRIPTION,
    verbose=True,
    handle_parsing_errors=True  # ✅ prevent crash from LLM formatting mistakes
)

# Agent 2: Insight generator
insight_tool = [generate_insights]
insighter = initialize_agent(
    tools=insight_tool,
    llm=llm,
    agent=AgentType.ZERO_SHOT_REACT_DESCRIPTION,
    verbose=True,
    handle_parsing_errors=True
)

# Agent 3: Deployer
deployer_tool = [deploy_dashboard]
deployer = initialize_agent(
    tools=deployer_tool,
    llm=llm,
    agent=AgentType.ZERO_SHOT_REACT_DESCRIPTION,
    verbose=True,
    handle_parsing_errors=True
)

# 3. Define a simple workflow between the agents
def multi_agent_workflow(csv_file_path: str):
    print("\n🔍 Upload Phase:")
    uploader_RES = uploader.invoke(f"Use the upload_and_visualize tool to upload this CSV file: {csv_file_path}")
    print(f"Uploader Result: {uploader_RES['output']}")

    print("\n🧠 Insight Phase:")
    insighter_RES = insighter.invoke(f"Use the generate_insights tool to extract insights from: {csv_file_path}")
    print(f"Insights Result: {insighter_RES['output']}")

    print("\n🛠️ Deploy Phase:")
    deployer_RES = deployer.invoke(f"Use deploy_dashboard to deploy results from: {csv_file_path}")
    print(f"Deployer Result: {deployer_RES['output']}")

    return deployer_RES['output']

# 4. Main CLI entrypoint
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run multi-agent orchestration pipeline on a CSV file")
    parser.add_argument("file_path", help="Path to the CSV file to process")
    args = parser.parse_args()

    final_result = multi_agent_workflow(args.file_path)
    print("\n✅ Final Result:\n", final_result)
