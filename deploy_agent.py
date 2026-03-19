import os
import yaml
import re
import vertexai
from vertexai import agent_engines # <-- UPDATED: Using native agent_engines
from market_team import app, PROJECT_ID, LOCATION

def update_config_file(engine_id):
    dir_path = os.path.dirname(os.path.realpath(__file__))
    config_file = os.path.join(dir_path, "ae_config.config")
    
    if os.path.exists(config_file):
        with open(config_file, "r") as f:
            lines = f.readlines()
            
        with open(config_file, "w") as f:
            for line in lines:
                if line.startswith("ENGINE_ID="):
                    f.write(f'ENGINE_ID="{engine_id}"\n')
                else:
                    f.write(line)

def deploy():
    print(f"🚀 Initializing Vertex AI deployment in project: {PROJECT_ID}, location: {LOCATION}")
    vertexai.init(project=PROJECT_ID, location=LOCATION, staging_bucket="gs://marketresearch-agents")

    # <-- UPDATED: The exact dependencies Vertex AI requires to serve an AdkApp
    requirements = [
        "google-cloud-aiplatform[agent_engines,adk]", 
        "google-cloud-storage",
        "pyyaml"
    ]

    print("📦 Packaging and Deploying to Vertex AI Agent Engine...")
    print("⏳ This may take a few minutes as it zips the code and builds the container...")
    
    # <-- UPDATED: Using agent_engines.create() instead of ReasoningEngine
    sa_email = None
    dir_path = os.path.dirname(os.path.realpath(__file__))
    config_file = os.path.join(dir_path, "ae_config.config")
    if os.path.exists(config_file):
        with open(config_file, "r") as f:
            for line in f:
                if line.startswith("SA_EMAIL="):
                    sa_email = line.split('=', 1)[1].strip().strip('"').strip("'")

    if not sa_email:
        print("❌ Error: Could not load SA_EMAIL from ae_config.config. Deployment aborted.")
        return

    remote_app = agent_engines.create(
        agent_engine=app,
        requirements=requirements,
        extra_packages=[
            "market_team.py", 
            "values.yaml"
        ],
        display_name="Market-Team-Agent-App",
        description="Daily market sweep agent acting as CIO.",
        service_account=sa_email
    )

    print(f"✅ Deployment successful!")
    print(f"🔗 Resource Name: {remote_app.resource_name}")
    
    # Extract the ID from the end of the resource name
    engine_id = remote_app.resource_name.split("/")[-1]
    update_config_file(engine_id)
    print(f"📝 Automatically wrote Engine ID {engine_id} to ae_config.config")

if __name__ == "__main__":
    deploy()