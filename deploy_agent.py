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
    # Load configuration from ae_config.config
    dir_path = os.path.dirname(os.path.realpath(__file__))
    config_file = os.path.join(dir_path, "ae_config.config")
    ae_cfg = {}
    if os.path.exists(config_file):
        with open(config_file, "r") as f:
            for line in f:
                if "=" in line and not line.strip().startswith("#"):
                    key, val = line.split("=", 1)
                    ae_cfg[key.strip()] = val.strip().strip('"').strip("'")

    project_id = ae_cfg.get("PROJECT_ID", PROJECT_ID)
    location = ae_cfg.get("LOCATION", LOCATION)
    staging_bucket = ae_cfg.get("STAGING_BUCKET", "gs://marketresearch-agents")
    sa_email = ae_cfg.get("SA_EMAIL")
    old_engine_id = ae_cfg.get("ENGINE_ID")
    
    # Semicolon-separated strings to Python lists
    requirements_str = ae_cfg.get("REQUIREMENTS", "google-cloud-aiplatform[agent_engines,adk];google-cloud-storage;pyyaml")
    requirements = [r.strip() for r in requirements_str.split(";") if r.strip()]
    
    extra_packages_str = ae_cfg.get("EXTRA_PACKAGES", "market_team.py;values.yaml")
    extra_packages = [p.strip() for p in extra_packages_str.split(";") if p.strip()]

    print(f"🚀 Initializing Vertex AI deployment in project: {project_id}, location: {location}")
    if not sa_email:
        print("❌ Error: Could not load SA_EMAIL from ae_config.config. Deployment aborted.")
        return

    vertexai.init(project=project_id, location=location, staging_bucket=staging_bucket)

    print("📦 Packaging and Deploying to Vertex AI Agent Engine...")
    print("⏳ This may take a few minutes as it zips the code and builds the container...")
    
    remote_app = agent_engines.create(
        agent_engine=app,
        requirements=requirements,
        extra_packages=extra_packages,
        display_name="Market-Team-Agent-App",
        description="Daily market sweep agent acting as CIO.",
        service_account=sa_email
    )

    print(f"✅ Deployment successful!")
    print(f"🔗 Resource Name: {remote_app.resource_name}")
    
    # Extract the ID from the end of the resource name
    new_engine_id = remote_app.resource_name.split("/")[-1]
    update_config_file(new_engine_id)
    print(f"📝 Automatically wrote Engine ID {new_engine_id} to ae_config.config")

    # --- AUTO CLEANUP OF OLD ENGINE ---
    if old_engine_id and old_engine_id != new_engine_id:
        old_resource_name = f"projects/{project_id}/locations/{location}/reasoningEngines/{old_engine_id}"
        print(f"🧹 Detected previous deployment: {old_engine_id}")
        print(f"🗑️ Decommissioning old engine to stay clean...")
        try:
            agent_engines.delete(resource_name=old_resource_name, force=True)
            print(f"✅ Old engine {old_engine_id} successfully deleted.")
        except Exception as e:
            print(f"⚠️ Warning: Old engine {old_engine_id} was not deleted automatically (it may already be gone or permissions were lacking). Error: {str(e)}")

if __name__ == "__main__":
    deploy()