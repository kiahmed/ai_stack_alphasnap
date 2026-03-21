import os
import vertexai
from vertexai import agent_engines

# Load settings from ae_config.config
def load_config():
    dir_path = os.path.dirname(os.path.realpath(__file__))
    config_file = os.path.join(dir_path, "ae_config.config")
    cfg = {}
    if os.path.exists(config_file):
        with open(config_file, "r") as f:
            for line in f:
                if "=" in line and not line.strip().startswith("#"):
                    key, val = line.split("=", 1)
                    cfg[key.strip()] = val.strip().strip('"').strip("'")
    return cfg

def cleanup_engine(engine_id=None):
    cfg = load_config()
    project_id = cfg.get("PROJECT_ID")
    location = cfg.get("LOCATION")
    
    # If no ID provided, try to find an old one or use the one from config
    target_id = engine_id or cfg.get("ENGINE_ID")
    
    if not target_id or not project_id or not location:
        print("❌ Error: Missing configuration (PROJECT_ID, LOCATION, or ENGINE_ID).")
        return

    vertexai.init(project=project_id, location=location)
    
    resource_name = f"projects/{project_id}/locations/{location}/reasoningEngines/{target_id}"
    
    print(f"🧹 Attempting to delete Reasoning Engine: {target_id}")
    print(f"🔗 Resource: {resource_name}")
    
    try:
        # Testing the fixed 'resource_name' argument with force=True
        agent_engines.delete(resource_name=resource_name, force=True)
        print(f"✅ Successfully triggered deletion for {target_id}.")
    except Exception as e:
        print(f"❌ Deletion failed: {str(e)}")

if __name__ == "__main__":
    import sys
    # Usage: python3 verify_delete.py [OPTIONAL_ENGINE_ID]
    passed_id = sys.argv[1] if len(sys.argv) > 1 else None
    cleanup_engine(passed_id)
