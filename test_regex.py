import os
import re

def update_config_file(engine_id):
    dir_path = os.path.dirname(os.path.realpath(__file__))
    config_file = os.path.join(dir_path, "ae_config.config")
    
    print(f"Looking for config file at: {config_file}")
    if os.path.exists(config_file):
        print("Found file. Reading...")
        with open(config_file, "r") as f:
            content = f.read()
        
        print("Original content:")
        print(content)
        
        content, count = re.subn(r'^ENGINE_ID=.*$', f'ENGINE_ID="{engine_id}"', content, flags=re.MULTILINE)
        print(f"Replaced {count} instances.")
        
        if count == 0:
            print("REGEX FAILED. Did not find a match.")
        
        print("\nNew content:")
        print(content)

update_config_file("99999999999")
