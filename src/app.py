import yaml
from datagraphs.client import Client as DatagraphsClient

def main(config_key)->None:
    with open('.app.config.yml', 'r') as config_file:
        configs = yaml.safe_load(config_file)
        if config_key in configs:
            config = configs[config_key]
            dg_client = DatagraphsClient(
                project_name=config['project_name'], 
                api_key=config['api_key'], 
                client_id=config['client_id'], 
                client_secret=config['client_secret']
            )
            print(dg_client.get('ProductActionType'))
            print('Done.')
        else:
            print("Unrecognised config key - please select from: "+", ".join(config.keys()))

if __name__ == "__main__":
    main('dev-blue')