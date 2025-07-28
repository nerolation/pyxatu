import click
import importlib.resources as resources
from pathlib import Path
import shutil
from .pyxatu import PyXatu

@click.group()
def cli():
    """Main command group for PyXatu CLI."""
    pass

@click.command()
def setup():
    """CLI command to set up the default configuration in the user's home directory."""
    home = Path.home()
    user_config_path = home / '.pyxatu_config.json'

    try:
        print(f"Looking for config.json in the installed package...")
        default_config_file = resources.files('pyxatu') / 'config.json'
        print(f"Config file found at: {default_config_file}")

        if not user_config_path.exists():
            shutil.copy(default_config_file, user_config_path)
            print(f"Default configuration copied to {user_config_path}.")
            print("Enter your credentials to use PyXatu.")
        else:
            print(f"User configuration already exists at {user_config_path}.")
    except Exception as e:
        print(f"Error during configuration setup: {e}")

@click.command()
@click.argument('query')
@click.option('--config', type=click.Path(exists=True), help='Path to the configuration file', default=None)
@click.option('--columns', multiple=True, help='List of column names')
def query(query, config, columns):
    """Run queries against the Xatu API."""
    xatu = PyXatu(config_path=config)
    result = xatu.request_query(query, columns=columns)
    
    if result is not None:
        print(result)
    else:
        print("Query failed")

# Add both commands to the main CLI group
cli.add_command(setup)
cli.add_command(query)

if __name__ == "__main__":
    cli()
