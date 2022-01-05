import toml

# load configuration variables from .toml file
with open('config/config.toml', 'r') as f:
    config = toml.load(f)