import argparse
from .. import databundle_from_yaml


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "yaml",
        help="YAML configuration file describing the data sources and target "
             "bundle format."
    )
    return parser.parse_args()


def main():
    args = parse_args()
    databundle_from_yaml(args.yaml) 
