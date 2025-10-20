from driftbench.spec.core import run_all as run_yaml_all
import driftbench.spec.types  # ensure handlers registered

import argparse

def main():
    parser = argparse.ArgumentParser("driftbench")
    sub = parser.add_subparsers(dest="cmd")

    # existing subcommands...
    y = sub.add_parser("run-yaml", help="Run a DriftSpec YAML")
    y.add_argument("spec", help="Path to YAML spec")

    args = parser.parse_args()
    if args.cmd == "run-yaml":
        run_yaml_all(args.spec)
    else:
        parser.print_help()

if __name__ == "__main__":
    main()
