from driftbench.spec.core import run_all as run_yaml_all
import driftbench.spec.types  # ensure handlers registered

import argparse

def main():
    parser = argparse.ArgumentParser("driftbench")
    sub = parser.add_subparsers(dest="cmd")

    # existing subcommands...
    y = sub.add_parser("run-yaml", help="Run a DriftSpec YAML")
    y.add_argument("spec", help="Path to YAML spec")

    t = sub.add_parser("trace-to-spec", help="Generate a DriftSpec YAML from a trace summary")
    t.add_argument("trace", help="Path to trace summary (CSV or JSON)")
    t.add_argument("output", help="Path to output DriftSpec YAML")
    t.add_argument("--trace-type", choices=["data", "workload"], help="Override trace_type inference")
    t.add_argument("--mapping", help="Optional mapping JSON for trace column selection")

    args = parser.parse_args()
    if args.cmd == "run-yaml":
        run_yaml_all(args.spec)
    elif args.cmd == "trace-to-spec":
        from driftbench.spec.trace_spec import trace_to_spec
        trace_to_spec(args.trace, args.output, trace_type=args.trace_type, mapping_path=args.mapping)
    else:
        parser.print_help()

if __name__ == "__main__":
    main()
