"""Step-by-step pgbench companion; implementation shared with its notebook."""

if __package__:
    from ._walkthrough import Walkthrough, main
else:
    from _walkthrough import Walkthrough, main


def create(*, output_dir, package_target="installed", **options):
    return Walkthrough("pgbench", output_dir=output_dir, package_target=package_target, **options)


if __name__ == "__main__":
    raise SystemExit(main("pgbench"))
