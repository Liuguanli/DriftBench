"""Step-by-step ycsb companion; implementation shared with its notebook."""

if __package__:
    from ._walkthrough import Walkthrough, main
else:
    from _walkthrough import Walkthrough, main


def create(*, output_dir, package_target="installed", **options):
    return Walkthrough("ycsb", output_dir=output_dir, package_target=package_target, **options)


if __name__ == "__main__":
    raise SystemExit(main("ycsb"))
