import argparse
import itertools
import pathlib

import yaml


def rewrite_artifact_path(
    metadata_file: pathlib.Path,
    artifact_path_key: str,
    artifact_path: pathlib.Path,
    debug: bool,
):
    with open(metadata_file, "r") as f:
        y = yaml.safe_load(f)
        y[artifact_path_key] = f"file://{artifact_path}"

    with open(metadata_file, "w") as f:
        print(
            yaml.dump(y, default_flow_style=False, sort_keys=False)
        ) if debug else None
        yaml.dump(y, f, default_flow_style=False, sort_keys=False)


def fix_artifact_paths(path: str, debug: bool = False):
    absolute_path = pathlib.Path(path).resolve()
    absolute_path_trash = absolute_path / ".trash"

    for experiment_folder in itertools.chain(
        # experiments in "mlruns/"
        filter(
            lambda el: not el.is_file() and el.name != "trash" and el.name != "models",
            absolute_path.iterdir(),
        ),
        # experiments in "mlruns/.trash/"
        filter(lambda el: not el.is_file(), absolute_path_trash.iterdir()),
    ):
        print(f"=> experiment directory: {experiment_folder}")

        metadata_file = experiment_folder / "meta.yaml"
        print(f"==> metadata file: {metadata_file}")

        # fix experiment metadata
        if metadata_file.exists():
            rewrite_artifact_path(
                metadata_file=metadata_file,
                artifact_path_key="artifact_location",
                artifact_path=experiment_folder,
                debug=debug,
            )

            for run_folder in filter(
                lambda el: not el.is_file() and el.name != "tags",
                experiment_folder.iterdir(),
            ):
                print(f"==> run directory: {run_folder}")

                metadata_file = run_folder / "meta.yaml"
                print(f"===> metadata file: {metadata_file}")

                # fix run metadata
                if metadata_file.exists():
                    rewrite_artifact_path(
                        metadata_file=metadata_file,
                        artifact_path_key="artifact_uri",
                        artifact_path=run_folder / "artifacts",
                        debug=debug,
                    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("path", help="Path to root of `mlruns` folder.")

    args = parser.parse_args()

    fix_artifact_paths(args.path)


if __name__ == "__main__":
    main()
