import pathlib
from functools import partial, update_wrapper, wraps

import click
import prov.model

from mlflow2prov import __version__
from mlflow2prov.config.config import Config
from mlflow2prov.dependencies import Dependencies
from mlflow2prov.log import create_logger
from mlflow2prov.prov.operations import (
    SerializationFormat,
    StatisticsFormat,
    StatisticsResolution,
)
from mlflow2prov.service_layer import services


def enable_logging(ctx: click.Context, _, enable: bool):
    """Callback that enables logging."""

    if enable:
        create_logger()


def load_config(ctx: click.Context, _, filepath: str):
    """Callback that loads a configuration file."""

    if not filepath:
        return

    config = Config.read(filepath)
    ok, err = config.validate()

    if not ok:
        ctx.fail(f"Validation failed: {err}")

    context = ctx.command.make_context(
        ctx.command.name, args=config.parse(), parent=ctx
    )
    ctx.command.invoke(context)
    ctx.exit()


def validate_config(ctx: click.Context, _, filepath: str):
    """Callback that validates a configuration file."""

    if not filepath:
        return

    config = Config.read(filepath)
    ok, err = config.validate()

    if not ok:
        ctx.fail(f"Validation failed: {err}")

    click.echo("Validation successful")
    ctx.exit()


def processor(func, wrapped=None):
    """Decorator that turns a function into a processor.

    A processor is a function that takes a stream of values, applies an operation to each value and returns a new stream of values. A processor therefore transforms a stream of values into a new stream of values.
    """

    @wraps(wrapped or func)
    def new_func(*args, **kwargs):
        def processor(stream):
            return func(stream, *args, **kwargs)

        return processor

    return update_wrapper(new_func, func)


def generator(func):
    """Decorator that turns a function into a generator.

    A generator is a special case of a processor. A generator is a processor that doesn't apply any operation to the values but adds new values to the stream.
    """

    @partial(processor, wrapped=func)
    def new_func(stream, *args, **kwargs):
        yield from stream
        yield from func(*args, **kwargs)

    return update_wrapper(new_func, func)


@click.group(
    chain=True,
    invoke_without_command=False,
)
@click.version_option(
    version=__version__,
    prog_name="mlflow2prov",
)
@click.option(
    "--verbose",
    "-v",
    is_flag=True,
    default=False,
    expose_value=False,
    callback=enable_logging,
    help="Enable logging to stdout.",
)
@click.option(
    "--config",
    type=click.Path(exists=True, dir_okay=False),
    expose_value=False,
    callback=load_config,
    help="Read configuration from file.",
)
@click.option(
    "--validate",
    type=click.Path(exists=True, dir_okay=False),
    expose_value=False,
    callback=validate_config,
    help="Validate configuration file and exit.",
)
@click.pass_context
def cli(ctx: click.Context):
    """Extract provenance information from ML experiment projects that use Git repositories and MLflow tracking."""

    ctx.obj = Dependencies()


@cli.result_callback()
def process_commands(processors):
    """Execute the chain of commands.

    This function is called after all subcommands have been chained together. It executes the chain of commands by piping the output of one command into the input of the next command. Subcommands can be processors that transform the stream of values or generators that add new values to the stream.
    """

    # Start with empty iterable
    it = ()

    # Pipe it through all stream processors
    for processor in processors:
        it = processor(it)

    # Evaluate stream and throw away items
    for _ in it:
        pass


@cli.command("extract")
@click.option(
    "--repository_path",
    "repository_path",
    type=click.Path(exists=True, dir_okay=True, path_type=pathlib.Path),
    required=True,
    help="Git repository path.",
)
@click.option(
    "--mlflow_url",
    "mlflow_url",
    type=str,
    required=True,
    help="MLflow tracking server URL.",
)
@click.pass_obj
@generator
def extract(deps: Dependencies, repository_path: pathlib.Path, mlflow_url: str):
    """
    Extract a provenance document from an ML experiment project based on its Git repository and MLflow tracking server.
    """

    services.fetch_git_from_path(
        path=repository_path, uow=deps.uow, git_fetcher=deps.git_fetcher
    )
    services.fetch_mlflow(
        url=mlflow_url, uow=deps.uow, mlflow_fetcher=deps.mlflow_fetcher
    )

    doc = services.compile_graph(
        uow=deps.uow, locations=[str(repository_path), mlflow_url]
    )

    doc = services.transform(document=doc)

    yield doc


@cli.command("load")
@click.option(
    "--input",
    "filenames",
    default=["-"],
    multiple=True,
    type=click.Path(dir_okay=False),
    help="Provenance file path (specify '-' to read from <stdin>).",
)
@generator
def load(filenames: list[str]):
    """
    Load provenance documents from one or more file(s).
    """

    for filename in filenames:
        doc = services.read(filename=filename)
        yield doc


@cli.command("save")
@click.option(
    "--format",
    "formats",
    multiple=True,
    default=["json"],
    type=click.Choice(SerializationFormat.values()),
    help="Serialization format.",
)
@click.option(
    "--output",
    "destination",
    default="-",
    help="Output file path (specify '-' to write to <stdout>).",
)
@processor
def save(
    documents: list[prov.model.ProvDocument],
    destination: str,
    formats: list[str],
):
    """
    Save one or more provenance documents to file(s).
    """

    for i, doc in enumerate(documents, start=1):
        for format in formats:
            filename = f"{destination}{'-' + str(i) if len(list(documents)) > 1 else ''}.{format}"

            services.write(
                document=doc,
                filename=filename,
                format=SerializationFormat.from_string(format),
            )

            yield doc


@cli.command("merge")
@processor
def merge(documents: list[prov.model.ProvDocument]):
    """
    Merge one or more given provenance documents into a single document.
    """

    try:
        yield services.merge(documents)
    except Exception as e:
        click.echo(f"Could not merge documents: {e}", err=True)


@cli.command("transform")
@click.option(
    "--use_pseudonyms",
    is_flag=True,
    help="Use pseudonyms.",
)
@click.option(
    "--eliminate_duplicates",
    is_flag=True,
    help="Eliminate duplicates.",
)
@click.option(
    "--merge-aliased-agents",
    "aliased_agents_mapping",
    type=click.Path(exists=True, dir_okay=False),
    default=None,
    help="Merge aliased agents.",
)
@processor
def transform(
    documents: list[prov.model.ProvDocument],
    use_pseudonyms: bool = False,
    eliminate_duplicates: bool = False,
    aliased_agents_mapping: pathlib.Path | None = None,
):
    """
    Apply a set of transformations to one or more given provenance documents.
    """

    for doc in documents:
        transformed_doc = services.transform(
            document=doc,
            use_pseudonyms=use_pseudonyms,
            eliminate_duplicates=eliminate_duplicates,
            merge_aliased_agents=aliased_agents_mapping,
        )
        yield transformed_doc


@cli.command("statistics")
@click.option(
    "--resolution",
    "resolution",
    default=StatisticsResolution.COARSE,
    type=click.Choice(StatisticsResolution.values()),
    help="Print the number of PROV elements for each element type and, only with fine resolution, each relation type.",
)
@click.option(
    "--format",
    "format",
    default=StatisticsFormat.TABLE,
    type=click.Choice(StatisticsFormat.values()),
    help="Statistics output format.",
)
@processor
def statistics(
    documents: list[prov.model.ProvDocument],
    resolution: StatisticsResolution = StatisticsResolution.COARSE,
    format: StatisticsFormat = StatisticsFormat.TABLE,
):
    """
    Print statistics for one or more provenance documents.
    """

    for doc in documents:
        stats = services.statistics(document=doc, resolution=resolution, format=format)
        click.echo(stats)

        yield doc
