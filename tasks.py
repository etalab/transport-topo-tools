from invoke import task
import requests
import logging
import unidecode

logging.basicConfig(level=logging.INFO)

API = "https://topo.transport.data.gouv.fr/api.php"
SPARQL = "https://sparql.topo.transport.data.gouv.fr/bigdata/sparql"
COMMON_ARGS = f"--api {API} --sparql {SPARQL}"
DATA_GOUV_URL_PROP_ID = None


def _get_producer(ctx, dataset):
    datagouv_url = f"https://www.data.gouv.fr/fr/datasets/{dataset['datagouv_id']}"
    data_gouv_prop_id = _get_data_gouv_prop_id(ctx)
    cmd = (
        f'entities search --claim "{data_gouv_prop_id}=<{datagouv_url}>" {COMMON_ARGS}'
    )
    logging.info(f"searching producer: {cmd}")

    p = ctx.run(cmd)

    return p.stdout.strip()


def _get_all_datasets():
    r = requests.get("https://transport.data.gouv.fr/api/datasets")
    return r.json()


@task()
def init(ctx):
    prepopulate(ctx)
    create_data_gouv_url_prop(ctx)
    create_all_producer(ctx)


@task()
def create_data_gouv_url_prop(ctx):

    cmd = f'entities create "data_gouv_url" --type urlproperty {COMMON_ARGS}'
    logging.info(f"searching 'data_gouv_url' prop: {cmd}")

    p = ctx.run(cmd)

    global DATA_GOUV_URL_PROP_ID
    DATA_GOUV_URL_PROP_ID = p.stdout.strip()

    return DATA_GOUV_URL_PROP_ID


def _get_data_gouv_prop_id(ctx):
    if DATA_GOUV_URL_PROP_ID is None:
        create_data_gouv_url_prop(ctx)
    return DATA_GOUV_URL_PROP_ID


@task()
def prepopulate(ctx):
    """
    List all backuped ressources
    """
    ctx.run(f"prepopulate {COMMON_ARGS}")


@task()
def create_all_producer(ctx):
    """
    List all backuped ressources
    """
    nb_datasets = 0
    for d in _get_all_datasets():
        nb_datasets += 1
        title = d["title"]

        logging.info(f"creating producer {title}")
        if title is None:
            logging.info(f"skipping dataset {d}")
            continue

        # we use only ascii character for the title as cellar can be a tad strict
        title = unidecode.unidecode(title)

        datagouv_url = f"https://www.data.gouv.fr/fr/datasets/{d['datagouv_id']}"

        data_gouv_prop_id = _get_data_gouv_prop_id(ctx)

        cmd = f'entities create "{title}" {COMMON_ARGS} --type item --unique-claim "@instance_of=@producer" --claim "{data_gouv_prop_id}={datagouv_url}"'

        ctx.run(cmd)


class DatasetImportResult(object):
    def __init__(self):
        super().__init__()
        self.nb_resources = 0
        self.failed = []


def _import_dataset(ctx, d, override):
    dataset_name = d["title"]

    producer = _get_producer(ctx, d)

    logging.info(f"dataset {dataset_name}, producer {producer}")
    result = DatasetImportResult()

    for r in d["resources"]:
        url = r.get("url")
        if not url:
            continue
        if d.get("type") != "public-transit":
            continue
        if r.get("format").lower() != "gtfs":
            continue
        result.nb_resources += 1

        cmd = f"import-gtfs {COMMON_ARGS} --input-gtfs {url} --producer {producer}"

        if override:
            cmd += " --override-existing"

        logging.info(f"running {cmd}")

        res = ctx.run(cmd, warn=True)

        if res.exited != 0:
            logging.warn(f"command {res.command} exited with code {res.exited}")
            result.failed.append(
                {
                    "dataset": dataset_name,
                    "produced": producer,
                    "command": cmd,
                    "status": res.exited,
                    "resource": r.get("title"),
                }
            )
    return result


@task()
def import_all_ressources(ctx, override=False):
    """
    List all backuped ressources
    """
    nb_datasets = 0
    nb_resources = 0
    failed = []

    for d in _get_all_datasets():
        nb_datasets += 1
        res = _import_dataset(ctx, d, override)
        nb_resources += res.nb_resources
        failed.extend(res.failed)

    logging.info(f"{nb_datasets} datasets and {nb_resources} resources imported")

    if failed:
        logging.warn("failed datasets:")
        for f in failed:
            logging.warn(f)
