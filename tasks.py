from invoke import task
import requests
import logging
import unidecode

logging.basicConfig(level=logging.INFO)

API = "https://topo.transport.data.gouv.fr/api.php"
SPARQL = "https://sparql.topo.transport.data.gouv.fr/bigdata/sparql"
COMMON_ARGS = f"--api {API} --sparql {SPARQL}"
DATA_GOUV_URL_PROD_ID = "P17"


def _get_producer(ctx, dataset):
    datagouv_url = f"https://www.data.gouv.fr/fr/datasets/{dataset['datagouv_id']}"
    cmd = f'entities search --claim "{DATA_GOUV_URL_PROD_ID}=<{datagouv_url}>" {COMMON_ARGS}'
    logging.info(f"searching producer: {cmd}")

    p = ctx.run(cmd)

    return p.stdout.strip()


def _get_all_datasets():
    r = requests.get("https://transport.data.gouv.fr/api/datasets")
    return r.json()


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

        cmd = f'producer create "{title}" {COMMON_ARGS} --claim "{DATA_GOUV_URL_PROD_ID}:{datagouv_url}"'

        ctx.run(cmd)


@task()
def import_all_ressources(ctx, override=False):
    """
    List all backuped ressources
    """
    nb_datasets = 0
    nb_resources = 0
    failed = []

    for d in _get_all_datasets():
        dataset_name = d["title"]

        producer = _get_producer(ctx, d)

        logging.info(f"dataset {dataset_name}, producer {producer}")
        nb_datasets += 1

        for r in d["resources"]:
            url = r.get("url")
            if not url:
                continue
            if d.get("type") != "public-transit":
                continue
            if r.get("format").lower() != "gtfs":
                continue
            nb_resources += 1

            cmd = f"import-gtfs {COMMON_ARGS} --input-gtfs {url} --producer {producer}"

            if override:
                cmd += ' --override-existing'

            logging.info(f"running {cmd}")

            res = ctx.run(cmd, warn=True)

            if res.exited != 0:
                logging.warn(f"command {res.command} exited with code {res.exited}")
                failed.append(
                    {
                        "dataset": dataset_name,
                        "produced": producer,
                        "command": cmd,
                        "status": res.exited,
                        "resource": r.get("title"),
                    }
                )

    logging.info(f"{nb_datasets} datasets and {nb_resources} resources imported")

    if failed:
        logging.warn("failed datasets:")
        for f in failed:
            logging.warn(f)
