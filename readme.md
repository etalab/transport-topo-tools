
## Install

You need to install [Pipenv](https://docs.pipenv.org/en/latest/) to run the script:

`pip install --user pipenv`

Then you need to run `pipenv install`

## Running

There are several endpoints to the script.

### Prepopulate

```bash
pipenv run invoke prepopulate
```

### Create all producer

```bash
pipenv run invoke create-all-producer
```

### Import transport.data.gouv.fr ressources

```bash
pipenv run invoke import-all-ressources
```