# PRM GP2GP Data Sandbox

This repository contains focused explorations of data associated with GP2GP
utilisation.

These are each contained within Jupyter notebooks under the `notebooks`
directory.

Notebooks typically read data in from S3. Some sources of data, such as
transfers data or ODS metadata, are ready to use as-is and can be read in
directly from the relevant S3 bucket. Datasets that require manual preparation
(e.g raw SPINE logs) should be stored in the notebooks data bucket in a
directory named after the notebook the data is supporting. This directory
should be named in the following format:

```
[bucket-name]/[story-number]-[description]
```

For example:

```
prm-gp2gp-notebook-data-prod/PRMT-1234-attachments-metadata-aug2021/
```


The `data` directory contains:
 - Data-sets used in early notebooks (typically exported via queries from NMS).
 - Small data lookups (e.g GP2GP error codes)
 - Helper functions to load in data that requires pre-processing to work in
   Pandas (e.g ODS metadata)

## Local Setup

If you need to run the notebooks locally, follow these steps:

### Create a virtual environment

From the base directory of the project, create a python3 virtual environment
and activate it:

```
python3 -m venv venv
source venv/bin/activate
```

The shell prompt should now show that the virtual environment has been
activated.

### Install dependencies

```
pip install --upgrade pip
pip install -r requirements.txt
```
### Configure notebook friendly git diffs

Configure [`nbdime` (link)](nbdime.readthedocs.io/en/latest/) for viewing diffs
in jupyter notebooks

```
nbdime config-git --enable
```

If you receive an error like `fatal: external diff died, stopping at
<notebookpath>.ipynb` when running `git diff`, then check that the virtual
environment is activated - this should fix the issue.

To deactivate the virtual environment:
```sh
deactivate
```

### Starting the Jupyter server

To start a local Jupyter server and access the notebooks:

#### Activate the virtual environment:

```sh
source venv/bin/activate
```

#### Start the Jupyter server:
```sh
jupyter notebook
```
