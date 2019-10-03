# PRM GP2GP Data Sandbox

This repository contains focused explorations of data associated with GP2GP utilisation.

These are each contained within Jupyter notebooks under the `notebooks` directory.

The `data` directory contains data-sets (typically exported via queries from NMS) used in the explorations.

## Setup

1. From the base directory of the project, create a python3 virtual environment and activate it:
   ```sh
   $ python3 -m venv venv
   $ source venv/bin/activate
   ```
   The shell prompt should now show that the virtual environment has been activated:
   ```sh
   (venv)$
   ```
2. Install the dependencies:
   ```sh
   (venv)$ pip install --upgrade pip
   (venv)$ pip install -r requirements.txt
   ```

To deactivate the virtual environment:
```sh
(venv)$ deactivate
```

## Starting the Jupyter server

The explorations are self-contained [Jupyter notebooks](https://https://jupyter.org). To start a local Jupyter server and access the notebooks:

1. Activate the virtual environment:
   ```sh
   $ source venv/bin/activate
   ```
2. Start the Jupyter server:
   ```sh
   (venv)$ jupyter notebook
   ```
