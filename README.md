# DE-Assessments - Feb, 2023

This repo contains the code for creating a data pipeline to calculate metrics for a temperature dataset:

* `etl_temperature.py` -- executes the pipeline using the `city_temperature.csv` file on repo's root.

# Installation

To get this repo running:

* Install Python 3.  You can find instructions [here](https://wiki.python.org/moin/BeginnersGuide/Download).
* Clone this repo with `git clone https://github.com/l-acosta/DE-Assessments.git`
* Create a virtual environment with the command: `python -m venv venv`.
    - Activate your virtual environment: `source venv/bin/activate`
* Install the requirements with `pip install -r requirements.txt`

# Usage

* Source environment variable: `source env_variables.sh`
* Launch Airflow: `bash launch_airflow.sh`
* Go to the UI at `http://localhost:8080/`
    - credentials: username: `admin`, standalone password in `airflow/standalone_admin_password.txt`
