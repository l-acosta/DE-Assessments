# DE-Assessments - Feb, 2023

This repo contains the code for creating a data pipeline to calculate metrics for a temperature dataset:

* `etl_temperature.py` -- executes the pipeline using the `city_temperature.csv` file on repo's root.

# Installation

To get this repo running:

* Install Python 3.  You can find instructions [here](https://wiki.python.org/moin/BeginnersGuide/Download).
* Create a virtual environment with the command: `python -m venv venv`.
    - Activate your virtual environment: `source venv/bin/activate`
* Clone this repo with `git clone https://github.com/l-acosta/DE-Assessments.git`
* Save the input file as `city_temperature.csv` inside the `Assessment_01` folder.
* Get into the folder with `cd Assessment_01`
* Install the requirements with `pip install -r requirements.txt`

# Usage

* Execute the script mentioned above: `python3 etl_temperature.py`

You should see the output files created on schemas corresponding folders.
