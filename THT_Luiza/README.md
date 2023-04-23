# Take Home Test - Luiza Araujo Costa Silva - Apr, 2023

# Overview

This project aims to implement a event processing to analyse the connectivity environment of a device in the period of
time surrounding when an order is dispatched to it.
* `events_processing.py` -- executes the pipeline using the files stored in the folder `data/input`.

It computes numbers of pooling events related to orders across 3 periods of time:

- 3 minutes before the order creation time
- 3 minutes after the order creation time
- 60 minutes before the order creation time
    
Then it presents the counts of:

- The total count of all polling events
- The count of each type of polling status_code
- The count of each type of polling error_code
- The count of responses with no error_code.

# Installation

To get this project running:

* Install Python 3.  You can find instructions [here](https://wiki.python.org/moin/BeginnersGuide/Download).
* Open this folder/repo in your IDE
* Create a virtual environment with the command: `python -m venv venv`.
    - Activate your virtual environment: `source venv/bin/activate`
* Install the requirements with `pip install -r requirements.txt`

# Usage

1. To run the application, execute `python events_processing.py` in your terminal.
2. When it finishes, the `events_report` file will be saved in the folder `data/output`.

## Assumptions
These are assumptions made based on data found in provided dataset:

- There are only 3 possible values for `status_code`: `[0, 200, 401]`
- There are only 2 possible values for `error_code`: `['ECONNABORTED', 'GENERIC_ERROR']`

# Future releases
- Implement the usage of parameterized arguments for the `main` input and output (file names and paths).
- Improve unit tests to validade specific input and output columns.
- Implement integration test and a complete unit test for transformation module.
- Implement Docker containers to simplify the deployment of the application.
