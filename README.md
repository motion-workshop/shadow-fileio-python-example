# Shadow File IO Example for Python

[![Build Status](https://travis-ci.org/motion-workshop/shadow-fileio-python-example.svg?branch=master)](https://travis-ci.org/motion-workshop/shadow-fileio-python-example)

## Introduction

Example usage of the Shadow take loader Python module. Implements a command
line application that will convert the most recent take to the Avro or CSV file
format.

## Quick Start

Install the required Python modules.

  pip install -r requirements.txt

Run the application. Convert to Avro format.

  python main.py

Run the application. Convert to CSV format.

  python main.py --csv

## Upload

### BigQuery

Use the bq command line tool. You can use the upload interface in the GCP
Console but the option to replace the table and convert the timestamp format
are not available.

  bq load --replace --use_avro_logical_types --source_format=AVRO test.stream data.avro

## License

This project is distributed under a permissive [BSD License](LICENSE).
