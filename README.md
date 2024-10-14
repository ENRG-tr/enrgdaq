# ENRGDAQ

ENRGDAQ is a Python program that allows you to easily create and run data acquisition jobs. It provides a simple and flexible way to collect data from various sources and store it in various formats.

## Installation

0. Make sure you have Python 3.12.
1. Clone the repository:

```
git clone https://github.com/ENRG-tr/enrgdaq.git
cd enrgdaq
```

2. To install the dependencies:

- We recommend using uv for installing dependencies:

```
uv sync
```

- Or you can use venv:

```
python -m venv venv
source venv/bin/activate # On Windows use `venv\Scripts\activate`
pip install -r requirements.txt
```

3. Using the configuration files in `configs/examples/` as a guide, create your own configuration files in the `configs/` directory, with `.toml` extension.

4. Finally, run the program:

```
python src/main.py
```

# Development

## Pre commit hooks

To install the pre-commit hooks, run the following command:

```
pre-commit install
```

## Running tests

To run the tests, use the following command:

```
python src/run_tests.py
```
