# scitodate-institute-disambiguation

This repository represents the PySpark pipeline that solves *(on some level)*
the disambiguation of institutes' names.

## Run pipeline

You need to perform the following actions to run the pipeline:

1. Go to the project directory

```shell
cd /path/to/scitodate-institute-disambiguation
```

2. Create a virtual environment

```shell
python3 -m venv venv
```

3. Activate the virtual environment

```shell
source venv/bin/activate
```

4. Install the required dependencies

```shell
pip install -r requirements.txt
```

5. Run the pipeline using the following command
   *(It is said that PySpark has its own command to run, but we have what we have)*

```shell
python main.py --input /path/to/data.csv --output /path/to/output/
```

Here are the arguments of the run command:

* `--input` – the path to the file with data to process. Required.
* `--output` – the path to the folder to save the results of the pipeline.
  Optional, defaults to `./result`. 
