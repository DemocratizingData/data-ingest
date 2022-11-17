### About

This repo provides the package susdingest (referencing the legacy project name "Show Us The Data"), containing utilities
for ingesting model output provided by Elsevier into the SQL data model.

### Testing

Use tox to run unit tests and linting checks

```
tox
```

If you replace test data from a real run, please run the `sanitize_example.py` script on it prior to committing - this
will redact the proteceted snippet data and adjust the summary statistics to match the limited counts (please only
include a portion of a dump, e.g. one publication file)

### Building

```
python -m build
```

### Usage

The package provides a CLI:

```
susdingest -h
```

Which wraps the whole ingestion process of:

* Pulling data from an S3 bucket to local mirror
* Parsing the json publications data and verifying integrity
* Populating staging tables based on json layout
* Converting staging tables into final data model

The tool expects AWS credentials from the typical locations, either `~/.aws/credentials` or environment variables
`AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`
