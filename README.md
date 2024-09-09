![Endpoint Badge](https://img.shields.io/endpoint?url=https%3A%2F%2Fgist.githubusercontent.com%2Fredhog%2F9e15232e12bf2ddf537185b43ca2060f%2Fraw%2Fcb3a10893e74a0f97e94bcb7051c69270157fe49%2Fexperimental-pipeline-inversion-junit-tests.json)
![Endpoint Badge](https://img.shields.io/endpoint?url=https%3A%2F%2Fgist.githubusercontent.com%2Fredhog%2F9e15232e12bf2ddf537185b43ca2060f%2Fraw%2Fcb3a10893e74a0f97e94bcb7051c69270157fe49%2Fexperimental-pipeline-inversion-cobertura-coverage.json)

# emerald-beryl-pipeline

Luigi based pipeline to run inversions using [our simplified SimPEG wrapper](https://github.com/emerald-geomodelling/experimental-simpeg-ext)

# Installation

Clone this repo, then run `pip install .[all]`. Note the `[all]`.
Without this, emerald-processing-em and simpeg/simpleem won't be
installed, and you'll have to install some version of these manually.

# Documentation

* Documentation on how to [run all the pipelines from anotebook](docs/run%20processing%20inversion%20luigi.ipynb)
* Documentation on how to [extract the API documentation](docs/run%20introspection.ipynb) used to generate the front end UI
  * This should be used to test any changes / additions to e.g. processing pipeline filters

# Running from the command line

To run this pipeline locally:

```
luigi --module beryl_pipeline.inversion Inversion --inversion-name=file:///some/temp/dir
```

This assumes you've copied `docs/example-real.yml` to `/some/temp/dir/config.yml`.


# Unit tests

To run the unit tests, first `pip install nose2` and then run

```
nose2 -s tests
```
