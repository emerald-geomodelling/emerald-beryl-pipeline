#!/usr/bin/env python

import setuptools
import os

setuptools.setup(
    name='emerald-beryl-pipeline',
    version='0.0.23',
    description='',
    long_description="",
    long_description_content_type="text/markdown",
    author='Egil Moeller ',
    author_email='em@emeraldgeo.no',
    url='https://github.com/emerald-geomodelling/emerald-beryl-pipeline',
    packages=setuptools.find_packages(),
    install_requires=[
        "cython",
        "numpy==1.26.4",
        "oauth2client>=4.1.3",
        "google-api-core>=1.25.0",
        "google-api-python-client>=1.12.5",
        "swaggerspect >= 0.0.13",
        "luigi",
        "pyyaml",
        "pymkl",
        "geojson",
        "geopandas",
        "msgpack",
        "msgpack-numpy",
        "libaarhusxyz",
        "pydantic",
        "poltergust-luigi-utils>=0.0.11",
        "emerald-monitor @ git+https://github.com/emerald-geomodelling/emerald-monitor",
        "python-slugify"
    ],
    entry_points = {
        'beryl_pipeline.import': [
            'SkyTEM XYZ=beryl_pipeline.file_import:LibaarhusXYZImporter'
        ],
        'simpeg.static_instrument': [
            'Dual moment TEM=SimPEG.electromagnetics.utils.static_instrument.dual:DualMomentTEMXYZSystem',
            'Workbench import=beryl_pipeline.inversion_workbench_import:WorkbenchImporter'
        ],
        'emeraldprocessing.pipeline_step': [
            'Workbench import=beryl_pipeline.processing_workbench_import:import_from_workbench',
        ]
    }
)
