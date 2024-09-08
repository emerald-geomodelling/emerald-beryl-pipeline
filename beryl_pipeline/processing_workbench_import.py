import typing
import pydantic
import emeraldprocessing.pipeline
from . import file_import

def import_from_workbench(processing : emeraldprocessing.pipeline.ProcessingData,
                          xyzfile: typing.Annotated[pydantic.AnyUrl, {"json_schema": {"minLength": 5, "pattern": "\.xyz$"}}],
                          gexfile: typing.Annotated[pydantic.AnyUrl, {"json_schema": {"minLength": 5, "pattern": "\.gex$"}}],
                          alcfile: typing.Annotated[pydantic.AnyUrl, {"json_schema": {"minLength": 5, "pattern": "\.alc$"}}] = None,
                          scalefactor = 1e-12,
                          projection: file_import.Projection = None):
    """Import a dataset processed in Aarhus Workbench. This replaces
    the input dataset completely."""
    imp = file_import.LibaarhusXYZImporter(
        xyzfile=xyzfile,
        gexfile=gexfile,
        alcfile=alcfile,
        scalefactor=scalefactor,
        projection=projection)
    processing.xyz = imp.xyz
    processing.gex = imp.gex

