import typing
import pydantic
import emeraldprocessing.pipeline
from . import file_import

class WorkbenchImporter(object):
    """Import a model inverted in Aarhus Workbench. This ignored
    the input dataset completely."""

    gexfile: typing.Annotated[pydantic.AnyUrl, {"json_schema": {"minLength": 5, "pattern": "\.gex$"}}] = None
    corrected_xyzfile: typing.Annotated[pydantic.AnyUrl, {"json_schema": {"title": "*.DAT.XYZ", "minLength": 5, "pattern": "\.xyz$"}}]
    model_xyzfile: typing.Annotated[pydantic.AnyUrl, {"json_schema": {"title": "*INV.XYZ", "minLength": 5, "pattern": "\.xyz$"}}]
    fwd_xyzfile: typing.Annotated[pydantic.AnyUrl, {"json_schema": {"title": "*SYN.XYZ", "minLength": 5, "pattern": "\.xyz$"}}]
    scalefactor = 1e-12
    projection: file_import.Projection = None

    @classmethod
    def load_gex(cls, gex):
        class Cls(cls):
            pass
        Cls.__name__ = cls.__name__
        Cls.gex = gex
        return Cls
    
    def invert(self):
        pass

    def __init__(self, xyz):        
        corrected = file_import.LibaarhusXYZImporter(
            xyzfile=self.corrected_xyzfile,
            gexfile=self.gexfile,
            scalefactor=self.scalefactor,
            projection=self.projection)
        if corrected.gex is not None:
            self.gex = corrected.gex
        self.corrected = corrected.xyz
        
        fwd = file_import.LibaarhusXYZImporter(
            xyzfile=self.fwd_xyzfile,
            gexfile=self.gexfile,
            scalefactor=self.scalefactor,
            projection=self.projection)
        self.l2pred = fwd.xyz
        model= file_import.LibaarhusXYZImporter(
            xyzfile=self.model_xyzfile,
            gexfile=self.gexfile,
            scalefactor=self.scalefactor,
            projection=self.projection)
        self.l2 = model.xyz
        self.sparsepred = None
        self.sparse = None
