import luigi
import luigi.contrib.opener
import luigi.format
import libaarhusxyz
import yaml
import tempfile
import shutil
from . import utils
from . import localize
import poltergust_luigi_utils.caching
import poltergust_luigi_utils.logging_task
import typing
import pydantic
import importlib.metadata
import pandas as pd
import os
import copy
import slugify


Projection = typing.Annotated[
    int,
    {"json_schema": {
        "format": "x-epsg",
    }}]

LibaarhusXYZImporterSelf = typing.TypeVar("Self", bound="LibaarhusXYZImporter")
class LibaarhusXYZImporter(libaarhusxyz.Survey):
    json_schema = {"hide": True}
    api_type = "__init__"
    def __init__(self: LibaarhusXYZImporterSelf,
                 files: typing.Annotated[dict, {"json_schema": 
                 {"type": "object", 
                  "x-format": "multi-url",
                  "description": "Required: .gex, .xyz | Optional: .alc",
                  "properties": {
                    "xyzfile": {"minLength": 5, "pattern": "\.xyz$", "type": "string", "format": "url", "description": "The data itself"},
                    "gexfile": {"minLength": 5, "pattern": "\.gex$", "type": "string", "format": "url", "description": "System description / calibration file"},
                    "alcfile": {"minLength": 5, "pattern": "\.alc$", "type": "string", "format": "url", "description": "Allocation file (column name mapping)"},
                }}}],
                 scalefactor = 1e-12,
                 projection: Projection = None):
        """Import SkyTEM data

        Parameters
        ----------
        scalefactor :
            Data unit, 1 = volt, 1e-12 = picovolt
        projection :
            EPSG code for the projection and chart datum of sounding locations
        """
        xyzfile = files.get("xyzfile")
        gexfile = files.get("gexfile")
        alcfile = files.get("alcfile")

        assert isinstance(projection, int) and projection > 0, "Invalid projection, please provide a valid projection"
        scalefactor = float(scalefactor)
        assert scalefactor != 0, "Invalid scalefactor, please provide a valid scalefactor"
        assert xyzfile is not None, "Missing xyz file"
        assert gexfile is not None, "Missing gex file"
        
        xyz = libaarhusxyz.XYZ(xyzfile, alcfile=alcfile)
        if scalefactor:
            xyz.model_info['scalefactor'] = scalefactor
        if projection:
            xyz.model_info['projection'] = projection        
        xyz.normalize(naming_standard="alc")
        
        assert "projection" in xyz.model_info
        assert "scalefactor" in xyz.model_info

        # Check for None only to support inversion_workbench_import
        gex = libaarhusxyz.GEX(gexfile) if gexfile is not None else None

        libaarhusxyz.Survey.__init__(self, xyz, gex)


def _load_helitem_gex(gexfile):
    """Load a HeliTEM GEX file, adding fields required by libaarhusxyz if missing.

    XCalibur HeliTEM GEX files are Aarhus Workbench compatible but may lack
    TransmitterMoment and TxLoopArea which libaarhusxyz.GEX requires.
    This function parses the raw GEX, adds the missing fields, computes
    ApproxDipoleMoment, and returns a libaarhusxyz.GEX object.
    """
    import numpy as np
    import re
    with open(gexfile) as f:
        lines = f.readlines()

    # Preprocess GEX for libaarhusxyz parser compatibility:
    # 1. Strip inline comments: "Key=Value  /comment" -> "Key=Value"
    # 2. Remove brackets from comment lines so they aren't parsed as sections
    #    e.g. "/Marks the start of the [General] section" -> "/Marks the start..."
    cleaned = []
    for line in lines:
        stripped = line.lstrip()
        if stripped.startswith('/') and '[' in stripped:
            # Comment line with brackets — remove brackets to avoid section detection
            line = line.replace('[', '(').replace(']', ')')
        elif '=' in line and not stripped.startswith('/'):
            # Strip inline comments after values
            line = re.sub(r'\s+/[^0-9].*', '\n', line)
        cleaned.append(line)

    sections, sectionheaders = libaarhusxyz.gex.split_sections(cleaned)

    gex_dict = {"header": sections["header"]}
    for header in sectionheaders:
        gex_dict[header.strip("[").strip("]")] = libaarhusxyz.gex.parse_parameters(sections[header])

    # Add TxLoopArea from TxLoopSides if missing (rectangular loop)
    general = gex_dict.get("General", {})
    if "TxLoopArea" not in general and "TxLoopSides" in general:
        sides = general["TxLoopSides"]
        general["TxLoopArea"] = float(sides[0]) * float(sides[1])

    # Add TransmitterMoment to each Channel if missing (single-moment system)
    number_channels = np.array(["Channel" in key for key in gex_dict.keys()]).sum()
    for channel in range(1, 1 + number_channels):
        channel_key = f"Channel{channel}"
        if "TransmitterMoment" not in gex_dict[channel_key]:
            gex_dict[channel_key]["TransmitterMoment"] = ""

    # Compute ApproxDipoleMoment (same logic as libaarhusxyz.gex._parse)
    for channel in range(1, 1 + number_channels):
        channel_key = f"Channel{channel}"
        tx_mom = gex_dict[channel_key]["TransmitterMoment"]
        turn_key = f"NumberOfTurns{tx_mom}"
        gex_dict[channel_key]["ApproxDipoleMoment"] = (
            gex_dict["General"][turn_key]
            * gex_dict["General"]["TxLoopArea"]
            * gex_dict[channel_key]["TxApproximateCurrent"]
        )

    return libaarhusxyz.GEX(gex_dict)


HeliTEM2LibAarhusImporterSelf = typing.TypeVar("Self", bound="HeliTEM2LibAarhusImporter")
class HeliTEM2LibAarhusImporter(libaarhusxyz.Survey):
    json_schema = {"hide": True}
    api_type = "__init__"
    def __init__(self: HeliTEM2LibAarhusImporterSelf,
                 files: typing.Annotated[dict, {"json_schema":
                 {"type": "object",
                  "x-format": "multi-url",
                  "description": "Required: .asc, .gex",
                  "properties": {
                    "ascfile": {"minLength": 5, "pattern": r"\.asc$", "type": "string", "format": "url", "description": "HeliTEM survey data file"},
                    "gexfile": {"minLength": 5, "pattern": r"\.gex$", "type": "string", "format": "url", "description": "System description / calibration file"},
                }}}],
                 scalefactor: float = 1.0,
                 projection: Projection = None):
        """Import XCalibur HeliTEM data

        Parameters
        ----------
        scalefactor :
            Data unit, 1 = SI units (default for HeliTEM)
        projection :
            EPSG code for the projection and chart datum of sounding locations
        """
        ascfile = files.get("ascfile")
        gexfile = files.get("gexfile")

        assert isinstance(projection, int) and projection > 0, "Invalid projection, please provide a valid projection"
        scalefactor = float(scalefactor)
        assert scalefactor != 0, "Invalid scalefactor, please provide a valid scalefactor"
        assert ascfile is not None, "Missing .asc file"
        assert gexfile is not None, "Missing .gex file"

        import emerald_helitem_converter

        # 1. Convert .asc + .gex -> .xyz + .alc + .gex in temp dir
        with tempfile.TemporaryDirectory() as tmpdir:
            xyz_path = emerald_helitem_converter.convert(
                asc_path=ascfile,
                gex_path=gexfile,
                output_dir=tmpdir,
                projection=projection,
                scalefactor=scalefactor,
            )
            alc_path = xyz_path.with_suffix('.alc')

            # 2. Load converted output with libaarhusxyz
            xyz = libaarhusxyz.XYZ(str(xyz_path), alcfile=str(alc_path))

        # 3. Set metadata and normalize
        xyz.model_info['scalefactor'] = scalefactor
        xyz.model_info['projection'] = projection
        xyz.normalize(naming_standard="alc")

        assert "projection" in xyz.model_info
        assert "scalefactor" in xyz.model_info

        # 4. Load GEX (with HeliTEM compatibility preprocessing)
        gex = _load_helitem_gex(gexfile)

        # 5. Initialize Survey base class
        libaarhusxyz.Survey.__init__(self, xyz, gex)


importers = {entry.name: entry for entry in importlib.metadata.entry_points(group="beryl_pipeline.import")}
        
class Import(poltergust_luigi_utils.logging_task.LoggingTask, luigi.Task):
    import_name = luigi.Parameter()
    logging_formatter_yaml = True

    def __init__(self, *arg, **kw):
        luigi.Task.__init__(self, *arg, **kw)
        self._log = []

    def config_target(self):
        return poltergust_luigi_utils.caching.CachingOpenerTarget('%s/config.yml' % (self.import_name,))
            
    def run(self):
        with self.logging():
            self.log("Read config")

            with self.config_target().open("r") as f:
                config = yaml.load(f, Loader=yaml.SafeLoader)

            self.log("Download files")

            with localize.localize(config) as config:
                with localize.upload_directory(self.import_name) as tempdir:
                    self.log("Import data")

                    importer_fn = importers[config["importer"]["name"]].load()
                    importer = importer_fn(**config["importer"].get("args", {}))

                    self.log("Write and upload data")
                    importer.dump(
                        xyzfile = '%s/out.xyz' % (tempdir,),
                        gexfile = '%s/out.gex' % (tempdir,),
                        msgpackfile = '%s/out.msgpack' % (tempdir,),
                        summaryfile = '%s/out.summary.yml' % (tempdir,),
                        geojsonfile = '%s/out.geojson' % (tempdir,))

                    for fline, line_data in importer.xyz.split_by_line().items():
                        fline = slugify.slugify(str(fline), separator="_")
                        line_importer = copy.copy(importer)
                        line_importer.xyz = line_data
                        line_importer.dump(
                            xyzfile = '%s/out.%s.xyz' % (tempdir, fline),
                            gexfile = '%s/out.%s.gex' % (tempdir, fline),
                            msgpackfile = '%s/out.%s.msgpack' % (tempdir, fline),
                            summaryfile = '%s/out.%s.summary.yml' % (tempdir, fline),
                            geojsonfile = '%s/out.%s.geojson' % (tempdir, fline))

                with self.output().open("w") as f:
                    f.write("DONE")                

    def logfile(self):
        return poltergust_luigi_utils.caching.CachingOpenerTarget(
            '%s/log.yml' % (self.import_name,))

    def system_data(self):
        return poltergust_luigi_utils.caching.CachingOpenerTarget(
            '%s/out.gex' % (self.import_name,),
            format=luigi.format.NopFormat())

    def data(self):
        return poltergust_luigi_utils.caching.CachingOpenerTarget(
            '%s/out.xyz' % (self.import_name,),
            format=luigi.format.NopFormat())

    def data_msgpack(self):
        return poltergust_luigi_utils.caching.CachingOpenerTarget(
            '%s/out.msgpack' % (self.import_name,),
            format=luigi.format.NopFormat())

    def summary(self):
        return poltergust_luigi_utils.caching.CachingOpenerTarget(
            '%s/out.summary.yml' % (self.import_name,))

    def fl_data(self, fline):
        return poltergust_luigi_utils.caching.CachingOpenerTarget(
            f'{self.import_name}/out.{fline}.xyz',
            format=luigi.format.NopFormat())

    def fl_data_msgpack(self, fline):
        return poltergust_luigi_utils.caching.CachingOpenerTarget(
            f'{self.import_name}/out.{fline}.msgpack',
            format=luigi.format.NopFormat())

    def fl_summary(self, fline):
        return poltergust_luigi_utils.caching.CachingOpenerTarget(
            f'{self.import_name}/out.{fline}.summary.yml')

    def output(self):
        return poltergust_luigi_utils.caching.CachingOpenerTarget('%s/DONE' % (self.import_name,))
