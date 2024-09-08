import luigi
import luigi.format
import libaarhusxyz
import pandas as pd
import yaml
import tempfile
import shutil
import os.path
import yaml
from . import utils
from . import localize
from . import file_import
import poltergust_luigi_utils.caching
import poltergust_luigi_utils.logging_task
from emeraldprocessing.pipeline import ProcessingData
import copy
import numpy as np
import slugify

from emeraldprocessing.tem.data_keys import inuse_key_prefix


class Processing(poltergust_luigi_utils.logging_task.LoggingTask, luigi.Task):
    processing_name = luigi.Parameter()
    logging_formatter_yaml = True

    def requires(self):
        with self.config_target().open("r") as f:
            config = yaml.load(f, Loader=yaml.SafeLoader)
        return luigi.task.externalize(file_import.Import(import_name=config["data"]["args"]["data"].rsplit("/", 1)[0]))

    def __init__(self, *arg, **kw):
        luigi.Task.__init__(self, *arg, **kw)

    def config_target(self):
        return poltergust_luigi_utils.caching.CachingOpenerTarget('%s/config.yml' % (self.processing_name,))
            
    def run(self):
        with self.logging():
            self.log("Read config")

            with self.config_target().open("r") as f:
                config = yaml.load(f, Loader=yaml.SafeLoader)

            self.log("Download files")

            with localize.localize(config) as config:
                with localize.upload_directory(self.processing_name) as tempdir:
                    self.log("Read data")

                    data = utils.load_fn(config["data"]["name"])(outdir = tempdir, **config["data"].get("args", {}))
                    data.orig_xyz = libaarhusxyz.XYZ(config["data"]["args"]["data"], naming_standard="alc", normalize=True)
                    data.orig_xyz_by_line = data.orig_xyz.split_by_line()

                    self.log("Processing")

                    data.process(config["steps"])

                    for key in data.xyz.layer_data.keys():
                        if inuse_key_prefix in key:
                            if '_' not in key.split(inuse_key_prefix)[0]:
                                col_name = f"num_{key}"
                                data.xyz.flightlines[col_name] = np.abs(data.xyz.layer_data[key]).sum(axis=1, skipna=True)

                    self.log("Write data")


                    data.dump(
                        xyzfile = '%s/processed.xyz' % (tempdir,),
                        gexfile = '%s/processed.gex' % (tempdir,),
                        msgpackfile = '%s/processed.msgpack' % (tempdir,),
                        diffmsgpackfile = '%s/processed.diff.msgpack' % (tempdir,),
                        summaryfile = '%s/processed.summary.yml' % (tempdir,),
                        geojsonfile = '%s/processed.geojson' % (tempdir,))

                    for fline, line_data in data.xyz.split_by_line().items():
                        sfline = slugify.slugify(str(fline), separator="_")
                        fl_data = copy.copy(data)
                        fl_data.xyz = line_data
                        fl_data.orig_xyz = data.orig_xyz_by_line[fline]
                        fl_data.dump(
                            xyzfile = '%s/processed.%s.xyz' % (tempdir, sfline),
                            gexfile = '%s/processed.%s.gex' % (tempdir, sfline),
                            msgpackfile = '%s/processed.%s.msgpack' % (tempdir, sfline),
                            diffmsgpackfile = '%s/processed.%s.diff.msgpack' % (tempdir, sfline),
                            summaryfile = '%s/processed.%s.summary.yml' % (tempdir, sfline),
                            geojsonfile = '%s/processed.%s.geojson' % (tempdir, sfline))
                                
            self.log("Done")

            with self.output().open("w") as f:
                f.write("DONE")

    def logfile(self):
        return poltergust_luigi_utils.caching.CachingOpenerTarget(
            '%s/log.yml' % (self.processing_name,))

    def data(self):
        return poltergust_luigi_utils.caching.CachingOpenerTarget('%s/processed.xyz' % (self.processing_name,))
    
    def system_data(self):
        return poltergust_luigi_utils.caching.CachingOpenerTarget('%s/processed.gex' % (self.processing_name,))
    
    def output(self):
        return poltergust_luigi_utils.caching.CachingOpenerTarget('%s/DONE' % (self.processing_name,))
