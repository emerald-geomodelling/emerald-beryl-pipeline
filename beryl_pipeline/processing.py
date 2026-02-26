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


def _strip_missing_channel_refs(steps, available_channels, log_fn=None):
    """Strip references to missing channels from processing step configs.

    Single-moment data (e.g., HeliTEM) only has Gate_Ch01. Processing steps
    like cull_on_geometry have defaults that include Gate_Ch02, which would
    crash if applied to single-moment data. This function removes channel
    references from step parameters that don't exist in the loaded data.

    Steps use the format: [{"modulename.function_name": {"arg1": val, ...}}, ...]
    """
    for step in steps:
        if not isinstance(step, dict):
            continue
        for step_name, args in step.items():
            if not isinstance(args, dict):
                continue
            for param_name, param_value in list(args.items()):
                if isinstance(param_value, dict):
                    keys_to_remove = [
                        k for k in param_value
                        if k.startswith('Gate_Ch') and k not in available_channels
                    ]
                    for k in keys_to_remove:
                        del param_value[k]
                        if log_fn:
                            log_fn(
                                f"Stripped '{k}' from step "
                                f"'{step_name}.{param_name}' "
                                f"(channel not present in data)"
                            )


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

            parent_url = config.get("parent_url")

            self.log("Download files")

            with localize.localize(config) as config:
                with localize.upload_directory(self.processing_name) as tempdir:
                    self.log("Read data")

                    data = utils.load_fn(config["data"]["name"])(outdir = tempdir, **config["data"].get("args", {}))
                    data.orig_xyz = libaarhusxyz.XYZ(config["data"]["args"]["data"], naming_standard="alc", normalize=True)
                    data.orig_xyz_by_line = data.orig_xyz.split_by_line()

                    self.log("Processing")

                    # Strip references to channels not in the data (e.g., Gate_Ch02
                    # defaults on single-moment HeliTEM data)
                    available_channels = {
                        k for k in data.xyz.layer_data.keys()
                        if k.startswith('Gate_Ch')
                    }
                    _strip_missing_channel_refs(
                        config["steps"], available_channels, log_fn=self.log
                    )

                    data.process(config["steps"])

                    for key in data.xyz.layer_data.keys():
                        if inuse_key_prefix in key:
                            if '_' not in key.split(inuse_key_prefix)[0]:
                                col_name = f"num_{key}"
                                data.xyz.flightlines[col_name] = np.abs(data.xyz.layer_data[key]).sum(axis=1, skipna=True)

                    self.log("Write data")

                    # Extract affected_lines from the last step if present
                    affected_lines = None
                    for step in reversed(config.get("steps", [])):
                        if isinstance(step, dict):
                            for step_args in step.values():
                                if isinstance(step_args, dict) and "affected_lines" in step_args:
                                    affected_lines = set(str(l) for l in step_args["affected_lines"])
                                    break
                        if affected_lines is not None:
                            break

                    data.dump(
                        xyzfile = '%s/processed.xyz' % (tempdir,),
                        gexfile = '%s/processed.gex' % (tempdir,),
                        msgpackfile = '%s/processed.msgpack' % (tempdir,),
                        diffmsgpackfile = '%s/processed.diff.msgpack' % (tempdir,),
                        summaryfile = '%s/processed.summary.yml' % (tempdir,),
                        geojsonfile = '%s/processed.geojson' % (tempdir,))

                    line_extensions = ['.xyz', '.gex', '.msgpack', '.diff.msgpack', '.summary.yml', '.geojson']

                    for fline, line_data in data.xyz.split_by_line().items():
                        sfline = slugify.slugify(str(fline), separator="_")

                        if affected_lines is not None and parent_url and str(fline) not in affected_lines:
                            # Unaffected line: copy per-line output from parent
                            self.log("Copying unaffected line %s from parent" % fline)
                            for ext in line_extensions:
                                parent_file = '%s/processed.%s%s' % (parent_url, sfline, ext)
                                local_file = '%s/processed.%s%s' % (tempdir, sfline, ext)
                                try:
                                    src = poltergust_luigi_utils.caching.CachingOpenerTarget(
                                        parent_file,
                                        format=luigi.format.NopFormat())
                                    with src.open("r") as inf:
                                        with open(local_file, "wb") as outf:
                                            shutil.copyfileobj(inf, outf)
                                except Exception as e:
                                    self.log("Warning: could not copy %s from parent: %s" % (parent_file, e))
                        else:
                            # Affected line (or no per-flightline optimization): dump normally
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
