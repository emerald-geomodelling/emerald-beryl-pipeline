import contextlib
import luigi
import luigi.contrib.opener
import luigi.format
import SimPEG
import libaarhusxyz
import yaml
import numpy as np
import pandas as pd
import copy
from . import utils
from . import localize
from . import processing
import poltergust_luigi_utils.caching
import poltergust_luigi_utils.logging_task
import emerald_monitor
import slugify

import os
import SimPEG.directives

class SaveOutputEveryIteration(SimPEG.directives.InversionDirective):
    def __init__(self, system, task):
        self.system = system
        self.task = task
        
    def endIter(self):
        system = self.system
        task = self.task
        task.save_dataset(
            "intermediate_%s_model" % self.opt.iter,
            system.inverted_model_to_xyz(system.inv.invProb.model, system.inv.invProb.dmisfit.simulation.thicknesses))
        task.save_dataset(
            "intermediate_%s_synthetic" % self.opt.iter,
            system.forward_data_to_xyz(system.inv.invProb.dpred, inversion=True))

class ReportingDirective(SimPEG.directives.InversionDirective):
    def __init__(self, task):
        self.task = task
        self.log({"step": 0, "status": "start"})

    def log(self, data):
        self.task.log("Inversion step", extra=data)
        
    def calc_rmse(self, status):
        n_data=np.sum(self.invProb.dmisfit.W.diagonal()>0)
        status['rmse_d'] = float(np.sqrt((status['phi_d']*2)/n_data))
        status['rmse_m'] = float(np.sqrt((status['phi_m']*2)/n_data))
        status['rmse_m_scaled'] = float(np.sqrt((status['phi_m_scaled']*2)/n_data))
        status['rmse_total'] = float(np.sqrt(status['rmse_d']**2 + status['rmse_m_scaled']**2))
    
    def endIter(self):
        status={"step" : int(self.opt.iter + 2),
                'iter' : int(self.opt.iter),
                'beta' : float(self.invProb.beta),
                "phi_d": float(self.opt.parent.phi_d * self.opt.parent.opt.factor),
                "phi_m": float(self.opt.parent.phi_m * self.opt.parent.opt.factor),
                'phi_m_scaled' : float(self.invProb.phi_m * self.opt.factor * self.invProb.beta),
                "f": float(self.opt.f),
                "|proj(x-g)-x|": float(np.linalg.norm(self.opt.projection(self.opt.xc - self.opt.g) - self.opt.xc)),
                "status": "update"}
        self.calc_rmse(status)
        self.log(status)
        
    def initialize(self):
        self.log({
            "step": 1,
            "status": "initialize"})

    def finish(self):
        self.log({
            "step": int(self.opt.iter + 2),
            "status": "end"})

class Inversion(poltergust_luigi_utils.logging_task.LoggingTask, luigi.Task):
    inversion_name = luigi.Parameter()
    logging_formatter_yaml = True
    logging_formatter_yaml_include = ["time", "msg", "step", "status", "iter",
                                      "beta", "phi_d", "phi_m", "phi_m_scaled", "f", "|proj(x-g)-x|",
                                      "rmse_d", "rmse_m", "rmse_m_scaled", "rmse_total"]

    def config_target(self):
        return poltergust_luigi_utils.caching.CachingOpenerTarget('%s/config.yml' % (self.inversion_name,))

    def requires(self):
        with self.config_target().open("r") as f:
            config = yaml.load(f, Loader=yaml.SafeLoader)
        return luigi.task.externalize(processing.Processing(processing_name=config["data"].rsplit("/", 1)[0]))
    
    @contextlib.contextmanager
    def load(self):
        with self.config_target().open("r") as f:
            config = yaml.load(f, Loader=yaml.SafeLoader)

        with localize.localize(config) as self.config:
            with utils.load_system(self.config["system"]) as self.System:
                if self.config.get("system_data", None) is not None:
                    self.system_data = libaarhusxyz.GEX(self.config["system_data"])
                    self.System = self.System.load_gex(self.system_data)

                task = self
                class PipelineSystem(self.System):
                    def make_directives(self):
                        directives = task.System.make_directives(self)
                        directives += [ReportingDirective(task)]
                        if getattr(self, "save_iterations", False):
                            directives += [SaveOutputEveryIteration(self, task)]
                        return directives
                        
                self.PipelineSystem = PipelineSystem

                self.data = libaarhusxyz.XYZ(self.config["data"], normalize=True)

                self.inversion = self.PipelineSystem(self.data)

                yield
        
    def invert(self):
        with emerald_monitor.resource_monitor() as monitor:
            monitor.start_logging()
            try:
                self.inversion.invert()
            finally:
                monitor.stop_logging()

        monitor_info = monitor.get_logs()
        self.log("Inversion time(hr): {}".format(self.inversion_time(monitor_info)))

        self.datasets = {"processed": self.inversion.corrected,
                         "sparse_model": self.inversion.sparse,
                         "sparse_synthetic": self.inversion.sparsepred,
                         "smooth_model": self.inversion.l2,
                         "smooth_synthetic": self.inversion.l2pred}

        self.resource_monitor_data = monitor_info

        # for name, dataset in list(self.datasets.items()):
        #     if dataset is not None:
        #         self.datasets[name.replace(".model", ".synthetic")] = self.System(dataset, times=self.inversion.times).forward()

    def save_dataset(self, name, dataset):
        # This is just needed because right now simpleem3 uses libaarhusxyz naming standard...
        dataset.normalize(naming_standard="alc")
        survey = libaarhusxyz.Survey(dataset, self.inversion.gex)

        with localize.upload_directory(self.inversion_name) as tempdir:
            survey.dump(
                xyzfile = '%s/%s.xyz' % (tempdir, name),
                msgpackfile = '%s/%s.msgpack' % (tempdir, name),
                summaryfile = '%s/%s.summary.yml' % (tempdir, name),
                geojsonfile = '%s/%s.geojson' % (tempdir, name))

            for fline, line_data in survey.xyz.split_by_line().items():
                fline = slugify.slugify(str(fline), separator="_")
                fl_data = copy.copy(survey)
                fl_data.xyz = line_data
                fl_data.dump(
                    xyzfile = '%s/%s.%s.xyz' % (tempdir, name, fline),
                    msgpackfile = '%s/%s.%s.msgpack' % (tempdir, name, fline),
                    summaryfile = '%s/%s.%s.summary.yml' % (tempdir, name, fline),
                    geojsonfile = '%s/%s.%s.geojson' % (tempdir, name, fline))
            try:
                self.resource_monitor_data.to_csv(path_or_buf='%s/monitor_info.csv' % (tempdir,))
            except:
                pass

    def inversion_time(self, monitor_info: pd.DataFrame):
        return np.round(monitor_info.iloc[-1].elapsed_time/60/60, 4)


    def save(self):
        for name, dataset in self.datasets.items():
            if dataset is None: continue
            self.save_dataset(name, dataset)
        
        with self.output().open("w") as f:
            f.write("DONE")

    def run(self):
        with self.logging():
            self.log("Loading...")
            with self.load():
                self.log("Inverting...")
                self.invert()
                self.log("Saving...")
                self.save()
                self.log("Save done")

    def logfile(self):
        return poltergust_luigi_utils.caching.CachingOpenerTarget(
            '%s/log.yml' % (self.inversion_name,))
    
    def output(self):
         return poltergust_luigi_utils.caching.CachingOpenerTarget('%s/DONE' % (self.inversion_name,))
        
