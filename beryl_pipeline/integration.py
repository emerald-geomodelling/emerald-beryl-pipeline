import luigi
import poltergust_luigi_utils.caching
import yaml
from . import file_import
from . import processing
from . import inversion

class IntegrationStep(luigi.Task):
    integration_name = luigi.Parameter()
    step = luigi.Parameter()

    def requires(self):
        if self.step == "inversion":
            return IntegrationStep(
                integration_name=self.integration_name,
                step="processing")
        elif self.step == "processing":
            return IntegrationStep(
                integration_name=self.integration_name,
                step="import")
        else:
            return None
        
    def run(self):
        yield self.subtask()

    def subtask(self):
        if self.step == "inversion":
            return inversion.Inversion(inversion_name=self.integration_name + "/inversion")
        elif self.step == "processing":
            return processing.Processing(processing_name=self.integration_name + "/processing")
        elif self.step == "import":
            return file_import.Import(import_name=self.integration_name + "/import")
        else:
            assert False, 'Unknown step %s' % (self.step,)
            
    def output(self):
        return self.subtask().output()
        
class Integration(luigi.Task):
    integration_name = luigi.Parameter()

    def config_target(self):
        return poltergust_luigi_utils.caching.CachingOpenerTarget('%s/config.yml' % (self.integration_name,))

    def subtask(self):
        return IntegrationStep(
            integration_name=self.integration_name,
            step="inversion")
    
    def run(self):
        subtask = self.subtask()
        inv = subtask.subtask()
        pro = subtask.requires().subtask()
        imp = subtask.requires().requires().subtask()
        
        with self.config_target().open("r") as f:
            config = yaml.load(f, Loader=yaml.SafeLoader)

        print("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA")
        print()
        print()
        print("IMP", imp, imp.config_target().url)
        print("PRO", pro, pro.config_target().url)
        print("INV", inv, inv.config_target().url)
        print()
        print()
        print("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA")
        
        with imp.config_target().open("w") as f:
            yaml.dump(config["importer"], f)
            
        with pro.config_target().open("w") as f:
            config["processing"]["data"] = {
                    "name": "emeraldprocessing.pipeline.ProcessingData",
                    "args": {
                        "data": imp.data().url,
                        "sidecar": None,
                        "system_data": imp.system_data().url,
                    }
            }
            yaml.dump(config["processing"], f)

        with inv.config_target().open("w") as f:
            config["inversion"]["data"] = pro.data().url
            config["inversion"]["system_data"] = pro.system_data().url
            yaml.dump(config["inversion"], f)

        yield subtask

    def output(self):
        return self.subtask().output()
