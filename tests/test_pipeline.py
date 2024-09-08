import unittest
import tempfile
import os
import yaml
import luigi
import poltergust_luigi_utils.caching

import beryl_pipeline.introspect
import beryl_pipeline.file_import
import beryl_pipeline.processing
import beryl_pipeline.inversion
import beryl_pipeline.integration

datadir = "file://" + os.path.join(os.path.dirname(__file__), "data")

update_results = False

class Base(unittest.TestCase):
    def setUp(self):
        self.cachedir = tempfile.TemporaryDirectory()
        poltergust_luigi_utils.caching.cachedir = self.cachedir.__enter__()
        self.tmpdir = tempfile.TemporaryDirectory()
        root = self.tmpdir.__enter__()
        if update_results:
            self.root = datadir
        else:
            self.root = root

    def tearDown(self):
        self.cachedir.__exit__(None, None, None)
        self.tmpdir.__exit__(None, None, None)

class Pipeline(Base):
    def test_import(self):
        with poltergust_luigi_utils.caching.CachingOpenerTarget(self.root + "/import/config.yml").open("w") as f:
            yaml.dump({
                "importer": {
                    "name": "SkyTEM XYZ",
                    "args": {
                        "files": {
                            "xyzfile": datadir + "/aem_processed_data_foothill_central_valley.100101.0.xyz",
                            "gexfile": datadir + "/20201231_20023_IVF_SkyTEM304_SKB.gex"
                        },
                        "projection": 32611 # Not 100% sure this is correct; the dataset does not specify...
                    }
                }
            }, f)

        import_task = beryl_pipeline.file_import.Import(import_name = self.root + "/import")
        luigi.build([import_task], local_scheduler=True)
        
        assert import_task.logfile().exists(), "Logfile not written"
        assert import_task.data().exists(), "Data not written"
        assert import_task.system_data().exists(), "System data not written"
        assert import_task.data_msgpack().exists(), "Msgpack binary not written"
        assert import_task.output().exists(), "Done marker file not written"

    def test_processing(self):
        with poltergust_luigi_utils.caching.CachingOpenerTarget(self.root + "/processing/config.yml").open("w") as f:
            yaml.dump({
                "steps": [],
                "data": {
                    "name": "emeraldprocessing.pipeline.ProcessingData",
                    "args": {
                        "data": datadir + "/import/out.xyz",
                        "sidecar": None,
                        "system_data": datadir + "/import/out.gex",
                    }
                }
            }, f)
    
        processing_task = beryl_pipeline.processing.Processing(processing_name = self.root + "/processing")
        luigi.build([processing_task], local_scheduler=True)

        for name in ['log.yml',
                     'processed.msgpack',
                     'processed.gex',
                     'processed.xyz',
                     'DONE']:
            assert poltergust_luigi_utils.caching.CachingOpenerTarget(os.path.join(processing_task.processing_name, name)).exists()

    def test_inversion(self):
        with luigi.contrib.opener.OpenerTarget(self.root + "/inversion/config.yml").open("w") as f:
            yaml.dump({
                "system": {
                    "name": "Dual moment TEM",
                    "args": {
                        "directives__irls": True,
                        "optimizer__max_iter": 4, # Don't make a good model, that takes way too long time
                        "optimizer__max_iter_cg": 2
                    }
                },
                "data": datadir + "/processing/processed.xyz",
                "system_data": datadir + "/processing/processed.gex",
            }, f)

        inversion_task = beryl_pipeline.inversion.Inversion(inversion_name = self.root + "/inversion")
        luigi.build([inversion_task], local_scheduler=True)

        assert luigi.contrib.opener.OpenerTarget(self.root + "/inversion/processed.xyz").exists(), "Corrected data not writen"
        assert luigi.contrib.opener.OpenerTarget(self.root + "/inversion/smooth_model.xyz").exists(), "L2 model not writen"
        assert luigi.contrib.opener.OpenerTarget(self.root + "/inversion/smooth_synthetic.xyz").exists(), "L2 synthetic data not writen"
        assert inversion_task.output().exists(), "Done marker file not written"

    def test_introspection(self):
        introspect_task = beryl_pipeline.introspect.Introspect(introspect_name = self.root + "/introspect")
        luigi.build([introspect_task], local_scheduler=True)
        with introspect_task.output().open("r") as f:
            introspection = yaml.load(f, Loader=yaml.SafeLoader)

        assert len(introspection["Import"]["anyOf"]) > 0, "No importers available"
        assert len(introspection["Processing"]["items"]["anyOf"]) > 0, "No processing pipeline steps available"
        assert len(introspection["Inversion"]["anyOf"]) > 0, "No inversion kernels available"
        
class PipelineIntegration(Base):
    def test_integration(self):
        with poltergust_luigi_utils.caching.CachingOpenerTarget(self.root + "/integration/config.yml").open("w") as f:
            yaml.dump({
                "importer": {
                    "importer": {
                        "name": "SkyTEM XYZ",
                        "args": {
                            "files": {
                                "xyzfile": datadir + "/aem_processed_data_foothill_central_valley.100101.0.xyz",
                                "gexfile": datadir + "/20201231_20023_IVF_SkyTEM304_SKB.gex"
                            },
                            "projection": 32611 # Not 100% sure this is correct; the dataset does not specify...
                        }
                    }
                },
                "processing": {
                    "steps": []
                },
                "inversion": {
                    "system": {
                        "name": "Dual moment TEM",
                        "args": {
                            "directives__irls": True,
                            "optimizer__max_iter": 4, # Don't make a good model, that takes way too long time
                            "optimizer__max_iter_cg": 2
                        }
                    }
                }
            }, f)

        import_task = beryl_pipeline.integration.Integration(integration_name = self.root + "/integration")
        luigi.build([import_task], local_scheduler=True)

if __name__ == '__main__':
    import nose2
    nose2.main()  
