import luigi
import luigi.contrib.opener
import luigi.format
import yaml
import poltergust_luigi_utils.caching
import swaggerspect

class Introspect(luigi.Task):
    introspect_name = luigi.Parameter()

    def run(self):
        # Hack to add save_iterations flag added to all inversion systems by the pipeline code
        inversion = swaggerspect.swagger_to_json_schema(swaggerspect.get_apis("simpeg.static_instrument"), multi=False)
        for system in inversion["anyOf"]:
            next(iter(system["properties"].values()))["properties"]["save_iterations"] = {
                "type": "boolean",
                "default": False,
                "description": "Save intermediate models and sythentic data for every inversion iteration"
            }
        
        data = {
            "Import": swaggerspect.swagger_to_json_schema(swaggerspect.get_apis("beryl_pipeline.import"), multi=False),
            "Processing": swaggerspect.swagger_to_json_schema(swaggerspect.get_apis("emeraldprocessing.pipeline_step")),
            "Inversion": inversion,
        }
        
        with self.output().open("w") as f:
            yaml.dump(data, f, sort_keys=False)
            
    def output(self):
         return poltergust_luigi_utils.caching.CachingOpenerTarget('%s/output.yml' % (self.introspect_name,))
    
