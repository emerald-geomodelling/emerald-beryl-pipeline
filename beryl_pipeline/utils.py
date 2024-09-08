import yaml
import importlib
import numpy as np
import poltergust_luigi_utils # Add GCS luigi opener
import poltergust_luigi_utils.caching
import os
import importlib.metadata
import contextlib
from . import localize

DB_URL = os.environ.get("DB_URL")

systems = {entry.name: entry for entry in importlib.metadata.entry_points()["simpeg.static_instrument"]}

def load_fn(name):
    mod, fn = name.rsplit(".", 1)
    return getattr(importlib.import_module(mod), fn)

def load_system_from_base(base, system):
    class System(base): pass
    for key, value in system.get("args", {}).items():
        setattr(System, key, value)
    return System

@contextlib.contextmanager
def load_system(system):
    if system["name"].startswith("/"):
        with open(system["name"]) as f:
            system_description = yaml.load(f, Loader=yaml.SafeLoader)
        with localize.localize(system_description) as system_description:
            with load_system(system_description) as base:
                yield load_system_from_base(base, system)
    else:
        yield load_system_from_base(systems[system["name"]].load(), system)

def iter_to_pcnt(x, a=0.5):
    return 100 * a * np.log(1 + x) / (1 + a*np.log(1 + x))

