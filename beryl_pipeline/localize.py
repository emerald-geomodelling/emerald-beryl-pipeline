import luigi.contrib.opener
import contextlib
import poltergust_luigi_utils.caching
import luigi.format
import shutil
import sys
import os
import tempfile

@contextlib.contextmanager
def localize(config):
    """A contextmanager that given a nested structure of dictionaries and
    lists, finds all embedded URL:s, downloads them, and returns
    the same structure, but with those replaced by the path to the
    downloaded file. The local files are deleted after the context
    manager exits.
    """
    mapping = {}
    def localize(config):
        if isinstance(config, dict):
            return {key: localize(value) for key, value in config.items()}
        elif isinstance(config, (list, tuple)):
            return [localize(value) for value in config]
        elif isinstance(config, str) and "://" in config:
            if "file://" in config:
                return config.split("file://")[1]
            else:
                mapping[config] = poltergust_luigi_utils.caching.CachingOpenerTarget(config, format=luigi.format.NopFormat())
                return mapping[config].__enter__()
        return config
    try:
        yield localize(config)
    finally:
        for key, value in mapping.items():
            value.__exit__()
 
@contextlib.contextmanager
def upload_directory(url):
    with tempfile.TemporaryDirectory() as tempdir:
        yield tempdir
        for name in os.listdir(tempdir):
            path = os.path.join(tempdir, name)
            target = poltergust_luigi_utils.caching.CachingOpenerTarget(
                os.path.join(url, name),
                format=luigi.format.NopFormat())
            with target.open("w") as outf:
                with open(path, "rb") as inf:
                    shutil.copyfileobj(inf, outf)
            
def save_survey(survey, **kw):
    files = {key:
          poltergust_luigi_utils.caching.CachingOpenerTarget(
              url,
              format=luigi.format.NopFormat()
          ).open("w")
          for key, url in kw.items()}
    try:
        survey.dump(**{k: v.__enter__() for k, v in files.items()})
    except Exception as e:
        for key, value in files.items():
            value.__exit__(*sys.exc_info())
        raise
    else:
        for key, value in files.items():
            value.__exit__(None, None, None)
