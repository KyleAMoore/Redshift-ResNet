import os
import shutil
import tempfile


def zip_dir(base_name, base_dir=None, format='tar'):
    if base_dir is None:
        base_dir = os.getcwd()
    base_path = os.path.join(tempfile.TemporaryDirectory(), base_name)
    archive_name = shutil.make_archive(base_name=base_path,
                                       root_dir=base_dir,
                                       format=format,
                                       dry_run=False)
    return archive_name
