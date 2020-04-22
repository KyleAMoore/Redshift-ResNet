import pickle
import json
import os
from pathlib import Path
import shutil
from datetime import datetime
import time

from dataclasses import dataclass

import numpy as np

from sdss_utils import get_hash
from logger_factory import LoggerFactory


@dataclass
class RedShiftCheckPointObject:
    """Checkpoint object for saving redshift data for galaxies"""
    key: str
    np_array: np.ndarray
    redshift: float
    galaxy_meta: dict
    image: str
    timestamp: datetime


class CheckPointer:
    """A generic checkpointer

    This class provides a basic checkpoint support for saving sdss images.
    The objects are saved to disk using pickle.

    Parameters
    ----------
    ckpt_dir : str or Path like
        The directory in which checkpoints are to be created
    guid : str
        The global unique identifier for this checkpoint, all the checkpoint files
        and metadta will be saved in a directory named `guid` inside the `ckpt_dir`
    metaclass : type
        The python class of which objects are created and saved in memory for
    overwrite : bool, default=False
        If True, overwrite the existing checkpoint files saved on disk
    meta_objects : list of dict or `metaclass` instances, default=None
        A list of meta_objects to initialize the checkpoint with
    max_objects : int, default=1000
        Maximum objects threshold for pickling

    Attributes
    ----------
    pkl_dir : Path
        The directory to save pickle files
    objs_in_disk : Path
        The file which saves information about objects in disk
    metadata_file : Path
        The file which saves information about each checkpoint step
    """
    def __init__(self,
                 ckpt_dir,
                 guid,
                 metaclass,
                 overwrite=False,
                 meta_objects=None,
                 max_objects=1000):
        self.guid = guid
        ckpt_path = Path(ckpt_dir).resolve()

        if not ckpt_path.exists():
            os.makedirs(ckpt_path)

        self.pkl_dir = (ckpt_path / guid)
        self.objs_in_disk = ckpt_path / f'{guid}.txt'
        self.metadata_file = ckpt_path / guid / 'metadata.json'
        self.logger = LoggerFactory.get_logger(self.__class__.__name__,
                                               level=10)
        if overwrite:
            self.logger.debug('Overwriting checkpoint files if they exist')
            shutil.rmtree(self.pkl_dir, ignore_errors=True)
            if self.objs_in_disk.exists():
                os.remove(self.objs_in_disk)

        os.makedirs(self.pkl_dir, exist_ok=True)

        if self.objs_in_disk.exists():
            with self.objs_in_disk.open('r') as objs_file:
                self.obj_set = set(objs_file.read().split(','))
            self.last_modified = os.path.getmtime(self.objs_in_disk)
        else:
            self.obj_set = set()
            self.last_modified = time.time()

        self.metaclass = metaclass
        self.max_objects = max_objects
        self.logger.debug(f'The checkpoints were last modified at '
                          f'{time.strftime("%b %d %Y %H:%M:%S", time.localtime(self.last_modified))}')
        self.save_objects(meta_objects)

    def save_objects(self, meta_objects=None):
        """Save checkpoint objects and metadata

        Parameters
        ----------
        meta_objects : list of dict or instances of metaclass
            List of
        """
        if meta_objects is not None:
            keys_to_save = []
            objects_to_save = {}
            if len(meta_objects) > self.max_objects:
                raise ValueError(f'Cannot save more than {self.max_objects} for pickling.')

            for obj_or_kwargs in meta_objects:
                current_object = obj_or_kwargs

                if isinstance(current_object, dict):
                    current_object = self.metaclass(**obj_or_kwargs)

                if not isinstance(current_object, self.metaclass):
                    raise TypeError(f'Cannot save object of type {type(current_object)}')

                if current_object.key not in self.obj_set:
                    self.logger.debug(f'{current_object.key} added to save objects list')
                    objects_to_save[current_object.key] = current_object
                    keys_to_save.append(current_object.key)
                else:
                    self.logger.info(f'{current_object.key} already saved in disk. Skipping...')

            if len(objects_to_save) > 0:
                self._save_pkl(objects_to_save)
                self._update_objects_on_disk(keys_to_save)
                self.last_modified = time.time()
                self.logger.debug(f'Checkpoint update complete. Saving modification time to '
                                  f'{time.strftime("%b %d %Y %H:%M:%S", time.localtime(self.last_modified))}')

    def _save_pkl(self, objects_to_save):
        """Save the pickle file and metadata on the pickle files"""
        filename = f'{get_hash(objects_to_save.keys())}.pkl'
        with (self.pkl_dir / filename).open('wb') as pkl_file:
            pickle.dump(objects_to_save, pkl_file)

        if self.metadata_file.exists():
            with self.metadata_file.open('r') as meta_json:
                metadata = json.load(meta_json)
        else:
            metadata = {}

        metadata[filename] = list(objects_to_save.keys())

        with self.metadata_file.open('w') as meta_json:
            json.dump(metadata, meta_json, indent=2)

    def _update_objects_on_disk(self, keys_to_save):
        """Update objects on disk"""
        with self.objs_in_disk.open('a+') as keys_file:
            keys_file.write(','.join(keys_to_save))
            keys_file.write(',')

        self.obj_set.update(set(keys_to_save))

    def get_specific_object(self, obj_key):
        """From the checkpoint dir return an object with specific key

        Parameter
        ---------
        obj_key : str
            A unique key representing the object in the checkpoint

        Returns
        -------
        instance of self.metaclass
           the object of class self.metaclass if it exists

        Raises
        ------
        KeyError
            If the object doesn't exist in the current checkpoints
        """
        if not self.metadata_file.exists():
            raise KeyError(f"{obj_key} doesn't exist in the checkpoints")
        else:
            tgt_file = None
            with self.metadata_file.open('r') as meta_json:
                meta_dict = json.load(meta_json)

            for key, value in meta_dict.items():
                value_set = set(value)
                if obj_key in value_set:
                    tgt_file = key

            if tgt_file is None:
                raise KeyError(f"{obj_key} doesn't exist in the checkpoints")
            else:
                with (self.pkl_dir / tgt_file).open('rb') as objs_pkl:
                    objects_dict = pickle.load(objs_pkl)
                    return objects_dict[obj_key]

    def return_all_objects_info(self):
        """Return a dictionary of metadata on current checkpoint files"""
        if not self.metadata_file.exists():
            return {}

        with self.metadata_file.open('r') as meta_json:
            return json.load(meta_json)

    def return_objects_for(self, checkpoint_file):
        """given a checkpoint pickle filename, return all the objects stored in it"""
        if not (self.pkl_dir / checkpoint_file).exists():
            raise FileNotFoundError(f'Cannot find the checkpoint file {checkpoint_file} '
                                    f'in the current checkpoint directory {self.pkl_dir.parent}')

        with (self.pkl_dir / checkpoint_file).open('rb') as obj_pkl:
            objects = pickle.load(obj_pkl)
            return objects
