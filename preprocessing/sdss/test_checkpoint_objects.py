import os
import random
import unittest
from datetime import datetime

import numpy as np

from preprocessing.sdss.checkpoint_objects import CheckPointer, RedShiftCheckPointObject


class TestCheckpoint(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        kwargs = [{
            'key': str(i),
            'np_array': np.random.rand(64, 64, 5),
            'redshift': random.random(),
            'galaxy_meta': {'name': 'this is my name'},
            'image': 'path/to/image',
            'timestamp': datetime.now()
        } for i in range(5)]

        cls.checkpointer = CheckPointer(
            ckpt_dir='ckpt',
            guid='ckptGUID',
            overwrite=True,
            metaclass=RedShiftCheckPointObject,
            meta_objects=kwargs
        )
        cls.last_modified = cls.checkpointer.last_modified

    def test_add_objects_with_same_keys(self):
        kwargs = [{
            'key': str(i),
            'np_array': np.random.rand(64, 64, 5),
            'redshift': random.random(),
            'galaxy_meta': {'name': 'this is my name'},
            'image': 'path/to/image',
            'timestamp': datetime.now()
        } for i in range(4, 10)]
        TestCheckpoint.checkpointer.save_objects(kwargs)

    def test_last_modified(self):
        assert TestCheckpoint.checkpointer.last_modified != TestCheckpoint.last_modified

    def test_get_specific_object(self):
        assert TestCheckpoint.checkpointer.get_specific_object('9').key == '9'

    def test_get_all_objects_info(self):
        assert len(TestCheckpoint.checkpointer.return_all_objects_info()) == 2

    def test_file_not_found(self):
        objects = TestCheckpoint.checkpointer.return_objects_for('f2231d2871e690a2995704f7a297bd7bc64be720.pkl')
        assert len(objects) == 5
        assert type(objects) == dict
        assert [isinstance(a_member, RedShiftCheckPointObject) for a_member in objects.values()]

if __name__ == '__main__':
    unittest.main()
