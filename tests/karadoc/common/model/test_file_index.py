import unittest

from karadoc.common.model import file_index


class FileIndex(unittest.TestCase):
    def test_is_hidden(self):
        self.assertTrue(file_index._is_hidden(folder="/_hidden"))
        self.assertTrue(file_index._is_hidden(folder="/.hidden"))
        self.assertFalse(file_index._is_hidden(folder="/file"))
        self.assertTrue(file_index._is_hidden(folder="/base_dir/folder/_hidden/file"))
        self.assertTrue(file_index._is_hidden(folder="/base_dir/folder/.hidden/file"))
        self.assertFalse(file_index._is_hidden(folder="/base_dir/folder/file"))
