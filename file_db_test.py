import unittest
import os


from file_db import FileDb, FileDbEntry

class FileDbTest(unittest.TestCase):

    def test_simple(self):
        if os.path.exists('test.bin'):
            raise FileExistsError()
        try:
            db = FileDb('test.bin')
            ent = FileDbEntry('test_dir', db.root)
            ent.set_directory()
            db.append(ent)
            db.close()

            db = FileDb('test.bin')
            dir = db.get_path('test_dir')
            self.assertTrue(dir.is_directory())
        finally:
            os.remove('test.bin')

    def test_append(self):
        if os.path.exists('test.bin'):
            raise FileExistsError()
        try:
            db = FileDb('test.bin')
            ent = FileDbEntry('test.txt', db.root)
            ent.set_directory()
            db.append(ent)
            db.close()

            db = FileDb('test.bin')
            ent = db.get_path('test.txt')
            ent.reset_meta()
            ent.sha256 = b'1234'
            db.append(ent)
            db.close()

            db = FileDb('test.bin')
            ent = db.get_path('test.txt')
            self.assertFalse(ent.is_directory())
            self.assertEqual(ent.sha256, b'1234')
            db.close()
        finally:
            os.remove('test.bin')

    def test_delete(self):
        if os.path.exists('test.bin'):
            raise FileExistsError()
        try:
            db = FileDb('test.bin')
            dir = FileDbEntry('test_dir', db.root)
            dir.set_directory()
            db.append(dir)
            file1 = FileDbEntry('file1.txt', dir)
            file1.sha256 = b'1234'
            db.append(file1)
            file2 = FileDbEntry('file2.txt', dir)
            file2.sha256 = b'4567'
            db.append(file2)
            db.close()

            db = FileDb('test.bin')
            file1 = db.get_path('test_dir/file1.txt')
            self.assertEqual(file1.sha256, b'1234')
            file1.set_removed()
            db.append(file1)
            db.close()

            db = FileDb('test.bin')
            with self.assertRaises(KeyError):
                db.get_path('test_dir/file1.txt')
            file2 = db.get_path('test_dir/file2.txt')
            self.assertEqual(file2.sha256, b'4567')
            file2.set_removed()
            db.append(file2)
            dir = file2.parent
            dir.set_removed()
            db.append(dir)
            db.close()

            db = FileDb('test.bin')
            with self.assertRaises(KeyError):
                db.get_path('test_dir/file1.txt')
            with self.assertRaises(KeyError):
                db.get_path('test_dir/file2.txt')
            with self.assertRaises(KeyError):
                db.get_path('test_dir')
            db.close()
        finally:
            os.remove('test.bin')


if __name__ == '__main__':
    unittest.main()