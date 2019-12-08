import hashlib
import unittest
import tempfile
import os
import random

import downloader
import workers


def file_SHA256(file_name, block_size=4096):
    sha256_hash = hashlib.sha256()
    with open(file_name, "rb") as f:
        # Read and update hash string value in blocks of 4K
        for byte_block in iter(lambda: f.read(block_size), b''):
            sha256_hash.update(byte_block)
    return sha256_hash.hexdigest()


class TestBlockMap(unittest.TestCase):
    def test_load_block_map(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            output_file = os.path.join(temp_dir, 'test.data')
            file_size = random.randrange(100)
            block_map = [bool(random.randrange(2)) for _ in range(random.randrange(1, 100))]
            with open(output_file, 'wb') as f:
                f.write(b'\x00' * file_size)
                f.write(bytes(block_map))
            ret = workers.get_block_map(output_file, file_size, len(block_map))
            self.assertEqual(ret, block_map)

    def test_split_blocks(self):
        num_workers = random.randrange(1, 100)
        block_map = [bool(random.randrange(2)) for _ in range(num_workers * random.randrange(1, 100))]

        splits = list(workers.split_remaining_blocks(block_map, num_workers))
        self.assertEqual(sum(splits, []), [i for i, b in enumerate(block_map) if not b])


URL = 'https://repo.anaconda.com/miniconda/Miniconda3-latest-Windows-x86_64.exe'
FILE_NAME = 'Miniconda3-latest-Windows-x86_64.exe'
SHA256 = 'f18060cc0bb50ae75e4d602b7ce35197c8e31e81288d069b758594f1bb46ab45'


class TestWorker(unittest.TestCase):
    def test_iter_content(self):
        block_size = random.randrange(1, 1024)
        content_length = random.randrange(1000) * block_size
        content = bytes(random.randrange(256) for _ in range(content_length))

        class DummyClass:
            def __init__(self):
                self.next_ind = 0

            def iter_content(self, chunk_size):
                while self.next_ind < content_length:
                    new_ind = self.next_ind + random.randrange(chunk_size + 1)
                    ret, self.next_ind = content[self.next_ind:new_ind], new_ind
                    yield ret

        dummy = DummyClass()

        ret = b''
        for i in range(content_length // block_size):
            block = workers.iter_content(dummy, block_size)
            self.assertEqual(len(block), block_size)
            ret += block
        self.assertEqual(content, ret)

    def test_range_walker(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            content_length, _ = downloader.get_metadata(URL)
            file_name = os.path.join(temp_dir, FILE_NAME)
            downloader.create_file(file_name, content_length)
            num_blocks = workers.get_num_blocks(content_length)
            t = workers.RangeWorker(URL, file_name, content_length,
                                    [False] * num_blocks, list(range(num_blocks)))
            t.run()
            self.assertEqual(file_SHA256(file_name), SHA256)

    def test_content_walker(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            content_length, _ = downloader.get_metadata(URL)
            file_name = os.path.join(temp_dir, FILE_NAME)
            downloader.create_file(file_name, content_length)
            num_blocks = workers.get_num_blocks(content_length)
            t = workers.ContentWorker(URL, file_name, content_length, [False] * num_blocks)
            t.run()
            self.assertEqual(file_SHA256(file_name), SHA256)


class TestDownloader(unittest.TestCase):
    def test_multithreaded(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            downloader.download_url(URL, 5, False, temp_dir)
            self.assertEqual(file_SHA256(os.path.join(temp_dir, FILE_NAME)), SHA256)


if __name__ == '__main__':
    unittest.main()