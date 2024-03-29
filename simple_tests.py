import hashlib
import unittest
import tempfile
import os
import random
import multiprocessing
import time
import signal

import blockmap
import downloader
import workers


def file_SHA256(file_name, block_size=4096):
    """
    Return SHA256 of a file
    """
    sha256_hash = hashlib.sha256()
    with open(file_name, "rb") as f:
        # Read and update hash string value in blocks of 4K
        for byte_block in iter(lambda: f.read(block_size), b''):
            sha256_hash.update(byte_block)
    return sha256_hash.hexdigest()


class TestBlockMap(unittest.TestCase):
    def test_load_block_map(self):
        """
        Test workers.get_block_map with correct file
        """
        with tempfile.TemporaryDirectory() as temp_dir:
            output_file = os.path.join(temp_dir, 'test.data')
            file_size = random.randrange(100)
            block_map = [bool(random.randrange(2)) for _ in range(random.randrange(1, 100))]
            with open(output_file, 'wb') as f:
                f.write(b'\x00' * file_size)
                f.write(bytes(block_map))
            ret = blockmap.get_block_map(output_file, file_size, len(block_map))
            self.assertEqual(ret, block_map)

    def test_load_block_map_corrupted(self):
        """
        Test workers.get_block_map with incorrect file
        """
        with tempfile.TemporaryDirectory() as temp_dir:
            output_file = os.path.join(temp_dir, 'test.data')
            file_size = random.randrange(100)
            block_map = [bool(random.randrange(2)) for _ in range(random.randrange(1, 100))]
            with self.assertRaises(Exception):  # Nonexistent file
                blockmap.get_block_map(output_file, file_size, len(block_map))

            with open(output_file, 'wb') as f:  # Unfinished file
                f.write(b'\x00' * file_size)
                # No block map
            with self.assertRaises(Exception):
                blockmap.get_block_map(output_file, file_size, len(block_map))

    def test_split_blocks(self):
        """
        Test workers.split_remaining_blocks
        """
        num_workers = random.randrange(1, 100)
        block_map = [bool(random.randrange(2)) for _ in range(num_workers * random.randrange(1, 100))]

        splits = list(blockmap.split_remaining_blocks(block_map, num_workers))
        self.assertEqual(sum(splits, []), [i for i, b in enumerate(block_map) if not b])


# Use files on the web
# Don't think it's a good idea
TEST_FILES = [
    ('https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh',
     'bfe34e1fa28d6d75a7ad05fd02fa5472275673d5f5621b77380898dee1be15d2'),
    ('https://nodejs.org/dist/v13.3.0/node-v13.3.0-linux-x64.tar.gz',
     '155b0510732d2f48150dc6bc4b25eb44ce5cd54d21c70d2ca7f31be3b9ab7fa6')
]


class TestWorker(unittest.TestCase):
    def test_iter_content(self):
        """
        Test workers.iter_content
        """
        block_size = random.randrange(1, 1024)
        content_length = random.randrange(1000) * block_size
        content = bytes(random.randrange(256) for _ in range(content_length))

        class DummyClass:
            """
            Stub for requests.Response
            """

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
        """
        Test workers.RangeWorker
        """
        with tempfile.TemporaryDirectory() as temp_dir:
            for URL, SHA256 in TEST_FILES:
                content_length, _ = downloader.get_metadata(URL)
                file_path = os.path.join(temp_dir, downloader.get_file_name(URL))
                downloader.create_file(file_path, content_length)
                num_blocks = blockmap.get_num_blocks(content_length)
                t = workers.RangeWorker(URL, file_path, content_length,
                                        [False] * num_blocks, list(range(num_blocks)))
                t.run()
                self.assertEqual(file_SHA256(file_path), SHA256)

    def test_whole_walker(self):
        """
        Test workers.WholeWorker
        """
        with tempfile.TemporaryDirectory() as temp_dir:
            for URL, SHA256 in TEST_FILES:
                content_length, _ = downloader.get_metadata(URL)
                file_path = os.path.join(temp_dir, downloader.get_file_name(URL))
                downloader.create_file(file_path, content_length)
                num_blocks = blockmap.get_num_blocks(content_length)
                t = workers.WholeWorker(URL, file_path, content_length, [False] * num_blocks)
                t.run()
                self.assertEqual(file_SHA256(file_path), SHA256)


class TestDownloader(unittest.TestCase):
    def test_download(self):
        """
        Test downloader.download_url from start
        """
        with tempfile.TemporaryDirectory() as temp_dir:
            for URL, SHA256 in TEST_FILES:
                p = multiprocessing.Process(target=downloader.download_url, args=(URL, 5, False, temp_dir))
                p.start()
                p.join()
                self.assertEqual(file_SHA256(os.path.join(temp_dir, downloader.get_file_name(URL))), SHA256)

    def test_resume_download(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            for URL, SHA256 in TEST_FILES:
                p = multiprocessing.Process(target=downloader.download_url, args=(URL, 5, True, temp_dir))
                p.start()
                time.sleep(5)
                os.kill(p.pid, signal.SIGINT)  # not in windows
                p.join()
                p = multiprocessing.Process(target=downloader.download_url, args=(URL, 5, True, temp_dir))
                p.start()
                p.join()
                self.assertEqual(file_SHA256(os.path.join(temp_dir, downloader.get_file_name(URL))), SHA256)


if __name__ == '__main__':
    unittest.main()
