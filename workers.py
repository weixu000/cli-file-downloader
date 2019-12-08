import math
import os.path
import threading
from abc import ABC, abstractmethod

import requests

DEFAULT_BLOCK_SIZE = 1024 * 512
RETRY = 5


def get_num_blocks(content_length, block_size=DEFAULT_BLOCK_SIZE):
    """
    Split file of specified length into blocks
    """
    return int(math.ceil(content_length / block_size))


def get_block_map(file_name, content_length, num_blocks):
    if os.path.getsize(file_name) != content_length + num_blocks:
        raise RuntimeError("File size does not match")
    with open(file_name, 'r+b') as f:
        f.seek(content_length)
        return [bool(b) for b in f.read(num_blocks)]


def split_remaining_blocks(block_map, num_workers):
    """
    Find unfinished blocks and split into equal shares
    """
    remaining_blocks = [i for i, b in enumerate(block_map) if not b]
    blocks_each = int(math.ceil(len(remaining_blocks) / num_workers))
    for i in range(num_workers):
        yield remaining_blocks[i * blocks_each:min(len(block_map), (i + 1) * blocks_each)]


def iter_content(response: requests.Response, size):
    """
    Wrap over requests.Response.iter_content
    Return specific amount of data
    """
    buffer = bytearray(size)
    num = 0
    try:
        while num < size:
            content = next(response.iter_content(size - num))
            buffer[num:num + len(content)] = content
            num += len(content)
    except StopIteration:
        raise RuntimeError('Content not long enough')
    return buffer


class Worker(threading.Thread, ABC):
    """
    Base class for workers to download and write to disk
    """

    def __init__(self, url, file_name, content_length, block_map, remaining_blocks):
        super().__init__(daemon=True)
        self.session = requests.Session()
        self.url = url
        self.file_name = file_name
        self.block_map = block_map
        self.content_length = content_length
        self.remaining_blocks = remaining_blocks

    @property
    def ranges(self):
        """
        Iterate over blocks (id, start, end)
        Different from HTTP ranges since end here is exclusive
        """
        for b in self.remaining_blocks:
            yield b, b * DEFAULT_BLOCK_SIZE, min(self.content_length, (b + 1) * DEFAULT_BLOCK_SIZE)

    @property
    @abstractmethod
    def blocks(self):
        """
        Iterate over downloading blocks (id, start, end, data)
        Handle connection here
        """
        pass

    def run(self):
        """
        Download each block, write it to the file and set block map
        """
        with open(self.file_name, 'r+b') as f:
            for i, start, end, b in self.blocks:
                f.seek(start)
                f.write(b)
                self.block_map[i] = True


class ContentWorker(Worker):
    def __init__(self, url, file_name, content_length, block_map):
        # Unset the whole block map since we cannot resume
        # Block map here is only to show the progress
        for i in range(len(block_map)):
            block_map[i] = False
        remaining_blocks = list(range(len(block_map)))
        super().__init__(url, file_name, content_length, block_map, remaining_blocks)

    @property
    def blocks(self):
        # Request the whole file
        for i_retry in range(RETRY):
            try:
                r = self.session.get(self.url, stream=True)
                if r.status_code != requests.codes.ok:
                    raise RuntimeError()
                for block_id, start, end in self.ranges:
                    block = iter_content(r, end - start)
                    yield block_id, start, end, block
            except:
                print(f'{self.ident} retrying {i_retry + 1} times')
            else:
                break
        else:
            print(f'{self.ident} retry failed')


class RangeWorker(Worker):
    def __init__(self, url, file_name, content_length, block_map, remaining_blocks):
        super().__init__(url, file_name, content_length, block_map, remaining_blocks)

    @property
    def blocks(self):
        # Request specific range of the file
        for block_id, start, end in self.ranges:
            for i_retry in range(RETRY):
                try:
                    r = self.session.get(self.url, stream=True,
                                         headers={'Range': f'bytes={start}-{end - 1}',
                                                  'Content-Encoding': 'identity'})
                    if r.status_code != requests.codes.partial_content:
                        continue
                    block = iter_content(r, end - start)
                    yield block_id, start, end, block
                    break
                except:
                    print(f'{self.ident} retrying {i_retry + 1} times')
            else:
                print(f'{self.ident} retry failed')
