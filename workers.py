import threading
from abc import ABC, abstractmethod

import requests

DEFAULT_BLOCK_SIZE = 1024 * 512


class Worker(threading.Thread, ABC):
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
        for b in self.remaining_blocks:
            yield b, b * DEFAULT_BLOCK_SIZE, min(self.content_length, (b + 1) * DEFAULT_BLOCK_SIZE)

    @property
    @abstractmethod
    def blocks(self):
        pass

    def run(self):
        with open(self.file_name, 'r+b') as f:
            for i, start, end, b in self.blocks:
                f.seek(start)
                f.write(b)
                self.block_map[i] = True


class ContentWorker(Worker):
    def __init__(self, url, file_name, content_length, block_map):
        for i in range(len(block_map)):
            block_map[i] = False
        remaining_blocks = list(range(len(block_map)))
        super().__init__(url, file_name, content_length, block_map, remaining_blocks)

    @property
    def blocks(self):
        r = self.session.get(self.url, stream=True)
        if r.status_code != requests.codes.ok:
            raise RuntimeError()
        buffer = bytearray(DEFAULT_BLOCK_SIZE)
        num = 0
        for block_id, start, end in self.ranges:
            block_size = end - start
            for content in r.iter_content(block_size):
                if num + len(content) < block_size:
                    buffer[num:num + len(content)] = content
                    num += len(content)
                else:
                    remain = num + len(content) - block_size
                    buffer[num:] = content[:len(content) - remain]
                    yield block_id, start, end, buffer[:block_size]
                    num = remain
                    break


class RangeWorker(Worker):
    def __init__(self, url, file_name, content_length, block_map, remaining_blocks):
        super().__init__(url, file_name, content_length, block_map, remaining_blocks)

    @property
    def blocks(self):
        buffer = bytearray(DEFAULT_BLOCK_SIZE)
        for block_id, start, end in self.ranges:
            r = self.session.get(self.url, stream=True,
                                 headers={'Range': f'bytes={start}-{end - 1}',
                                          'Content-Encoding': 'identity'})
            if r.status_code != requests.codes.partial_content:
                raise RuntimeError()
            block_size = end - start
            num = 0
            for content in r.iter_content(block_size):
                if num + len(content) < block_size:
                    buffer[num:num + len(content)] = content
                    num += len(content)
                else:
                    remain = num + len(content) - block_size
                    buffer[num:] = content[:len(content) - remain]
                    yield block_id, start, end, buffer[:block_size]
