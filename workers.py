import threading
from abc import ABC, abstractmethod

import requests

from blockmap import DEFAULT_BLOCK_SIZE

RETRY = 5


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


def iter_lines(response: requests.Response):
    """
    Wrap over requests.Response.iter_content
    Return one line without \r\n
    """
    buffer = b''
    while buffer[-2:] != b'\r\n':
        buffer += iter_content(response, 1)
    return buffer[:-2]


class Worker(threading.Thread, ABC):
    """
    Base class for workers to download and write to disk
    """

    def __init__(self, url, file_name, content_length, block_map, remaining_blocks):
        super().__init__()
        self.session = requests.Session()
        self.url = url
        self.file_name = file_name
        self.block_map = block_map
        self.content_length = content_length
        self.remaining_blocks = remaining_blocks
        self.stop_event = threading.Event()

    def stop(self):
        """
        Stop the worker
        Keep self.block_map consistent
        """
        self.stop_event.set()

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
                if self.stop_event.wait(0):
                    break  # stop downloading
                f.seek(start)
                f.write(b)
                self.block_map[i] = True


class WholeWorker(Worker):
    def __init__(self, url, file_name, content_length, block_map):
        # Unset the whole block map since we cannot resume
        # Block map here is only to show the progress
        for i in range(len(block_map)):
            block_map[i] = False
        remaining_blocks = list(range(len(block_map)))
        super().__init__(url, file_name, content_length, block_map, remaining_blocks)

    @property
    def blocks(self):
        """
        Request the whole file
        """
        for i_retry in range(RETRY):
            try:
                r = self.session.get(self.url, stream=True)
                r.raise_for_status()
                for block_id, start, end in self.ranges:
                    block = iter_content(r, end - start)
                    yield block_id, start, end, block
            except:
                print(f'{self.ident} retrying {i_retry + 1} times')
                # Restart from scratch
            else:
                break  # Download finished
        else:
            print(f'{self.ident} retry failed')


class RangeWorker(Worker):
    def __init__(self, url, file_name, content_length, block_map, remaining_blocks):
        super().__init__(url, file_name, content_length, block_map, remaining_blocks)

    @property
    def blocks(self):
        """
        Request multiples ranges of the file
        """
        ranges = list(self.ranges)
        i_ranges = 0  # ranges to be download
        for i_retry in range(RETRY):
            try:
                if i_ranges == len(ranges): break
                range_header = 'bytes=' + ', '.join(f'{start}-{end - 1}' for _, start, end in ranges[i_ranges:])
                r = self.session.get(self.url, stream=True,
                                     headers={'Range': range_header,
                                              'Content-Encoding': 'identity'})
                r.raise_for_status()
                if i_ranges == len(ranges) - 1:
                    # single range response
                    block_id, start, end = ranges[i_ranges]
                    block = iter_content(r, end - start)
                    yield block_id, start, end, block
                    i_ranges += 1
                else:
                    # multipart response
                    content_type, boundary = r.headers['Content-Type'].split('; boundary=')
                    while i_ranges < len(ranges):
                        block_id, start, end = ranges[i_ranges]
                        while iter_lines(r) != b'--' + boundary.encode():  # find the next part
                            pass
                        while iter_lines(r) != b'':
                            pass  # ignore header of this part
                        block = iter_content(r, end - start)
                        yield block_id, start, end, block
                        i_ranges += 1
            except:
                print(f'{self.ident} retrying {i_retry + 1} times')
                # Start from i_ranges
            else:
                break  # Download finished
        else:
            print(f'{self.ident} retry failed')
