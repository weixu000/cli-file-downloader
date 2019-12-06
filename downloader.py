import urllib.request
import argparse
import urllib.parse
import os.path
import threading
import time
import os.path
import math
import os

DEFAULT_BLOCK_SIZE = 1024 * 512


class Worker(threading.Thread):
    def __init__(self, url, file_name, content_length, block_map, remaining_blocks):
        super().__init__(daemon=True)
        self.url = url
        self.file_name = file_name
        self.block_map = block_map
        self.content_length = content_length
        self.remaining_blocks = remaining_blocks

    def iter_range(self):
        block_size = self.content_length // len(self.block_map)
        for block in self.remaining_blocks:
            yield block, block * block_size, min(self.content_length, (block + 1) * block_size)

    def run(self):
        with open(self.file_name, 'r+b') as f:
            buffer = bytearray(DEFAULT_BLOCK_SIZE)
            for block, start, end in self.iter_range():
                f.seek(start)
                request = urllib.request.Request(self.url, headers={'Range': f'bytes={start}-{end - 1}'})
                current_size = end - start
                with urllib.request.urlopen(request) as response:
                    num_read = 0
                    while num_read < current_size:
                        current_read = response.readinto(buffer)
                        f.write(buffer[0:current_read])
                        num_read += current_read
                self.block_map[block] = True


def create_file(file_name, content_length):
    with open(file_name, 'wb') as f:
        if content_length:
            f.seek(content_length - 1)
            f.write(b'\x00')


def get_metadata(url):
    head_req = urllib.request.Request(url, method='HEAD')
    with urllib.request.urlopen(head_req) as response:
        headers = dict(response.getheaders())
        content_length = int(headers['Content-Length'])
        content_type = headers['Content-Type']
        accept_ranges = headers.get('Accept-Ranges', 'none') == 'bytes'
    return content_type, content_length, accept_ranges


def download_url(url, num_threads, resume):
    url_components = urllib.parse.urlparse(url)
    file_name = os.path.basename(urllib.parse.unquote(url_components.path)) or 'index.html'
    print(f'Trying to download {url} to {os.path.abspath(file_name)}')

    print('Fetching metadata of the file')
    content_type, content_length, accept_ranges = get_metadata(url)

    if resume and not accept_ranges:
        print('HTTP range request not supported, ignore -c')
        resume = False

    num_blocks = int(math.ceil(content_length / DEFAULT_BLOCK_SIZE))
    print(f'Split file into {num_blocks} blocks')

    if resume and os.path.exists(file_name) and os.path.getsize(file_name) == content_length + num_blocks:
        with open(file_name, 'r+b') as f:
            f.seek(content_length)
            block_map = [bool(b) for b in f.read(num_blocks)]
            print(f'Trying to continue downloading with {sum(block_map)} blocks already finished')
    else:
        print('Creating file')
        create_file(file_name, content_length)
        block_map = [False] * num_blocks

    print('Downloading file')
    num_threads = num_threads if accept_ranges else 1
    remaining_blocks = [i for i, b in enumerate(block_map) if not b]
    blocks_for_worker = int(math.ceil(len(remaining_blocks) / num_threads))
    try:
        start = time.time()
        threads = []
        for i in range(num_threads):
            t = Worker(url, file_name, content_length, block_map,
                       remaining_blocks[i * blocks_for_worker:min(len(block_map), (i + 1) * blocks_for_worker)])
            t.start()
            threads.append(t)
        while any(t.is_alive() for t in threads):
            time.sleep(0.1)
            print(''.join('*' if b else '-' for b in block_map))
        for t in threads:
            t.join()
        print(f'Elapsed {time.time() - start:.2f} secs')
    finally:
        os.truncate(file_name, content_length)
        if all(block_map):
            print('Finished downloading')
        else:
            print('Append data to resume')
            with open(file_name, 'ab') as f:
                f.write(bytes(block_map))


def main():
    parser = argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument('url', metavar='URL', type=str,
                        help='URL to download')
    parser.add_argument('-c', dest='resume', action='store_true',
                        help='resume previous downloading')
    parser.add_argument('-n', dest='num_threads', type=int, default=1,
                        help='threads to download with in parallel')
    args = parser.parse_args()
    print(args)

    download_url(args.url, args.num_threads, args.resume)


if __name__ == '__main__':
    main()
