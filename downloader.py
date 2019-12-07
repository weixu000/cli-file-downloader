import requests
import argparse
import urllib.parse
import os.path
import time
import os.path
import math
import os

import workers


def create_file(file_name, content_length):
    with open(file_name, 'wb') as f:
        if content_length:
            f.seek(content_length - 1)
            f.write(b'\x00')


def get_metadata(url):
    headers = requests.head(url).headers
    content_length = int(headers['Content-Length'])
    accept_ranges = headers.get('Accept-Ranges', 'none') == 'bytes'
    return content_length, accept_ranges


def download_url(url, num_threads, resume):
    url_components = urllib.parse.urlparse(url)
    file_name = os.path.basename(urllib.parse.unquote(url_components.path)) or 'index.html'
    print(f'Trying to download {url} to {os.path.abspath(file_name)}')

    print('Fetching metadata of the file')
    content_length, accept_ranges = get_metadata(url)

    if resume and not accept_ranges:
        print('HTTP range request not supported, ignore -c')
        resume = False

    num_blocks = int(math.ceil(content_length / workers.DEFAULT_BLOCK_SIZE))
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
    accept_ranges = False
    if accept_ranges:
        num_threads = num_threads if accept_ranges else 1
        remaining_blocks = [i for i, b in enumerate(block_map) if not b]
        blocks_for_worker = int(math.ceil(len(remaining_blocks) / num_threads))
        threads = []
        for i in range(num_threads):
            t = workers.RangeWorker(url, file_name, content_length, block_map,
                                    remaining_blocks[
                                    i * blocks_for_worker:min(len(block_map), (i + 1) * blocks_for_worker)])
            threads.append(t)
    else:
        threads = [workers.ContentWorker(url, file_name, content_length, block_map)]
    try:
        start = time.time()
        for t in threads:
            t.start()
        while any(t.is_alive() for t in threads):
            print(''.join('*' if b else '-' for b in block_map))
            time.sleep(0.5)
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
