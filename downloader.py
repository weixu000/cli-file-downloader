import requests
import argparse
import urllib.parse
import os.path
import time
import os

import workers


def create_file(file_name, size):
    """
    Create file of specified size
    Discard existing file
    """
    with open(file_name, 'wb') as f:
        if size:
            f.seek(size - 1)
            f.write(b'\x00')


def get_metadata(url):
    """
    Send HEAD request to get infomation of the file
    """
    headers = requests.head(url).headers
    content_length = int(headers['Content-Length'])
    accept_ranges = headers.get('Accept-Ranges', 'none') == 'bytes'
    return content_length, accept_ranges


def download_url(url, num_threads, resume, download_to):
    url_components = urllib.parse.urlparse(url)
    file_path = os.path.basename(urllib.parse.unquote(url_components.path)) or 'index.html'
    file_path = os.path.join(download_to, file_path)
    print(f'Trying to download {url} to {os.path.abspath(file_path)}')

    if os.path.exists(file_path):
        print('File exists already, delete it to redownload')
        return

    print('Fetching metadata of the file')
    try:
        content_length, accept_ranges = get_metadata(url)
    except requests.exceptions.RequestException as e:
        print(e)
        print("Error while get metadata, check URL and try again later")
        return

    if resume and not accept_ranges:
        print('HTTP range requests not supported, ignore -c')
        resume = False

    partial_file_path = f'{file_path}.partial'  # Use another file to append block map

    num_blocks = workers.get_num_blocks(content_length)
    print(f'Split file into {num_blocks} blocks')

    if resume:
        try:
            block_map = workers.get_block_map(partial_file_path, content_length, num_blocks)
        except:
            print("Cannot read partial file, ignore -c")
            resume = False
    if not resume:
        create_file(partial_file_path, content_length)
        block_map = [False] * num_blocks

    print('Downloading file')
    if accept_ranges:
        # Assign equal number of unfinished blocks to each worker
        threads = []
        for blocks in workers.split_remaining_blocks(block_map, num_threads):
            t = workers.RangeWorker(url, partial_file_path, content_length, block_map, blocks)
            threads.append(t)
    else:
        threads = [workers.WholeWorker(url, partial_file_path, content_length, block_map)]
    try:
        # Display the progress in main thread
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
        if all(block_map):
            print('Finished downloading')
            os.truncate(partial_file_path, content_length)  # Remove block map
            os.replace(partial_file_path, file_path)  # Rename
        else:
            print('Append data to resume')
            workers.set_block_map(partial_file_path, content_length, block_map)


def main():
    parser = argparse.ArgumentParser(description='Download URL.')
    parser.add_argument('url', metavar='URL', type=str,
                        help='URL to download')
    parser.add_argument('-c', dest='resume', action='store_true',
                        help='resume previous downloading')
    parser.add_argument('-n', dest='num_threads', type=int, default=1,
                        help='threads to download with in parallel')
    args = parser.parse_args()

    download_url(args.url, args.num_threads, args.resume, os.curdir)


if __name__ == '__main__':
    main()
