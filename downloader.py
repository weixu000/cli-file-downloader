import urllib.request
import argparse
import urllib.parse
import os.path
import threading
import shutil
import time


def create_file(file_name, content_length):
    assert content_length > 0
    with open(file_name, 'wb') as f:
        f.seek(content_length - 1)
        f.write(b'\x00')


def download_chunck(url, file_name, content_length, num_chuncks, i_chunck):
    chunck_size = content_length // num_chuncks
    range_start, range_end = i_chunck * chunck_size, min(content_length, (i_chunck + 1) * chunck_size) - 1
    request = urllib.request.Request(url, headers={'Range': f'bytes={range_start}-{range_end}'})
    with open(file_name, 'r+b') as f, urllib.request.urlopen(request) as response:
        f.seek(range_start)
        shutil.copyfileobj(response, f)


def download_url(url, num_threads, resume):
    url_components = urllib.parse.urlparse(url)
    basename = os.path.basename(urllib.parse.unquote(url_components.path)) or 'index.html'
    print(f'Trying to download {basename} from {url_components.netloc}')

    print('Fetching metadata of the file')
    head_req = urllib.request.Request(url, method='HEAD')
    with urllib.request.urlopen(head_req) as response:
        headers = dict(response.getheaders())
        print(headers)
        content_length = int(headers['Content-Length'])
        content_type = headers['Content-Type']
        accept_ranges = headers.get('Accept-Ranges', 'none') == 'bytes'
        print(f'type:{content_type} length: {content_length} accept_ranges: {accept_ranges}')

    print('Creating file')
    create_file(basename, content_length)

    print('Downloading file')
    start = time.time()
    num_chuncks = num_threads if accept_ranges else 1
    threads = []
    for i in range(num_chuncks):
        t = threading.Thread(target=download_chunck, args=(url, basename, content_length, num_chuncks, i))
        t.start()
        threads.append(t)
    for t in threads:
        t.join()
    print(f'Elapsed {time.time() - start:.2f} secs')


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
