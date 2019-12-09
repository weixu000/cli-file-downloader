# cli-file-downloader
## Installation
- Python 3.5+
- [`requests`](https://requests.readthedocs.io/en/master/user/install/)
-  Tested on Windows and Ubuntu

## Usage
Use `argparse` to parse commandline options `downloader.py [-h] [-c] [-n NUM_THREADS] URL`
- `-h` show help message
- `-c` resume previous downloading if possible
- `-n` threads to download with in parallel
- `URL` URL to download

## General Workflow (`downloader.py`)
For example, run `python downloader.py -c -n 5 nodejs.org/dist/v13.3.0/node-v13.3.0-linux-x64.tar.gz`
1. Check URL and get file name from the URL `node-v13.3.0-linux-x64.tar.gz`.
2. Send HEAD request to get length of the file and check if the serve support range request.
3. `-c` is set so try to find partial file `node-v13.3.0-linux-x64.tar.gz.partial`.
4. Read the partial file, it contains previously downloaded data and a block map about where they are.
5. `-n 5` is set so start 5 threads besides the main thread, each downloads part of unfinished data to partial file.
6. Main thread displays the progress.
7. If finished, rename partial file to the file `node-v13.3.0-linux-x64.tar.gz`, otherwise update block map in the partial file.

## Block Map to Continue Download (`blockmap.py`)
- With length of the file known from HEAD response, it is divided into blocks of equal size `DEFAULT_BLOCK_SIZE` by `get_num_blocks()`.
- At the end of partial file, a "block map" is maintained by `get_block_map()` and `set_block_map()` about whether or not each block is downloaded.
    - In memory, block map is a list of bool; in disk, it is series of bytes.
- To continue, the block map is scanned and unfinished blocks are assign to different threads (workers) by `split_remaining_blocks()`.

## Multithreading to Download in parallel (`workers.py`)
- Each worker runs in a different thread and updates global block map to tell the main thread its progress.
- `Worker` is the base class, it fetches each downloaded block from `self.blocks` in `self.run()` to write data to disk.
- If the server does not support range requests, `WholeWorker` is used to request the whole file.
    - Block map is only to show progress, as we cannot request part of the file.
    - Only one thread is downloading, as it is fallback option.
- If the server supports range requests, `RangeWorker` is used to send request of assigned blocks.

## Tradeoff
- To speed up download, it is better to request whole data and write them together as net/disk IO should do in bulk.
- To enable concurrent downloading and allow users to continue, it is better to download in pieces and record which is done.

I just learned [HTTP range requests](https://developer.mozilla.org/en-US/docs/Web/HTTP/Range_requests) while coding this,
it is good to use because the client can request multiple parts of the file and get all of them in one response.
So using range requests we can download in multiple threads and each thread can do minimal network IO. (The overhead is response headers)

On the other hand, thank to internal buffering, writing data block one by one to disk is not the bottleneck compare to network IO.

The `DEFAULT_BLOCK_SIZE` sets the granularity of download.
Being too large makes resuming download useless, while being too small makes network overhead worse as each range in request has a header.

## Miscellaneous
- HEAD request is simple and not prone to broken connection while GET request should retry if connection is broken.
- Python thread is not true thread so by itself CPU overhead is not serious, it is suitable for IO intensive tasks.
- Disk IO is inherent for download and block map is small relative to file.
- Network IO is minimal by using range requests but setting up too many connections may impede performance.
- Use `threading.Event` to gracefully stop workers
- Use `requests` to handle basic HTTP stuff.
- Do not overwrite exisiting file to avoid users' misuse.
- Find the scheme of URL and file name of file for easier use.

## Limitations
Because I have several final exams next week, the implementation is not that great.
- Does not check if partial file is up to date with the file on server, may use ETag.
- Representation of block map in disk is not compact enough, may use run-length coding.
- Not user friendly.
- Using real-world URL in unit test may not cover all the cases.
- Python is not tuned for performance.

## Thanks you for consideration!