import math
import os.path

DEFAULT_BLOCK_SIZE = 1024 * 512


def get_num_blocks(content_length, block_size=DEFAULT_BLOCK_SIZE):
    """
    Split file of specified length into blocks
    """
    return int(math.ceil(content_length / block_size))


def get_block_map(file_name, content_length, num_blocks):
    """
    Get block map from the file
    """
    if os.path.getsize(file_name) != content_length + num_blocks:
        raise RuntimeError("File size does not match")
    with open(file_name, 'r+b') as f:
        f.seek(content_length)
        return [bool(b) for b in f.read(num_blocks)]


def set_block_map(file_name, content_length, block_map):
    """
    Set block map to the file
    """
    with open(file_name, 'r+b') as f:
        f.seek(content_length)
        f.write(bytes(block_map))


def split_remaining_blocks(block_map, num_workers):
    """
    Find unfinished blocks and split into equal shares
    """
    remaining_blocks = [i for i, b in enumerate(block_map) if not b]
    blocks_each = int(math.ceil(len(remaining_blocks) / num_workers))
    for i in range(num_workers):
        yield remaining_blocks[i * blocks_each:min(len(block_map), (i + 1) * blocks_each)]