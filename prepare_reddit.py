import re
import os
import json
import zstandard
from tqdm import tqdm
from collections import namedtuple


import argparse
parser = argparse.ArgumentParser()
parser.add_argument('--raw_dir', type=str, default='reddit')
parser.add_argument('--prep_dir', type=str, default='reddit-prep')
parser.add_argument('--date', type=str, default='2021-01')
parser.add_argument('--min_length', type=int, default=9)
parser.add_argument('--max_length', type=int, default=128)
parser.add_argument('--max_depth', type=int, default=5)
parser.add_argument('--sep_ord', type=int, default=1000)
args = parser.parse_args()



Comment = namedtuple("Comment", ["id", "thread_id", "parent_id", "author", "body"])


# https://github.com/Watchful1/PushshiftDumps/blob/master/scripts/combine_folder_multiprocess.py
def read_and_decode(reader, chunk_size, max_window_size, previous_chunk=None, bytes_read=0):
    chunk = reader.read(chunk_size)
    bytes_read += chunk_size
    if previous_chunk is not None:
        chunk = previous_chunk + chunk
    try:
        return chunk.decode()
    except UnicodeDecodeError:
        if bytes_read > max_window_size:
            raise UnicodeError(f"Unable to decode frame after reading {bytes_read:,} bytes")
        return read_and_decode(reader, chunk_size, max_window_size, chunk, bytes_read)

def read_lines_zst(file_name):
    with open(file_name, 'rb') as file_handle:
        buffer = ''
        reader = zstandard.ZstdDecompressor(max_window_size=2**31).stream_reader(file_handle)
        while True:
            chunk = read_and_decode(reader, 2**27, (2**29) * 2)
            if not chunk:
                break
            
            lines = (buffer + chunk).split("\n")
            for line in lines[:-1]:
                yield line, file_handle.tell()
            buffer = lines[-1]

        reader.close()


def normalize_body(body, min_length, max_length):
    body = body.replace('\n', ' ')
    if not min_length <= len(body) <= max_length:
        body = '[deleted]'
    return body

def normalize_id(raw_id):
    """Reddit IDs start with t1_, t2_, etc. which need to be stripped."""
    return re.sub("^t[0-9]_", "", raw_id)

def normalize_comment(comment, min_length, max_length):
    return Comment(
        id=comment['id'],
        thread_id=normalize_id(comment['link_id']),
        parent_id=normalize_id(comment['parent_id']),
        author=(comment['author']),
        body=normalize_body(comment['body'], min_length, max_length),
    )


def should_skip(comment):
    if comment.body in {"[deleted]", "[removed]"}:
        return True
    if re.search('bot', comment.author.lower()):
        return True
    if re.search("i'?\s*a?m a bot", comment.body.lower()):
        return True
    return False



def find_path(_id, id_to_comment, min_length, max_depth):
    depth = 0
    path = []
    
    while True:
        cmt = id_to_comment.get(_id, None)
        if cmt is None:
            break
            
        if not should_skip(cmt):
            path.append(cmt.body)
            depth += 1
        
        if cmt.thread_id == cmt.parent_id:
            break
            
        if depth == max_depth:
            break
        
        _id = cmt.parent_id
        
    return path[::-1]


def main(args):
    os.makedirs(args.prep_dir, exist_ok=True)
    fname = f'RC_{args.date}'

    # 약 160분 소요
    print('started reading comments')
    id_to_comment = {}
    ids = []
    cnt = 0

    for line, _ in read_lines_zst(f'{args.raw_dir}/{fname}.zst'):
        comment = json.loads(line)
        comment = normalize_comment(comment, min_length=args.min_length, max_length=args.max_length)
        
        id_to_comment[comment.id] = comment
        ids.append(comment.id)
        cnt += 1

        if cnt % 10000 == 0:
            print(f'reading {cnt} comments')

        # for debug
        # if len(ids) > 100000:
        #     break
    print('finished reading comments')

    print('started finding paths')
    f = open(f'{args.prep_dir}/{fname}.txt', 'a')
    for _id in tqdm(ids):
        thread = find_path(_id, id_to_comment, min_length=args.min_length, max_depth=args.max_depth)
        if len(thread) < 2:
            continue
        else:
            thread = chr(args.sep_ord).join(thread)
            f.write(thread + '\n')
    print('finished finding paths')


if __name__ == '__main__':
    main(args)