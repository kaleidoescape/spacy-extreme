import logging
from os import stat
from pathlib import Path

from typing import Generator, Union

logger = logging.getLogger(__name__)


class Chunker:
    """ Chunker that can chunk a file into byte ranges which can then be retrieved as a list of encoded lines. """
    def __init__(self, srcfp, tgtfp, batch_size: int = 5000, encoding: str = 'utf-8'):
        """
        :param fin: filename to chunk
        :param batch_size: approximate size of each chunk (in number of lines)
        :param encoding: encoding of the input file. Will be used when retrieving the encoded batch of lines
        """
        self.batch_size = int(batch_size)
        self.encoding = encoding
        self.srcfp = srcfp
        self.tgtfp = tgtfp

        logger.info(f"Chunking with a batch size of {batch_size:,} lines.")

    def file_len(self, fname):
        with open(fname) as f:
            for i, l in enumerate(f):
                pass
        return i + 1

    def chunkify(self) -> Generator:
        """ Chunks a file into sequential byte ranges of approximately the same size as defined in the constructor.
        The size of each chunk is not exactly the same because if a chunk ends on an incomplete line, the remainder
        of the line will also be read and included in the chunk.

        :returns a generator that yields tuples of two integers: the starting byte of the chunk and its size
        """
        length = self.file_len(self.srcfp)

        with open(self.srcfp, 'r', encoding='utf-8') as srcin, \
             open(self.tgtfp, 'r', encoding='utf-8') as tgtin:
            ln = 0
            prev_s_pos = 0
            prev_t_pos = 0
            while ln < length:
                for _ in range(self.batch_size):
                    srcin.readline()
                    s_pos = srcin.tell()
                    tgtin.readline()
                    t_pos = tgtin.tell()
                s_chunk_size = s_pos - prev_s_pos
                t_chunk_size = t_pos - prev_t_pos
                yield (prev_s_pos, s_chunk_size, prev_t_pos, t_chunk_size)
                ln += self.batch_size 
                prev_s_pos = s_pos
                prev_t_pos = t_pos
                
    def get_batch(self, 
                  src_chunk_start: int, 
                  src_chunk_size: int, 
                  tgt_chunk_start: int,
                  tgt_chunk_size: int,
                  rm_newlines: bool = True) -> Generator:
        """ Retrieves a chunk, given a starting byte and chunk size, as a batch of encoded lines through a generator.

        :param chunk_start: the starting byte position of the requested chunk
        :param chunk_size: the size of the requested chunk
        :param rm_newlines: whether to remove the newlines at the end of each line (rstrip)
        :returns a generator that yields each encoded line in the batch
        """
        with open(self.srcfp, 'rb') as srcin, \
             open(self.tgtfp, 'rb') as tgtin:
            srcin.seek(src_chunk_start)
            src_chunk = srcin.read(src_chunk_size)
            tgtin.seek(tgt_chunk_start)
            tgt_chunk = tgtin.read(tgt_chunk_size) 

        if rm_newlines:
            src = (s.decode(self.encoding).rstrip() 
                   for s in src_chunk.split(b'\n') if s)
            tgt = (s.decode(self.encoding).rstrip() 
                   for s in tgt_chunk.split(b'\n') if s)
        else:
            src = (s.decode(self.encoding) 
                   for s in src_chunk.split(b'\n') if s)
            tgt = (s.decode(self.encoding) 
                   for s in tgt_chunk.split(b'\n') if s)
        return src, tgt
