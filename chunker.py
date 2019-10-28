import os
import logging
from typing import Generator

from utils import setup_logger

class Chunker:
    """
    Chunker that can chunk a files into byte ranges which 
    can then be retrieved as a list of encoded lines.
    """
    def __init__(self, 
                 fps, 
                 batch_size: int=5000, 
                 encoding: str='utf-8', 
                 linesep: str=os.linesep, 
                 logdir: str=''):
        """
        Args:
            fps: parallel filenames to chunk 
            batch_size: approximate size of each chunk (in number of lines)
            encoding: encoding of the input files. Will be used when 
                      retrieving the encoded batch of lines
        """
        self.logdir = logdir
        if not self.logdir:
            self.logdir = os.getcwd()
        self.logger = setup_logger(self.logdir, name='chunker')

        self.batch_size = int(batch_size)
        self.encoding = encoding
        self.linesep = linesep
        self.fps = fps

        self.logger.info(f"Chunking {len(fps)} parallel files with a batch size of {batch_size:,} lines.")

    def file_len(self, fname):
        with open(fname) as f:
            for i, l in enumerate(f):
                pass
        return i + 1

    def chunkify(self) -> Generator:
        """
        Chunks a files into sequential byte ranges that cover the number of
        lines defined in the constructor.

        Returns:
            a generator that yields iterables of tuples; each tuple
                is the (chunk_start, chunk_size) for each file, where
                chunk_start is the starting byte of the chunk; the 
                iterable is these tuples for each of the parallel files
        """
        length = self.file_len(self.fps[0])
        fhs = [open(fp, 'r', 
                    newline=self.linesep, 
                    encoding=self.encoding) \
               for fp in self.fps]
        
        try:
            ln = 0
            positions = [0]*len(fhs)
            prev_positions = [0]*len(fhs)
            chunk_sizes = [0]*len(fhs)
            while ln < length:
                for i, fh in enumerate(fhs):
                    for _ in range(self.batch_size):
                        fh.readline()
                        positions[i] = fh.tell()
                    chunk_sizes[i] = positions[i] - prev_positions[i]

                yield zip(prev_positions, chunk_sizes)
                ln += self.batch_size 
                prev_positions = positions.copy()

        finally:
            [fh.close() for fh in fhs]
                
    def get_batch(self, chunk_positions, rm_newlines: bool=True) -> list:
        """
        Retrieves a chunk, given a starting byte and chunk size, 
        as a batch of encoded lines. 

        Args:
            chunk_starts: the starting byte position of the chunk in each file
            chunk_sizes: the size in bytes of the chunk in each file
            rm_newlines: whether to remove the newlines at the end (rstrip)

        Returns:    
            the encoded parallel lines in the batch
        """
        fhs = [open(fp, 'rb') for fp in self.fps]
        chunked = []
        try:
            for i, chunk_position in enumerate(chunk_positions):
                chunk_start, chunk_size = chunk_position
                fhs[i].seek(chunk_start)
                data = fhs[i].read(chunk_size)
                if rm_newlines:
                    content = (s.decode(self.encoding).rstrip()
                               for s in data.split(b'\n') if s)
                else:
                    content = (s.decode(self.encoding) 
                               for s in data.split(b'\n') if s)
                chunked.append(content)
        finally:
            [fh.close() for fh in fhs]
        return chunked

