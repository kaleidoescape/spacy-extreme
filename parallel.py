import datetime
import logging 
import os
import sys
from multiprocessing import Manager, Pool, Process

import psutil
import spacy

from chunker import Chunker
from utils import setup_logger


""" Processes a single, huge text file without running into memory issues 
    IF the right parameters are chosen.
    
    Important parameters:
        - -b, --batch-size: the batch size (in bytes) to process at the same time. A larger batch, will mean that
            every task (in the max-tasks-per-child) will use more memory. You need to find a good balance between
            batch-size and max-tasks-per-child.
        - -m, --max-tasks-per-child: the number of batches to process before a child process is killed and replaced
            this will effectively free the memory used by that child process. If you are very low on memory, 
            set this to 1, meaning that each process will only process one batch before being replaced.
        - -n, --n-workers: the number of child processes to spawn that will process the batches. It is important
            to know that the readers and writers are working in their own subprocesses, so don't use all cores 
            for n-workers. Also, the more cores you put to work simultaneously, the more memory you will be using.
            On top of that, if your batch-size is too small, the reader will not be fast enough to feed all the workers.
            So, again, you need to find a good trade-off focused on the batch-size.

    Reading input happens in chunks. The byte file pointers of each chunk are passed to the child processes,
    leaving them in charge of actually getting the contents from the file.
    Because byte chunks are line-agnostic, we assume that the last line of each chunk is an incomplete line whose
    second part is actually the first line of the next chunk. Therefore, we return the first and last line of all
    chunks, and process them at the very end; stitching them back together.
    This means, though, that the order of the sentence in the input file is NOT preserved.
    
    You can use this file as a template, and only change the process_batch method to your liking.
    That's where the actual values from spaCy are retrieved and processed.
"""

DEFAULT_WORKERS = (os.cpu_count() - 2) or 1


class Parallel(object):
    def __init__(self, logdir=None, encoding='utf-8', linesep=os.linesep):
        self.logdir = logdir
        if not self.logdir:
            self.logdir = os.getcwd()
        self.logger = setup_logger(self.logdir, name='parallel')

        self.encoding = encoding
        self.linesep = linesep

        self.results_q = None
        self.work_q = None
        self.chunker = None

    def loader(self):
        pass

    def get_sents(self, batch, language_direction=0):
        result = [s for s in batch]
        n_sentences = len(batch)
        n_tokens = 0
        for sent in batch:
            n_tokens += len(sent)
        return result, n_sentences, n_tokens

    def get_output_paths(self, fps, outdir, suffix='.out'):
        new_fps = []
        for fp in fps:
            if outdir is None:
                fpout = fp + suffix
            else:
                os.makedirs(outdir, exist_ok=True)
                fpout = os.path.join(
                    outdir, os.path.basename(fp) + suffix)
            new_fps.append(fpout)
        return new_fps 

    def process(self, fps, n_workers, max_tasks_per_child, outdir=None):
        self.logger.info(f"Started processing {fps} with {n_workers} workers.")
        if max_tasks_per_child:
            self.logger.info(f"Max. {max_tasks_per_child} tasks per child process before replacement.")

        fpouts = tuple(self.get_output_paths(fps, outdir))

        start_time = datetime.datetime.now()

        total_n_src_sents = 0
        total_n_src_tokens = 0
        with Manager() as manager:
            self.results_q = manager.Queue(maxsize=max(n_workers * 100, 256))
            self.work_q = manager.Queue(maxsize=n_workers * 2)

            # Create a reader and a writer process
            reader_proc = Process(target=self.reader)
            reader_proc.start() #the reader starts filling up the work_q
            writer_proc = Process(
                target=self.writer, 
                args=(fpouts,)
            )
            writer_proc.start()

            with Pool(n_workers, maxtasksperchild=max_tasks_per_child) as pool:

                #reload objects to avoid memory leaks, e.g. spaCy has this problem:
                #https://github.com/explosion/spaCy/issues/3618
                if self.loader:
                    self.loader()

                worker_jobs = []
                self.logger.info('Chunking...')
                while True:
                    # Get work from the working queue
                    work = self.work_q.get()
                    if work == 'done':
                        break

                    # Apply work to workers
                    job = pool.apply_async(
                        self.process_batch, 
                        (work,)
                    )
                    worker_jobs.append(job)
                self.logger.info('Done chunking...')

                # After the queue is 'done', the reader can close
                reader_proc.join()
                reader_proc.terminate()

                # When a worker has finished its job, get its information back
                for job_idx, job in enumerate(worker_jobs, 1):
                    n_src_sents, n_src_tokens = job.get()

                    total_n_src_sents += n_src_sents
                    total_n_src_tokens += n_src_tokens

                    # Log some progress info
                    if job_idx == 1 or job_idx % n_workers == 0:
                        time_since_start = (datetime.datetime.now() - start_time)
                        sents_perf = total_n_src_sents // time_since_start.total_seconds()
                        time_since_start = self._format_time(time_since_start)
                        self.logger.info(f"Processed batch #{job_idx:,}: {n_src_sents:,} source sents ({sents_perf:,.0f} sents/s)."
                                     f" Mem. use: {psutil.virtual_memory().percent}%. Running for {time_since_start}")

                # Notify the writer that we're done
                self.results_q.put('done')

            writer_proc.join()
            writer_proc.terminate()

        # Log some info
        running_time = (datetime.datetime.now() - start_time)
        src_sents_perf = total_n_src_sents // running_time.total_seconds()
        running_time = self._format_time(running_time)
        self.logger.info(f"Done processing in {running_time} ({src_sents_perf:,.0f} sentences/s)."
                     f" Processed {total_n_src_sents:,.0f} source sentences and {total_n_src_tokens:,.0f} source tokens.")

    def process_batch(self, chunk_tuples):
        chunks = self.chunker.get_batch(chunk_tuples)

        outs = []
        n_sents, n_tokens = 0, 0
        for i, batch in enumerate(chunks):
            sents, n_sents, n_tokens = self.get_sents(batch, i)
            outs.append(sents)

        # Pass results to queue, so they can be written to file by the writer
        self.results_q.put(zip(*outs))

        # Return the number of sentences and number of tokens, just to keep track
        return n_sents, n_tokens 

    @staticmethod
    def _format_time(delta):
        hours, remainder = divmod(delta.total_seconds(), 3600)
        minutes, seconds = divmod(remainder, 60)

        return f"{hours:02,.0f}:{minutes:02.0f}:{seconds:02.0f}"

    # I/O methods
    def writer(self, fps):
        fhs = [open(fp, 'w', encoding=self.encoding) for fp in fps]
        try:
            while True:
                results = self.results_q.get()
                if results =='done':
                    break

                for result in results:
                    for i, item in enumerate(result):
                        fhs[i].write(item + self.linesep)
                        fhs[i].flush()
        finally:
            [fh.close() for fh in fhs]

    def reader(self):
        for chunk_tuple in self.chunker.chunkify():
            self.work_q.put(chunk_tuple)

        self.work_q.put('done')

