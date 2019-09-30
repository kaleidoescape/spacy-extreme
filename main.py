import datetime
import logging
<<<<<<< HEAD
import os
import subprocess
from math import inf
from multiprocessing import Manager, Pool, Process
=======
from math import inf
from multiprocessing import Manager, Pool, Process
from os import cpu_count
>>>>>>> 1f5191d138e6e6d0b95ac9e6d678dc50fc1b1052
from pathlib import Path

import psutil
import spacy

from Chunker import Chunker

<<<<<<< HEAD

=======
>>>>>>> 1f5191d138e6e6d0b95ac9e6d678dc50fc1b1052
logging.basicConfig(datefmt='%d-%b %H:%M:%S',
                    format='%(asctime)s - [%(levelname)s]: %(message)s',
                    level=logging.INFO,
                    handlers=[
                        logging.FileHandler('progress.log'),
                        logging.StreamHandler()
                    ])

<<<<<<< HEAD

DEFAULT_WORKERS = (os.cpu_count() - 2) or 1
=======
DEFAULT_WORKERS = (cpu_count() - 2) or 1
>>>>>>> 1f5191d138e6e6d0b95ac9e6d678dc50fc1b1052

""" Processes a single, huge text file with spaCy, without running into memory issues 
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
        - --space-model: it makes sense that if you use a large spaCy model, you will consume more memory. 

    Reading input happens in chunks. The byte file pointers of each chunk are passed to the child processes,
    leaving them in charge of actually getting the contents from the file.
    Because byte chunks are line-agnostic, we assume that the last line of each chunk is an incomplete line whose
    second part is actually the first line of the next chunk. Therefore, we return the first and last line of all
    chunks, and process them at the very end; stitching them back together.
    This means, though, that the order of the sentence in the input file is NOT preserved.
    
    You can use this file as a template, and only change the process_batch method to your liking.
    That's where the actual values from spaCy are retrieved and processed.
"""


class Representator:
    def __init__(self, 
                 src_spacy='de',
                 tgt_spacy="en_core_web_sm",
                 disable=["tokenizer", "parser", "ner", "textcat"],
                 logdir=None):
        self.src_spacy = src_spacy
        self.tgt_spacy = tgt_spacy
        self.disable = disable
        self.results_q = None
        self.work_q = None
        self.chunker = None

        self.load_nlp()

    def load_nlp(self):
        try:
            self.src_nlp = spacy.load(self.src_spacy, disable=self.disable)
        except IOError as e:
            subprocess.call(["python", "-m", "spacy", "download", self.src_spacy])
            self.src_nlp = spacy.load(self.src_spacy, disable=self.disable)
        try:
            self.tgt_nlp = spacy.load(self.tgt_spacy, disable=self.disable)
        except IOError as e:
            subprocess.call(["python", "-m", "spacy", "download", self.tgt_spacy])
            self.tgt_nlp = spacy.load(self.tgt_spacy, disable=self.disable)

        self.src_nlp.add_pipe(self._prevent_sbd, 
                              name='prevent-sbd', 
                              before='tagger')
        self.tgt_nlp.add_pipe(self._prevent_sbd, 
                              name='prevent-sbd', 
                              before='tagger')

    def get_output_paths(self, srcfp, tgtfp, outdir):
        if outdir is None:
            srcfpout = srcfp + '.spacy'
            tgtfpout = tgtfp + '.spacy'
        else:
            os.makedirs(outdir, exist_ok=True)
            srcfpout = os.path.join(
                outdir, os.path.basename(srcfp) + '.spacy')
            tgtfpout = os.path.join(
                outdir, os.path.basename(tgtfp) + '.spacy')
        return srcfpout, tgtfpout

    def process(self, srcfp, tgtfp, n_workers, max_tasks_per_child, outdir=None):
        logging.info(f"Started processing {srcfp} + {tgtfp} with {n_workers} workers.")
        if max_tasks_per_child:
            logging.info(f"Max. {max_tasks_per_child} tasks per child process before replacement.")

        srcfpout, tgtfpout = self.get_output_paths(srcfp, tgtfp, outdir)

        start_time = datetime.datetime.now()

        total_n_src_sents = 0
        total_n_src_tokens = 0
        total_n_tgt_sents = 0
        total_n_tgt_tokens = 0
        with Manager() as manager:
            self.results_q = manager.Queue(maxsize=max(n_workers * 100, 256))
            self.work_q = manager.Queue(maxsize=n_workers * 2)

            # Create a reader and a writer process
            reader_proc = Process(target=self.reader)
            reader_proc.start() #the reader starts filling up the work_q
            writer_proc = Process(
                target=self.writer, 
                args=(srcfpout, tgtfpout)
            )
            writer_proc.start()

            with Pool(n_workers, maxtasksperchild=max_tasks_per_child) as pool:

                #reload nlp to avoid SpaCy memory leak: https://github.com/explosion/spaCy/issues/3618
                self.load_nlp() 

                worker_jobs = []
                logging.info('Chunking...')
                while True:
                    # Get work from the working queue
                    work = self.work_q.get()
                    if work == 'done':
                        break

                    src_chunk_start, src_chunk_size, tgt_chunk_start, tgt_chunk_size = work
                    # Apply work to workers
                    job = pool.apply_async(
                        self.process_batch, 
                        (src_chunk_start, src_chunk_size, 
                         tgt_chunk_start, tgt_chunk_size)
                    )
                    worker_jobs.append(job)
                logging.info('Done chunking...')

                # After the queue is 'done', the reader can close
                reader_proc.join()
                reader_proc.terminate()

                # When a worker has finished its job, get its information back
                for job_idx, job in enumerate(worker_jobs, 1):
                    n_src_sents, n_src_tokens, n_tgt_sents, n_tgt_tokens = job.get()

                    total_n_src_sents += n_src_sents
                    total_n_src_tokens += n_src_tokens
                    total_n_tgt_sents += n_tgt_sents
                    total_n_tgt_tokens += n_tgt_tokens

                    # Log some progress info
                    if job_idx == 1 or job_idx % n_workers == 0:
                        time_since_start = (datetime.datetime.now() - start_time)
                        sents_perf = total_n_src_sents // time_since_start.total_seconds()
                        time_since_start = self._format_time(time_since_start)
                        logging.info(f"Processed batch #{job_idx:,}: {n_src_sents:,} source sents ({sents_perf:,.0f} sents/s)."
                                     f" Mem. use: {psutil.virtual_memory().percent}%. Running for {time_since_start}")

                # Notify the writer that we're done
                self.results_q.put('done')

            writer_proc.join()
            writer_proc.terminate()

        # Log some info
        running_time = (datetime.datetime.now() - start_time)
        src_sents_perf = total_n_src_sents // running_time.total_seconds()
        running_time = self._format_time(running_time)
        logging.info(f"Done processing in {running_time} ({src_sents_perf:,.0f} sentences/s)."
                     f" Processed {total_n_src_sents:,.0f} source sentences and {total_n_src_tokens:,.0f} tokens."
                     f" Processed {total_n_tgt_sents:,.0f} target sentences and {total_n_tgt_tokens:,.0f} tokens.")

    def process_sentence(self, sentence):
        return ' '.join([token.lemma_ for token in sentence])

    def get_sents(self, batch, src_or_tgt='src'):
        # Parse text with spaCy
        if src_or_tgt == 'src':
            docs = self.src_nlp.pipe(batch)
        elif src_or_tgt == 'tgt':
            docs = self.tgt_nlp.pipe(batch)
        # Chop into sentences
        spacy_sents = [sent for doc in docs for sent in doc.sents]
        del docs
        n_sentences = len(spacy_sents)
        n_tokens = 0
        # Get some value from spaCy that we want to write to files
        sents_tok = []
        for sent in spacy_sents:
            n_tokens += len(sent)
            sents_tok.append(self.process_sentence(sent))
        return sents_tok, n_sentences, n_tokens

    def process_batch(self, 
                      src_chunk_start, 
                      src_chunk_size, 
                      tgt_chunk_start, 
                      tgt_chunk_size):
        src_batch, tgt_batch = self.chunker.get_batch(src_chunk_start,
                                                      src_chunk_size, 
                                                      tgt_chunk_start, 
                                                      tgt_chunk_size)

        src_sents, n_src_sents, n_src_tokens = self.get_sents(src_batch, 'src')
        tgt_sents, n_tgt_sents, n_tgt_tokens = self.get_sents(tgt_batch, 'tgt')

        # Pass results to queue, so they can be written to file by the writer
        self.results_q.put((src_sents, tgt_sents))

        # Return the number of sentences and number of tokens, just to keep track
        # Also return first and last line. These are likely to be 'broken' sentences
        # due to chunking. After processing everything, we will process these 'partial
        # sentences' separately in the main process.
        return n_src_sents, n_src_tokens, n_tgt_sents, n_tgt_tokens

    @staticmethod
    def _prevent_sbd(doc):
        # If you already have one sentence per line in your file
        # you may wish to disable sentence segmentation with this function,
        # which is added to the nlp pipe in the constructor
        for token in doc:
            token.is_sent_start = False
        return doc

    @staticmethod
    def _format_time(delta):
        hours, remainder = divmod(delta.total_seconds(), 3600)
        minutes, seconds = divmod(remainder, 60)

        return f"{hours:02,.0f}:{minutes:02.0f}:{seconds:02.0f}"

    # I/O methods
    def writer(self, srcfp, tgtfp):
        with open(srcfp, 'w', encoding='utf-8') as srcout, \
             open(tgtfp, 'w', encoding='utf-8') as tgtout:

            while True:
                m = self.results_q.get()
                if m == 'done':
                    break

                src_sents, tgt_sents = m
                srcout.write('\n'.join(src_sents) + '\n')
                srcout.flush()
                tgtout.write('\n'.join(tgt_sents) + '\n')
                tgtout.flush()

    def reader(self):
        for chunk_tuple in self.chunker.chunkify():
            self.work_q.put(chunk_tuple)

        self.work_q.put('done')


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='Parse HUGE text files with spaCy in parallel without running'
                                                 ' into memory issues.',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('srcfp', help='source language input file.')
    parser.add_argument('tgtfp', help='target language input file.')
    parser.add_argument('-b', '--batch-size', type=int, default=20000,
                        help='batch size (in lines).')
    parser.add_argument('-m', '--max-tasks-per-child', type=int, default=5,
                        help="max number of batches that a child process can process before it is killed and replaced."
                             " Use this when running into memory issues.")
    parser.add_argument('-n', '--n-workers', type=int, default=DEFAULT_WORKERS,
                        help=f"number of workers to use (default depends on your current system).")
    parser.add_argument('--src-spacy', default='de',
                        help='spaCy model to use for source sentences.')
    parser.add_argument('--tgt-spacy', default='en_core_web_sm',
                        help='spaCy model to use for target sentences.')
    parser.add_argument('--outdir', default=None,
                        help='directory to put outputs (if different from where original files are).')
    parser.add_argument('--logdir', default=None,
                        help='directory to put logs (default will be current working dir)')
    args = parser.parse_args()

    args = vars(args)
    srcfp = args.pop('srcfp')
    tgtfp = args.pop('tgtfp')
    workers = args.pop('n_workers')
    b_size = args.pop('batch_size')
    max_tasks = args.pop('max_tasks_per_child')
    outdir = args.pop('outdir')

    representer = Representator(**args)
    representer.chunker = Chunker(srcfp, tgtfp, b_size)
    representer.process(srcfp, tgtfp, workers, max_tasks, outdir=outdir)

