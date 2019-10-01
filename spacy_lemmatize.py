import datetime
import logging
import os
import subprocess
from math import inf
from multiprocessing import Manager, Pool, Process
from pathlib import Path

import psutil
import spacy

from chunker import Chunker
from parallel import Parallel

logging.basicConfig(datefmt='%d-%b %H:%M:%S',
                    format='%(asctime)s - [%(levelname)s]: %(message)s',
                    level=logging.INFO,
                    handlers=[
                        logging.FileHandler('progress.log'),
                        logging.StreamHandler()
                    ])


DEFAULT_WORKERS = (os.cpu_count() - 2) or 1

class ParallelSpacy(Parallel):
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

        self.loader()

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
            sents_tok.append(self.get_lemmas(sent))
        return sents_tok, n_sentences, n_tokens
        
    def get_lemmas(self, sentence):
        return ' '.join([token.lemma_ for token in sentence])

    def loader(self):
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

    @staticmethod
    def _prevent_sbd(doc):
        # If you already have one sentence per line in your file
        # you may wish to disable sentence segmentation with this function,
        # which is added to the nlp pipe in the constructor
        for token in doc:
            token.is_sent_start = False
        return doc

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

    representer = ParallelSpacy(**args)
    representer.chunker = Chunker(srcfp, tgtfp, b_size)
    representer.process(srcfp, tgtfp, workers, max_tasks, outdir=outdir)

