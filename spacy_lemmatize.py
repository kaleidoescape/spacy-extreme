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
                 spacy_langs=['de', 'en_core_web_sm'], 
                 disable=["tokenizer", "parser", "ner", "textcat"],
                 logdir=None,
                 encoding='utf-8'):
        self.disable = disable
        self.encoding = encoding

        self.results_q = None
        self.work_q = None
        self.chunker = None

        self.spacy_langs = spacy_langs
        self.spacys = [None]*len(self.spacy_langs)
        self.loader()

    def get_sents(self, batch, spacy_number=0):
        # Parse text with the appropriate spacy in self.spacys
        docs = self.spacys[spacy_number].pipe(batch)
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
        for i, lang in enumerate(self.spacy_langs):
            try:
                self.spacys[i] = spacy.load(lang, disable=self.disable)
            except IOError as e:
                subprocess.call(["python", "-m", "spacy", "download", lang])
            self.spacys[i] = spacy.load(lang, disable=self.disable)
            self.spacys[i].add_pipe(self._prevent_sbd, 
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

    parser = argparse.ArgumentParser(
        description='Parse HUGE text files with spaCy in parallel without' 
                    ' running into memory issues.',
                    formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('fps', nargs='+', 
        help='parallel input files (in order)')
    parser.add_argument('-b', '--batch-size', type=int, default=20000,
        help='batch size (in lines).')
    parser.add_argument('-m', '--max-tasks-per-child', type=int, default=5,
        help="max number of batches that a child process can"
             " process before it is killed and replaced."
             " Use this when running into memory issues.")
    parser.add_argument('-n', '--n-workers', type=int, default=DEFAULT_WORKERS,
        help=f"number of workers to use (default depends on your system)")
    parser.add_argument('--spacy-langs', nargs='+', default=['de', 'en'],
        help='spaCy model language names (in order) to use for lemmatization')
    parser.add_argument('--outdir', default=None,
        help='directory to put outputs (if different original file locations)')
    parser.add_argument('--logdir', default=None,
        help='directory to put logs (default will be current working dir)')
    args = parser.parse_args()

    args = vars(args)
    fps = args.pop('fps')
    workers = args.pop('n_workers')
    b_size = args.pop('batch_size')
    max_tasks = args.pop('max_tasks_per_child')
    outdir = args.pop('outdir')

    representer = ParallelSpacy(**args)
    representer.chunker = Chunker(fps, b_size)
    representer.process(fps, workers, max_tasks, outdir=outdir)

