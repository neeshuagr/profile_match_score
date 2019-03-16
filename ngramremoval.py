#!/usr/bin/python3.4
import sys
import dcrconfig
import utility
import datetime


def remove_ngram_from_allphrasefile():
    utility.write_to_file(dcrconfig.ConfigManager().SemanticGraphLogFile, 'a',
        'Semantic graph Generation Step 5..! (ngramremoval.py) ' + str(datetime.datetime.now()))
    # Loop thru all phrase files and generate the integer graph
    phrase_file = open(dcrconfig.ConfigManager().PhraseFile, 'r')
    ng_phrase_file = open(dcrconfig.ConfigManager().NGramFilteredPhraseFile,
                          'w')

    for line in phrase_file:
        line = line.strip()
        if (line.startswith('--')):
            #  If the line starts with -- then it is job descriptin beginning
            #  So print a dot indicate the progress
            print('.', end='')
            sys.stdout.flush()
            print(line, file=ng_phrase_file)
            # If the line doesn't start with -- or is not empty space
        if not (line.startswith('--') or len(line.strip()) < 1):
            print(remove_ngram(line), file=ng_phrase_file)


# Splits words in a phrase and keeps only the ones which are 3 words or less in
# filtered phrase file
def remove_ngram(doc):
    docs = ''
    phrases = doc.split('|')
    for phrase in phrases:
        words = phrase.split()
        if len(words) < 4:
            docs = docs + phrase + '|'
    return docs


remove_ngram_from_allphrasefile()
# print(remove_ngram(ngram_doc))
