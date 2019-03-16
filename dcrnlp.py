#    NLP Utility Module
#
#
import re
import nltk
from nltk.corpus import stopwords
import nltk.collocations

stopwords = stopwords.words('english')


def remove_whitespaces(text):
    text = text.replace('\n', '.')
    reexp = re.compile('\s{2,}')
    redotspaces = re.compile('[. ]{2,}')
    text = reexp.sub(' ', text)
    textfinal = redotspaces.sub('. ', text)
    return textfinal


"""
    Name:           sentence_tokenize
    Description:    Function sentence_tokenize will tokonize sentences into
                    sentences. By default it uses nlt sent_tokenize. But this
                    can be changed that fits best.
"""


def sentence_tokenize(text):
    import nltk
    return nltk.sent_tokenize(text)

sentence_re = r'''(?x)      # set flag to allow verbose regexps
                \w+(\s*\w)+ # Use this grammer to divide sentences into
                            # multiple parts. Ignore all the puctations
                            # This will help with defining collocations.
                    '''
"""
    Name:           sentence_tokenize
    Description:    Function sentence_tokenize will tokonize sentences into
                    sentences. By default it uses nlt sent_tokenize. But this
                    can be changed that fits best.
"""


def sentence_part_tokenize(sentences, islist=True):
    if islist is True:
        parts = [nltk.regexp_tokenize(s, sentence_re) for s in sentences]
        sentence_parts = [i for x in parts for i in x]
        return sentence_parts
    else:
        return nltk.regexp_tokenize(sentences, sentence_re)


def leaves(tree):
    """Finds NP (nounphrase) leaf nodes of a chunk tree."""
    for subtree in tree.subtrees(filter=lambda t: t.label() == 'SSNP'):
        yield subtree.leaves()


def normalise(word):
    """Normalises words to lowercase and stems and lemmatizes it."""
    word = word.lower()
    # word = stemmer.stem_word(word)
    # word = lemmatizer.lemmatize(word)
    return word


def acceptable_word(word):
    """Checks conditions for acceptable word: length, stopword."""
    accepted = bool(2 <= len(word) <= 40 and word.lower() not in stopwords)
    return accepted


def get_terms(tree):
    for leaf in leaves(tree):
        term = [normalise(w) for w, t in leaf if acceptable_word(w)]
        yield term

grammar = r"""
            NBAR:
                {<NN.*|JJ>*<NN.*>} # Nouns and Adjs, terminated with Nouns

            NP:
                {<NBAR>}
                {<NBAR><IN><NBAR>}  # Above, connected with in/of/etc...
"""
sent_re = r'''(?x)      # set flag to allow verbose regexps : Python syntax
        ([A-Z])(\.[A-Z])+\.?  # abbreviations, capital letters followed by.
        | \w+(-\w+)*            # words with optional internal hyphens
        | \$?\d+(\.\d+)?%?      # currency and percentages, e.g. $67.20, 20%
        | \.\.\.                # ellipsis
        | [][.,;"'?():-_`]      # these are separate tokens.
                                # More symbols may be added to this list.
        '''
newgrammar = """NP: {<DT>?<JJ>*<NN>}
                    {<JJ><NNS>}
              SNP:  {<NP>*<NNP>*}
                    {<VBP><NNS>}
                    {<NNS><VBP>}
              SSNP: {<SNP>*}
                   """


def extract_nounphrases(text):

    sent_parts = sentence_part_tokenize(sentence_tokenize(text))
    for part in sent_parts:
        words = nltk.regexp_tokenize(part, sent_re)
        poswords = nltk.pos_tag(words)
        parser = nltk.RegexpParser(newgrammar)
        tree = parser.parse(poswords)
        terms = get_terms(tree)

        # print ('-'*100)
        # print ("%s"%(tree,) )

        for term in terms:
            nphrase = " "
            """for word in term:
                nphrase = nphrase + ' ' + word"""
            yield nphrase.join(term)


def extract_nounphrases_sentences(text):
    phrased_text = ""
    sentences = sentence_tokenize(text)
    for s in sentences:
        sent_parts = sentence_part_tokenize(s, False)
        for part in sent_parts:
            words = nltk.regexp_tokenize(part, sent_re)
            poswords = nltk.pos_tag(words)
            parser = nltk.RegexpParser(newgrammar)
            tree = parser.parse(poswords)
            terms = get_terms(tree)

            # print ('-'*100)
            # print ("%s"%(tree,) )

            for term in terms:
                nphrase = " "
                phrased_text += '|' + nphrase.join(term)
        phrased_text += '.'
    return phrased_text


def bigram_finder(text):
    bigram_measures = nltk.collocations.BigramAssocMeasures()
    # trigram_measures = nltk.collocations.TrigramAssocMeasures()

    # change this to read in your data
    finder = (nltk.BigramCollocationFinder
              .from_words([normalise(w) for w in
                           nltk.regexp_tokenize(text, sent_re)
                           if acceptable_word(w)]))

    # only bigrams that appear 3+ times
    finder.apply_freq_filter(1)

    # return the 10 n-grams with the highest PMI
    print("%s" % (finder.nbest(bigram_measures.pmi, 50),))
