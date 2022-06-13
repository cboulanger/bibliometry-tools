# This is an adapted version of code available at
# https://github.com/akshatgui/Domain_Teminology_Extraction/blob/9a1b71c0ede2a1693c0ab429289250b89023941a/keys.py
#
# The algorithm is described in: S. Rajput, A. Gahoi, M. Reddy, und D. M.
# Sharma, „N-Grams TextRank A Novel Domain Keyword Extraction Technique“,
# Proceedings of the 17th International Conference on Natural Language
# Processing: TermTraction 2020, 2020, S. 9–12.
#
# Prerequisites:
# python -m spacy download <language-pack> # i.e, "en_core_web_sm" or "de_core_news_sm"

import sys, os, regex as re, csv
from collections import OrderedDict
import numpy as np
import spacy
from spacy.lang.en.stop_words import STOP_WORDS


class TextRank4Keyword():
    """Extract keywords from text"""

    def __init__(self):
        self.nlp = spacy.load('en_core_web_sm')
        self.d = 0.85  # damping coefficient, usually is .85
        self.min_diff = 1e-5  # convergence threshold
        self.steps = 10  # iteration steps
        self.node_weight = None  # save keywords and its weight

    def set_stopwords(self, stopwords):
        """Set stop words"""
        for word in STOP_WORDS.union(set(stopwords)):
            lexeme = self.nlp.vocab[word]
            lexeme.is_stop = True

    def sentence_segment(self, doc, candidate_pos, lower, bigrams, trigrams):
        """Store those words only in cadidate_pos"""
        sentences = []
        for sent in doc.sents:
            selected_words = []
            bigram_words = []
            for token in sent:
                bigram_words.append(token.text)
                # Store words only with cadidate POS tag
                if token.pos_ in candidate_pos and token.is_stop is False:
                    if lower is True:
                        selected_words.append(token.text.lower())
                    else:
                        selected_words.append(token.text)
            if bigrams == True:
                for i in range(len(sent) - 1):
                    if sent[i].pos_ in candidate_pos and sent[i].is_stop is False and sent[
                        i + 1].pos_ in candidate_pos and sent[i + 1].is_stop is False:
                        if lower is True:
                            selected_words.append(sent[i].text.lower())
                        else:
                            selected_words.append(str(sent[i].text + " " + sent[i + 1].text))
            if trigrams == True:
                for i in range(len(sent) - 2):
                    if sent[i].pos_ in candidate_pos and sent[i].is_stop is False and sent[
                        i + 1].pos_ in candidate_pos and sent[i + 1].is_stop is False and sent[
                        i + 2].pos_ in candidate_pos and sent[i + 2].is_stop is False:
                        if lower is True:
                            selected_words.append(sent[i].text.lower())
                        else:
                            selected_words.append(str(sent[i].text + " " + sent[i + 1].text + " " + sent[i + 2].text))
            sentences.append(selected_words)
        return sentences

    def get_vocab(self, sentences):
        """Get all tokens"""
        vocab = OrderedDict()
        i = 0
        for sentence in sentences:
            for word in sentence:
                if word not in vocab:
                    vocab[word] = i
                    i += 1
        return vocab

    def get_token_pairs(self, window_size, sentences):
        """Build token_pairs from windows in sentences"""
        token_pairs = list()
        for sentence in sentences:
            for i, word in enumerate(sentence):
                for j in range(i + 1, i + window_size):
                    if j >= len(sentence):
                        break
                    pair = (word, sentence[j])
                    if pair not in token_pairs:
                        token_pairs.append(pair)
        return token_pairs

    def symmetrize(self, a):
        return a + a.T - np.diag(a.diagonal())

    def get_matrix(self, vocab, token_pairs):
        """Get normalized matrix"""
        # Build matrix
        vocab_size = len(vocab)
        g = np.zeros((vocab_size, vocab_size), dtype='float')
        for word1, word2 in token_pairs:
            i, j = vocab[word1], vocab[word2]
            g[i][j] = 1

        # Get Symmeric matrix
        g = self.symmetrize(g)

        # Normalize matrix by column
        norm = np.sum(g, axis=0)
        g_norm = np.divide(g, norm, where=norm != 0)  # this is ignore the 0 element in norm

        return g_norm

    def get_weights(self):
        node_weight_list = [list(ele) for ele in self.node_weight.items()]
        for i in node_weight_list:
            res = len(i[0].split())
            if res == 2:
                # weight for bigrams
                i[1] = 4 * i[1]
            if res == 3:
                # weight for trigrams
                i[1] = 6 * i[1]
        return OrderedDict(sorted(node_weight_list, key=lambda t: t[1], reverse=True))

    def get_keywords(self, keyword_size=10):
        """Return the given number of keywords"""
        return list(self.get_weights().keys())[:keyword_size]

    def analyze(self, text,
                candidate_pos=['NOUN', 'VERB'],
                window_size=4,
                lower=False,
                bigrams=True,
                trigrams=True,
                stopwords=list()):
        """Main function to analyze text"""

        # setting the maximun length to the length of the text. This might cause out of memory errors in really large corpuses
        self.nlp.max_length = len(text)

        # Set stop words
        self.set_stopwords(stopwords)

        # Pare text by spaCy
        doc = self.nlp(text)

        # Filter sentences
        sentences = self.sentence_segment(doc, candidate_pos, lower, bigrams, trigrams)  # list of list of words

        # Build vocabulary
        vocab = self.get_vocab(sentences)

        # Get token_pairs from windows
        token_pairs = self.get_token_pairs(window_size, sentences)

        # Get normalized matrix
        g = self.get_matrix(vocab, token_pairs)

        # Initialization for weight (pagerank value)
        pr = np.array([1] * len(vocab))

        # Iteration
        previous_pr = 0
        for epoch in range(self.steps):
            pr = (1 - self.d) + self.d * np.dot(g, pr)
            if abs(previous_pr - sum(pr)) < self.min_diff:
                break
            else:
                previous_pr = sum(pr)

        # Get weight for each node
        node_weight = dict()
        for word, index in vocab.items():
            node_weight[word] = pr[index]

        self.node_weight = node_weight


def get_words_from_file(file_path, replace_terms: dict = None) -> list:
    """
    Given a file path and a dict of terms to be replaced, return a list of words
    contained in the file
    :param file_path:
    :param replace_terms:
    :return:
    """
    with open(file_path, "r", encoding="utf-8") as f:
        text = f.read()
    # remove OCR artifacts
    text = re.sub(r"ﬁ", "fi", text)
    # join hyphenated words
    text = re.sub(r'- *[\r\n]+', "", text)
    # replace terms
    if replace_terms is not None:
        for key, value in replace_terms.items():
            text = re.sub(key, value, text)
    # remove punctuation
    text = re.sub("\p{P}", "", text)
    # return remaining words
    return text.split()


def extract_keywords(corpus_dir_or_file,
                     replace_terms: dict = None,
                     keyword_size=20,
                     window_size=4,
                     lower=False,
                     bigrams=True,
                     trigrams=True,
                     candidate_pos: list = ['NOUN', 'VERB'],
                     stopwords=list()):
    words = []
    if os.path.isdir(corpus_dir_or_file):
        # load corpus from files in the given directory
        files = os.listdir(corpus_dir_or_file)
        print(f"Loading text from {len(files)} files in {corpus_dir_or_file} ...")
        for file_name in files:
            if file_name.endswith(".txt"):
                words.extend(
                    get_words_from_file(os.path.join(corpus_dir_or_file, file_name), replace_terms=replace_terms))
    elif os.path.isfile(corpus_dir_or_file):
        words.extend(get_words_from_file(corpus_dir_or_file, replace_terms=replace_terms))
    else:
        raise ValueError(f"Invalid file/dir {corpus_dir_or_file}")

    text = ' '.join(words)
    tr4w = TextRank4Keyword()
    tr4w.analyze(text, candidate_pos=candidate_pos, window_size=window_size, lower=lower, bigrams=bigrams,
                 trigrams=trigrams, stopwords=stopwords)
    return tr4w.get_keywords(keyword_size)

def get_replace_dict_from_file(replace_terms_file_path):
    """
    Reads a csv file containing terms in column 1 that should be replaced with the value of column 2 or
    omitted if column 2 is empty or does not exist. returns a dict having the term to be replaced
    as key and the replacement as value
    :param replace_terms_file_path:
    :return:dict
    """
    replace_terms_dict = {}
    with open(replace_terms_file_path) as f:
        reader = csv.reader(f)
        for row in reader:
            if row[0]:
                replace_terms_dict[row[0]] = row[1] if len(row) > 1 else ""
    return replace_terms_dict

if __name__ == "__main__":
    corpus_dir_or_file = sys.argv[1]
    fp1 = sys.argv[2] if len(sys.argv) > 2 else None
    replace_terms_dict = None
    if fp1 and os.path.isfile(fp1):
        replace_terms_dict = get_replace_dict_from_file(fp1)
    if os.path.isdir(corpus_dir_or_file):
        fp2 = os.path.join(corpus_dir_or_file, "replace-terms.csv")
        replace_terms_dict = {**replace_terms_dict, **get_replace_dict_from_file(fp2)}
    keywords = extract_keywords(corpus_dir_or_file, replace_terms=replace_terms_dict)
    print(keywords)
