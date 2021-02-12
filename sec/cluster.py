import re
import pandas as pd
import numpy as np
from sklearn.base import BaseEstimator, TransformerMixin
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import linear_kernel


STOP_TOKENS = '[\W_]+|(corporation$)|(corp.$)|(corp$)|(incorporated$)|(inc.$)|(inc$)|(company$)|(common$)|(com$)'


class StringCluster(BaseEstimator, TransformerMixin):
    def __init__(self, ngram_size: int = 2, threshold: float = 0.8, stop_tokens: str = '[\W_]+'):
        self.ngram_size = ngram_size
        self.threshold = threshold
        self.stop_tokens = re.compile(stop_tokens)
        self._vec = TfidfVectorizer(analyzer='char_wb', ngram_range=(ngram_size, ngram_size))
        self.similarity_ = None
        self.labels_ = None

    def fit(self, X, y=None) -> "StringCluster":
        self.similarity_ = self._get_cosine_similarity(X, y)
        self.labels_ = self._get_labels()
        return self

    def transform(self, X, y=None):
        if y:
            return pd.Series(y)[self.labels_].reset_index(drop=True)
        return pd.Series(X)[self.labels_].reset_index(drop=True)

    def fit_transform(self, X, y=None, **fit_params):
        return self.fit(X, y).transform(X, y)

    def _get_labels(self):
        return np.where(self.similarity_ > self.threshold, 1., self.similarity_).argmax(1)

    def _get_cosine_similarity(self, X, y=None):
        if y:
            a, b = self._clean_series(X), self._clean_series(y)
        else:
            a, b = self._clean_series(X), self._clean_series(X)
        self._vec.fit(b)
        return linear_kernel(self._vec.transform(a), self._vec.transform(b))

    def _clean_series(self, X) -> "StringCluster":
        return pd.Series(X).apply(self._clean_string)

    def _clean_string(self, string: str) -> str:
        return self.stop_tokens.sub(' ', string.lower()).strip()


def test_small():
    series = pd.Series(
        ['Johnson & Johnson, Inc.', 'Johnson & Johnson Inc.', 'Johnson & Johnson Inc',
         'Johnson & Johnson', 'Intel Corp', 'Intel Corp.', 'Intel Corporation', 'Google',
         'Apple', 'Amazon', 'Amazon Inc', 'Comcast Inc.', 'Comcast Corp']
    )
    master = ['Johnson & Johnson', 'Intel Corp', 'Google', 'Apple Inc', 'Amazon', 'Comcast']
    c = StringCluster(ngram_size=2, stop_tokens=STOP_TOKENS)
    labs = c.fit_transform(series, )
    res = pd.DataFrame({'actual': series, 'label': labs})
    return res


if __name__ == '__main__':
    import time
    start = time.time()
    res = test_small()
    stop = time.time()
    print(res)
    print(f'Process took {stop-start} seconds.')