import re
import pandas as pd
import numpy as np
from sklearn.base import BaseEstimator, TransformerMixin
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import linear_kernel


STOP_TOKENS = '[\W_]+|(corporation)|(corp)|(incorporated)|(inc)|(company)|(common)|(com)'


class StringCluster(BaseEstimator, TransformerMixin):
    def __init__(self, ngram_size: int = 2, similarity: float = 0.8, stop_tokens: str = '[\W_]+'):
        self.ngram_size = ngram_size
        self.similarity = similarity
        self.stop_tokens = re.compile(stop_tokens)
        self._vec = TfidfVectorizer(analyzer='char_wb', ngram_range=(ngram_size, ngram_size))
        self.dist_ = None
        self.labels_ = None

    def fit(self, X, y=None) -> "StringCluster":
        _ = y
        self.dist_ = self._get_dist_matrix(X)
        self.labels_ = self._get_labels()
        return self

    def transform(self, X):
        return X.reset_index(drop=True)[self.labels_]

    def _get_labels(self):
        return np.where(self.dist_ > self.similarity, 1., 0.).argmax(1)

    def _get_dist_matrix(self, X):
        a, b = self._clean_series(X), self._clean_series(X)
        return linear_kernel(self._vec.fit_transform(a), self._vec.fit_transform(b))

    def _clean_series(self, X) -> "StringCluster":
        return pd.Series(X).apply(self._clean_string)

    def _clean_string(self, string: str) -> str:
        return self.stop_tokens.sub(' ', string.lower())