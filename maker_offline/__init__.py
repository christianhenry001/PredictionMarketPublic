"""Offline replay harness. Places nothing, connects to nothing.

tape.py loads recorded books and trades as one time-ordered stream on the exchange clock; fills.py decides
which virtual resting orders fill, trade by trade, under conservative and optimistic queue models. The
quoting policies replayed through it are not part of this repository.
"""
