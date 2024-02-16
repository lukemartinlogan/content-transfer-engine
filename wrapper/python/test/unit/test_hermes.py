from unittest import TestCase
from py_hermes import Hermes, TRANSPARENT_HERMES
import pathlib
import os

class AccessPatternLog:
    def __init__(self):
        TRANSPARENT_HERMES()
        self.hermes = Hermes()
        self.stats = []

    def collect(self):
        access_pattern = self.hermes.ParseAccessPattern()
        self.stats = list(access_pattern)

class TestHermes(TestCase):
    def test_metadata_query(self):
        TRANSPARENT_HERMES()
        hermes = Hermes()
        mdm = hermes.CollectMetadataSnapshot()
        print(mdm.blob_info)
        print("Done")

    def test_access_log(self):
        mdm = AccessPatternLog()
        mdm.collect()
