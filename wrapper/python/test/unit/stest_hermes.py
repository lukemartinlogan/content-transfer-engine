from unittest import TestCase
from py_hermes import Hermes, TRANSPARENT_HERMES
import pathlib
import os

class AccessInfo:
    @staticmethod
    def unique(id):
        return f'{id.node_id}.{id.unique}'

    def __init__(self, access_info):
        self.tag_id_ = self.unique(access_info.tag_id_)
        self.blob_id_ = self.unique(access_info.blob_id_)
        self.score_ = float(access_info.score_)
        self.blob_name_ = str(access_info.blob_name_)
        self.acc_off_ = float(access_info.acc_off_)
        self.acc_size_ = float(access_info.acc_size_)
        self.blob_size_ = float(access_info.blob_size_)

class AccessPatternLog:
    def __init__(self):
        TRANSPARENT_HERMES()
        self.hermes = Hermes()
        self.stats = []

    def collect(self):
        access_pattern = self.hermes.ParseAccessPattern()
        self.stats = [AccessInfo(access_info)
                      for access_info in access_pattern]


def test_access_log():
    mdm = AccessPatternLog()
    mdm.collect()

test_access_log()
