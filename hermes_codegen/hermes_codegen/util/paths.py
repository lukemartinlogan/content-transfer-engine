import os,sys
import pathlib

def GetHermes():
    util_path = pathlib.Path(__file__).parent.resolve()
    codegen_path1 = os.path.dirname(util_path)
    codegen_path2 = os.path.dirname(codegen_path1)
    hermes_path = os.path.dirname(codegen_path2)
    return hermes_path

HERMES_ROOT = GetHermes()
