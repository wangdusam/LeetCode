import ast
import os
import sys
from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
pth = os.path.abspath(os.getcwd())
sys.path.append(pth)
pth1 = os.path.dirname(os.path.abspath(__file__))
sys.path.append(pth1)
from read_json import fn_runtestcases


class TestSolutionOperator(BaseOperator):

    @apply_defaults
    def __init__(
            self,
            directory: str,
            confprojectlistfile: str,
            *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.directory = directory
        self.confprojectlistfile = confprojectlistfile

    def execute(self, context):
        fn_runtestcases(self.directory, self.confprojectlistfile)

