from airflow.operators.python import PythonVirtualenvOperator
from airflow.utils.decorators import apply_defaults
from attr import has
import cloudpickle
from typing import Callable, Dict, Iterable, List, Optional, Union


class PythonVirtualenvCloudOperator(PythonVirtualenvOperator):
    @apply_defaults
    def __init__(  # pylint: disable=too-many-arguments
        self,
        *,
        python_callable: Callable,
        requirements: Optional[Iterable[str]] = None,
        python_version: Optional[Union[str, int, float]] = None,
        system_site_packages: bool = True,
        op_args: Optional[List] = None,
        op_kwargs: Optional[Dict] = None,
        string_args: Optional[Iterable[str]] = None,
        templates_dict: Optional[Dict] = None,
        templates_exts: Optional[List[str]] = None,
        **kwargs,
    ):
        super().__init__(
            python_callable=python_callable,
            requirements=requirements,
            python_version=python_version,
            use_dill=False,
            system_site_packages=system_site_packages,
            op_args=op_args,
            op_kwargs=op_kwargs,
            string_args=string_args,
            templates_dict=templates_dict,
            templates_exts=templates_exts,
            **kwargs,
        )
        if not self.system_site_packages and 'cloudpickle' not in self.requirements:
            self.requirements.append('cloudpickle')
        self.pickling_library = cloudpickle

    def execute_callable(self):
        for key, value in self.op_kwargs.items():
            self.log.info(f'{key}: {value} - {type(value)}')
            if key == 'task':
                for k, v in value.__dict__.items():
                    self.log.info(f'TASK {k}: {v} - {type(v)}')
        #             if k == '_dag':
        #                 for a, b in v.__dict__.items():
        #                     self.log.info(f'TASK _DAG {a}: {b} - {type(b)}')
        #                     self.pickling_library.dumps(b)
        #             self.pickling_library.dumps(v)
        #     self.pickling_library.dumps(value)
        # self.op_kwargs['xcom_pull'] = self.op_kwargs['task'].xcom_pull
        # self.op_kwargs['task'].__dict__.pop('_dag')
        # self.op_kwargs.pop('task')
        # self.op_kwargs.pop('dag')
        return super().execute_callable()   
