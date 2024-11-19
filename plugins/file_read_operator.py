"""
Custom operator for reading files in Apache Airflow.
"""

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.plugins_manager import AirflowPlugin


class FileReadOperator(BaseOperator):
    """
    Custom operator to read content from a specified file path.
    """

    @apply_defaults
    def __init__(self, file_path, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.file_path = file_path

    def execute(self, context):
        """
        Executes the file read operation.
        """
        self.log.info(f"Reading from file: {self.file_path}")
        try:
            with open(self.file_path, 'r', encoding='utf-8') as f:
                data = f.read()
                self.log.info(f"File content: {data}")
            return data
        except FileNotFoundError:
            self.log.error(f"File not found: {self.file_path}")
            raise
        except IOError as e:
            self.log.error(f"Error reading file {self.file_path}: {str(e)}")
            raise

class FileReadPlugin(AirflowPlugin):
    """
    Plugin to register the FileReadOperator with Airflow.
    """
    name = "file_read_plugin"  # Unique identifier for the plugin
    operators = [FileReadOperator] # List of operators to register