

from typing import Any, Dict


class Flow:
    def __init__(self, flow_name: str, run_id: str):
        self.flow_name = flow_name
        self.run_id = run_id

    def log_step(self, step_name: str, step_data: Dict[str, Any]):
        raise NotImplementedError("Subclasses must implement this method")