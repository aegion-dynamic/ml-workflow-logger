import json
from pathlib import Path
from datetime import datetime

class Run:
    def __init__(self, run_name: str):
        self.run_id = datetime.now().strftime('%Y%m%d-%H%M%S')
        self.run_name = run_name
        self.run_dir = Path()
        self.params = {}
        self.metrics = {}

    def create_run_dir(self, log_dir: Path) -> None:
        self.run_dir = log_dir / self.run_id
        self.run_dir.mkdir(parents=True, exist_ok=True)

    def add_param(self, key: str, value: str):
        self.params[key] = value
        self._save_params()

    def add_metric(self, key: str, value: str):
        self.metrics[key] = value
        self._save_metrics()

    def _save_params(self):
        params_path = self.run_dir / "params.json"
        with params_path.open('w') as f:
            json.dump(self.params, f)

    def _save_metrics(self):
        metrics_path = self.run_dir / "metrics.json"
        with metrics_path.open('w') as f:
            json.dump(self.metrics, f)


