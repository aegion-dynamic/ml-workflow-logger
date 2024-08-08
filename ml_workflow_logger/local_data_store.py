import json
from pathlib import Path

class LocalDataStore:

    def __init__(self, store_dir: Path = Path('local_store')) -> None:
        self.store_dir = store_dir
        self.store_dir.mkdir(parents=True, exist_ok=True)
        
    def sav_params(self, run_id: str, params: dict):
        params_path = self.store_dir / f"{run_id}_params.json"
        with params_path.open('w') as f:
            json.dump(params, f)

    def sav_metrics(self, run_id: str, metrics: dict):
        metrics_path = self.store_dir / f"{run_id}_metrics.json"
        with metrics_path.open('w') as f:
            json.dump(metrics, f)