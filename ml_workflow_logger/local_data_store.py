import json
from pathlib import Path, PosixPath
from typing import Any, Dict

import pandas as pd


class LocalDataStore:

    def __init__(self, store_dir: Path = Path("local_store")) -> None:
        """Initialize the local data store directory."""
        self.store_dir = store_dir
        self.store_dir.mkdir(parents=True, exist_ok=True)

    def save_params(self, run_id: str, params: dict):
        """Save parameters to a JSON file."""
        params_path = self.store_dir / f"{run_id}_params.json"
        with params_path.open("w") as f:
            json.dump(params, f, indent=4)

    def save_metrics(self, run_id: str, metrics: dict):
        """Save metrics to a JSON file."""
        metrics_path = self.store_dir / f"{run_id}_metrics.json"
        with metrics_path.open("w") as f:
            json.dump(metrics, f, indent=4)

    def save_step(self, run_id: str, step_data: dict):
        step_path = self.store_dir / f"{run_id}_steps.json"
        if step_path.exists():
            with step_path.open("r+") as f:
                try:
                    existing_data = json.load(f)
                    if not isinstance(existing_data, list):
                        existing_data = []
                except json.JSONDecodeError:
                    existing_data = []

                existing_data.append(step_data)
                f.seek(0)
                f.truncate()
                json.dump(existing_data, f, indent=4)
        else:
            with step_path.open("w") as f:
                json.dump([step_data], f, indent=4)

    def save_run(self, run_id: str, run_data: Dict[str, Any]):
        # Convert any PosixPath objects to strings before saving
        run_data = {key: str(value) if isinstance(value, PosixPath) else value for key, value in run_data.items()}

        run_path = self.store_dir / f"{run_id}_run.json"
        with run_path.open("w") as f:
            json.dump(run_data, f, indent=4)

    def save_flow(self, flow_id: str, flow_data: Dict[str, Any]):
        """Save flow data to JSON file."""
        flow_path = self.store_dir / f"{flow_id}_flow.json"
        with flow_path.open("w") as f:
            json.dump(flow_data, f, indent=4)

    def save_dataframe(self, run_id: str, df: pd.DataFrame):
        """Save the benchmark data to a CSV file."""
        df_path = self.store_dir / f"{run_id}_benchmark.csv"
        df.to_csv(df_path, index=False)
