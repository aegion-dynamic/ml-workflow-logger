import json
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, Optional
from ml_workflow_logger.models.run_model import RunModel
from ml_workflow_logger.models.flow_model import FlowModel

class Run:
    def __init__(self, run_name: str, flow_ref: Optional[FlowModel] = None, run_dir: Path = Path("./")) -> None:
        """Initialize the run with a name, reference to flow, and run directory."""
        self.run_id = datetime.now().strftime('%Y%m%d-%H%M%S')
        self.run_name = run_name
        self.run_dir = run_dir
        self.params: Dict[str, Any] = {}
        self.metrics: Dict[str, Any] = {}
        self.start_time = datetime.now()
        self.end_time: Optional[datetime] = None
        self.flow_ref = flow_ref

    def create_run_dir(self, log_dir: Path) -> None:
        """Create a directory for the current run to store logs."""
        self.run_dir = log_dir / self.run_id
        self.run_dir.mkdir(parents=True, exist_ok=True)

    def add_param(self, key: str, value: str):
        """Add a parameter to the run and save it."""
        self.params[key] = value
        self._save_params()

    def add_metric(self, key: str, value: str):
        """Add a metrics to the run and save it."""
        self.metrics[key] = value
        self._save_metrics()

    def _save_params(self):
        """Save parameters to a JSON file"""
        params_path = self.run_dir / "params.json"
        with params_path.open('w') as f:
            json.dump(self.params, f)

    def _save_metrics(self):
        """Save metrics to a JSON file."""
        metrics_path = self.run_dir / "metrics.json"
        with metrics_path.open('w') as f:
            json.dump(self.metrics, f)

    def end_run(self):
        """Mark the end of the run and set the end time."""
        self.end_time = datetime.now()
        # Optionally save run state (e.g., save to file or update DB)

    def to_model(self) -> RunModel:
        """Convert the current run to a RunModel."""
        run_model = RunModel(
            name=self.run_name,
            start_time=self.start_time,
            end_time=self.end_time,
            params=self.params,
            metrics=self.metrics,
            flow_ref=self.flow_ref
        )

        return run_model
    
    def save_to_mongo(self, mongo_driver):
        """Save the current run to MongoDB uing the driver."""
        run_model = self.to_model()
        mongo_driver.save_run(run_model.to_dict())