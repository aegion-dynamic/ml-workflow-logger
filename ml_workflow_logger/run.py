import json
from pathlib import Path
from datetime import datetime
from pydantic import BaseModel
from typing import Dict, Any, Optional
from ml_workflow_logger.models.run_model import RunModel
from ml_workflow_logger.models.flow_model import FlowModel

class Run(BaseModel):
    def __init__(self, run_name: str, run_id: Optional[str] = None, flow_ref: Optional[FlowModel] = None, run_dir: Path = Path("./")) -> None:
        """Initialize the run with a name, reference to flow, and run directory."""
        self.run_id = run_id or datetime.now().strftime('%Y%m%d-%H%M%S')
        self.run_name = run_name
        self.run_dir = run_dir
        self.metrics: Dict[str, Any] = {}
        self.start_time = datetime.now()
        self.end_time: Optional[datetime] = None
        self.flow_ref = flow_ref
        self.status: str = "created"

    def create_run_dir(self, log_dir: Path) -> None:
        """Create a directory for the current run to store logs."""
        self.run_dir = log_dir / self.run_id
        self.run_dir.mkdir(parents=True, exist_ok=True)
        self._update_status: str = "Running"

    def add_metric(self, key: str, value: str):
        """Add a metrics to the run and save it."""
        self.metrics[key] = value
        self._save_metrics()

    def _save_metrics(self):
        """Save metrics to a JSON file."""
        metrics_path = self.run_dir / "metrics.json"
        with metrics_path.open('w') as f:
            json.dump(self.metrics, f)

    def end_run(self):
        """Mark the end of the run and set the end time."""
        self.end_time = datetime.now()
        self._update_status = "completed"
        # Optionally save run state (e.g., save to file or update DB)

    def update_status(self, _update_status: str) -> None:
        """Update the status of the run.

        Args:
            new_status (str): It will show the updated status
        """
        valid_statuses = {"created", "running", "completed", "failed"}
        if _update_status not in valid_statuses:
            raise ValueError(f"Invalid status '{_update_status}'. Valid statuses are: {valid_statuses}")
        self.status = _update_status
        print(f"Run '{self.run_name}' status updated to '{self.status}'.")

    def to_model(self) -> RunModel:
        """Convert the current run to a RunModel."""
        run_model = RunModel(
            name=self.run_name,
            start_time=self.start_time,
            end_time=self.end_time,
            metrics=self.metrics,
            flow_ref=self.flow_ref,
            status=self._update_status
        )
        return run_model
    
    def save_to_mongo(self, mongo_driver):
        """Save the current run to MongoDB uing the driver."""
        run_model = self.to_model()
        mongo_driver.save_run(run_model.to_dict())