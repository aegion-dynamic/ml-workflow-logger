from ml_workflow_logger.db_config import DBConfig, get_mongodb_collection
from ml_workflow_logger.logger import MLWorkFlowLogger
import networkx as nx

# Create a graph to visualize the workflow
workflow_graph = nx.DiGraph()

# Add nodes and edges to the workflow graph
workflow_graph.add_node("start", description="Starting the workflow")
workflow_graph.add_node("preprocessing", description="Data Preprocessing")
workflow_graph.add_node("training", description="Model Training")
workflow_graph.add_node("evaluation", description="Model Evaluation")
workflow_graph.add_edges_from([("start", "preprocessing"), ("preprocessing", "training"), ("training", "evaluation")])

# Initialize the logger
logger = MLWorkFlowLogger(log_dir='logs', graph=workflow_graph)

# Log some workflow steps
logger.log_workflow_step('start', {'status': 'completed'})
logger.log_workflow_step('preprocessing', {'status': 'completed'})
logger.log_workflow_step('training', {'status': 'in progress'})
logger.log_workflow_step('evaluation', {'status': 'pending'})

# Visualize the workflow
logger.visualize_workflow()

# Log some metrics
logger.log_metrics({'accuracy': 0.95, 'loss': 0.05})

# Generate local benchmark CSV
logger.save_dataframe(data)

# End the run
logger.end_run()


config = DBConfig(database="your_database_name", collection="your_collection_name")
collection = get_mongodb_collection(config)
print(f"Connected to collection: {config.collection}")