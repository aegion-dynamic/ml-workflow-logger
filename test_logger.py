from ml_workflow_logger.logger import MLWorkflowLogger
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
logger = MLWorkflowLogger(log_dir='logs', graph=workflow_graph)

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
logger.generate_benchmark_csv()
