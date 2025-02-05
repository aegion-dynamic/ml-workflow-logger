from math import log
from ml_workflow_logger.flow import Flow
from ml_workflow_logger.logger import MLWorkFlowLogger


def algorithm1(param1: float, param2: float, logger_instance) -> float:
    """A simple algorithm that calculates the log of the sum of two numbers."""

    # Step Start

    logger_instance.save_flow_record(
        "run1", "step_start", {"param1": param1, "param2": param2})

    # Step 1

    result1 = log(param1 + param2)

    logger_instance.save_flow_record("run1", "step1", {"result1": result1})

    # Step 2

    result2 = result1 * 2

    logger_instance.save_flow_record("run1", "step2", {"result2": result2})

    # Step 3

    result3 = result2 / 2

    logger_instance.save_flow_record("run1", "step3", {"result3": result3})

    # Step 4

    result4 = result3 + 1

    logger_instance.save_flow_record("run1", "step4", {"result4": result4})

    # Step End

    result = result4

    logger_instance.save_flow_record(
        "run1", "step_end", {"final_result": result})

    return result


logger = MLWorkFlowLogger()

# Define the flow

logger.add_new_flow(flow_name="algorithm1")

# Add a steps to the flow

logger.add_new_step(flow_id="algorithm1", step_name="step_start")
logger.add_new_step(flow_id="algorithm1", step_name="step1")
logger.add_new_step(flow_id="algorithm1", step_name="step2")
logger.add_new_step(flow_id="algorithm1", step_name="step3")
logger.add_new_step(flow_id="algorithm1", step_name="step4")
logger.add_new_step(flow_id="algorithm1", step_name="step_end")

print("Flow created successfully")


# Create a run for the flow

for i in range(10):
    print(f"Starting run {i+1}")
    run_id = f"run{i+1}"
    run = logger.start_new_run(run_id)
    # Start the algorithm
    algorithm1(0.1, 0.2, logger)
