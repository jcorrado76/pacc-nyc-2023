# deployment.py

import module_103_lab as flow_code
from prefect.deployments import Deployment

deployment = Deployment.build_from_flow(
    flow=flow_code.transform_stock_data,
    name="transform-stock-data-python",
    work_queue_name="default",
)

if __name__ == "__main__":
    deployment.apply()
