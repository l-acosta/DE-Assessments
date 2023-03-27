SCRIPT_PARENT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && cd .. && pwd)
export PROJECT_ABS_PATH=$SCRIPT_PARENT_DIR # replacement of DATA_PRODUCT_PATH
echo "PROJECT_ABS_PATH: $PROJECT_ABS_PATH"

export AIRFLOW_HOME=$PROJECT_ABS_PATH/airflow
echo "AIRFLOW_HOME: $AIRFLOW_HOME"
