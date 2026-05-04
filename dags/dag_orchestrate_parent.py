from dag_orchestrate_1 import first_orchestrator_dag
from dag_orchestrate_2 import second_orchestrator_dag
from airflow.sdk import dag, task
# from airflow.operators.trigger_dagrun import TriggerDagRunOperator  # Old way 
# Modern Import (Airflow 2.10+)
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator


'''Before triggering this parent DAG, ensure that both child DAGs
are enabled in the Airflow UI. Otherwise, this parent DAG will trigger
both the DAGs but they will be in the 'queued' state and won't execute
'''

@dag
def dag_orchestrate_parent():

    trigger_first_dag = TriggerDagRunOperator(
        task_id="trigger_first_orchestrator_dag",
        trigger_dag_id="first_orchestrator_dag",
        wait_for_completion=True,  # Wait for the first DAG to complete before triggering the second
        # Optional (This is slow): Waits for the triggered DAG to complete
    )

    trigger_second_dag = TriggerDagRunOperator(
        task_id="trigger_second_orchestrator_dag",
        trigger_dag_id="second_orchestrator_dag",
        wait_for_completion=True,  # Wait for the second DAG to complete before finishing the parent DAG
        # Optional (This is slow): Waits for the triggered DAG to complete
    )

    trigger_first_dag >> trigger_second_dag

# Instantiating the DAG
dag_orchestrate_parent()