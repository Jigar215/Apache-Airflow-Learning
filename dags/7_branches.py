from airflow.sdk import dag, task

@dag(
    dag_id = "branch_dag"
)
def branch_dag():

    @task.python 
    def extract_data(**kwargs):
        print("Extracting Data.....")
        ti = kwargs['ti']
        extracted_data = {"api_extracted_data": [1, 2, 3],
                          "db_extracted_data": [4, 5, 6],
                          "s3_extracted_data": [7, 8, 9],
                          "is_weekend": False}
        ti.xcom_push(key='return_value', value=extracted_data)

    @task.python
    def transform_task_api(**kwargs):
        ti = kwargs['ti']
        api_extracted_data = ti.xcom_pull(task_ids = 'extract_data')['api_extracted_data']
        print(f"Tranforming API data... {api_extracted_data}")
        transformed_api_data = [i*2 for i in api_extracted_data]
        ti.xcom_push(key='return_value', value=transformed_api_data)
    
    @task.python
    def transform_task_db(**kwargs):
        ti = kwargs['ti']
        db_extracted_data = ti.xcom_pull(task_ids = 'extract_data')['db_extracted_data']
        print(f"Tranforming DB data... {db_extracted_data}")
        transformed_db_data = [i*2 for i in db_extracted_data]
        ti.xcom_push(key='return_value', value=transformed_db_data)
    
    @task.python
    def transform_task_s3(**kwargs):
        ti = kwargs['ti']
        s3_extracted_data = ti.xcom_pull(task_ids = 'extract_data')['s3_extracted_data']
        print(f"Tranforming S3 data... {s3_extracted_data}")
        transformed_s3_data = [i*2 for i in s3_extracted_data]
        ti.xcom_push(key='return_value', value=transformed_s3_data)

    # Creating the decider node
    @task.branch
    def decider_task(**kwargs):
        ti = kwargs['ti']
        weekend_flag=ti.xcom_pull(task_ids ='extract_data')['is_weekend']
        if weekend_flag == 'true':
            return 'no_load_task'
        else:
            return 'load_data'

    @task.bash
    def load_data(**kwargs):
        print("Loading data to the destination...")
        ti = kwargs['ti']
        api_data = ti.xcom_pull (task_ids = 'transform_task_api')
        db_data = ti.xcom_pull (task_ids = 'transform_task_db')
        s3_data = ti.xcom_pull (task_ids = 'transform_task_s3')

        return f"echo 'Loaded Data: {api_data}, {db_data}, {s3_data}'"
    
    @task.bash
    def no_load_task(**kwargs):
        print("No loading on weekends.....")
        return "echo 'No load task Executed"
    
    # Defining task dependencies
    extract = extract_data()
    transform_api = transform_task_api()    
    transform_db = transform_task_db()
    transform_s3 = transform_task_s3()
    load = load_data()
    no_load = no_load_task()

    extract >> [transform_api, transform_db, transform_s3] >> decider_task() >> [load, no_load]

# Instantiating the DAG
branch_dag()