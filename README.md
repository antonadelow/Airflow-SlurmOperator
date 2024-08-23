
# SlurmOperator README

## Overview
`SlurmOperator` is a custom Apache Airflow operator designed to submit and monitor jobs on a SLURM workload manager. This operator simplifies the process of running scripts in a SLURM environment by automating job submission, monitoring, and logging through Airflow.

## Key Features
- **Job Submission:** Automates the creation and submission of SLURM batch scripts.
- **Job Monitoring:** Periodically checks the status of the SLURM job and logs output and errors.
- **Timeout Handling:** Manages job timeout and raises an exception if the job exceeds the specified duration.

## Installation
To use `SlurmOperator`, ensure that your environment has the following dependencies:
- Python 3.x
- Apache Airflow
- SLURM workload manager
- Conda (if using conda environments)

## Usage

### Initialization
To use `SlurmOperator` in your Airflow DAG, initialize it with the required parameters.

```python
from your_module import SlurmOperator

slurm_task = SlurmOperator(
    task_id='run_slurm_task',
    script='path/to/your_script.py',
    conda_path='/path/to/conda.sh',
    env='your_conda_environment',
    log_path='/path/to/log_directory',
    script_args=['arg1', 'arg2'],
    mem_per_cpu='4600M',
    cpus_per_task=8,
    num_gpus=1,
    poke_interval=60,
    timeout=3600,
    dag=your_dag
)
```

### Parameters

- **script** (`str`): Path to the Python script that needs to be executed on SLURM.
- **conda_path** (`str`): Path to the Conda initialization script (e.g., `/path/to/conda.sh`).
- **env** (`str`): Name of the Conda environment to activate.
- **log_path** (`str`): Directory where SLURM output and error logs will be stored.
- **script_args** (`list`, optional): List of arguments to pass to the script.
- **mem_per_cpu** (`str`, optional): Memory allocated per CPU (e.g., '4600M').
- **cpus_per_task** (`int`, optional): Number of CPUs allocated per task.
- **num_gpus** (`int`, optional): Number of GPUs required for the task.
- **poke_interval** (`int`, optional): Time (in seconds) between job status checks.
- **timeout** (`int`, optional): Maximum duration (in seconds) for the job before timing out.
- **args** (`list`, optional): Additional positional arguments for `BaseOperator`.
- **kwargs** (`dict`, optional): Additional keyword arguments for `BaseOperator`.

### Methods

- **submit_slurm_job()**: Submits the SLURM job using a batch script generated from the provided parameters.
- **monitor_slurm_job()**: Periodically monitors the SLURM job status, checking if it is still running or has completed.
- **log_slurm()**: Logs the SLURM output and error messages during job execution.
- **execute(context)**: Executes the SLURM job by calling `submit_slurm_job()` and `monitor_slurm_job()`.

### Example DAG
Here is an example of how to include `SlurmOperator` in an Airflow DAG:

```python
from airflow import DAG
from datetime import datetime
from your_module import SlurmOperator

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
}

dag = DAG(
    'example_slurm_dag',
    default_args=default_args,
    description='An example DAG using SlurmOperator',
    schedule_interval='@daily',
)

slurm_task = SlurmOperator(
    task_id='run_slurm_task',
    script='path/to/your_script.py',
    conda_path='/path/to/conda.sh',
    env='your_conda_environment',
    log_path='/path/to/log_directory',
    dag=dag
)

slurm_task
```

## License
This project is licensed under the MIT License.
