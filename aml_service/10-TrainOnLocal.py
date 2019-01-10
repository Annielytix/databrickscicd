from azureml.core.runconfig import RunConfiguration
from azureml.core import Workspace
from azureml.core import Experiment
from azureml.core import ScriptRunConfig
import os, json

# Get workspace
ws = Workspace.from_config()

# Attach Experiment
experiment_name = 'devops-ai-demo'
exp = Experiment(workspace  = ws, name = experiment_name)
print(exp.name, exp.workspace.name, sep = '\n')

# Editing a run configuration property on-fly.
run_config_user_managed = RunConfiguration()
run_config_user_managed.environment.python.user_managed_dependencies = True

print("Submitting an experiment.")
src = ScriptRunConfig(source_directory = './code', script = 'training/train.py', run_config = run_config_user_managed)
run = exp.submit(src)

# Shows output of the run on stdout.
run.wait_for_completion(show_output = True)

# if run.get_status() != 'Completed':
#   raise Exception('Training on local failed with following run status: {}'.format(run.get_status()))

# Writing the run id to /aml_config/run_id.json

run_id={}
run_id['run_id'] = run.id
run_id['experiment_name'] = run.experiment.name
with open('aml_config/run_id.json', 'w') as outfile:
  json.dump(run_id,outfile)
