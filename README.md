# BigQuery_Python_Example

### Steps to run the code:

1. export GOOGLE_APPLICATION_CREDENTIALS=/path_of_bigquery_service_account_auth_file

2. Create a project: candidate-eval and within that 
   project create a new dataset with name 'my_new_dataset'

3. First part of  Part A
   - run the file a1_evaluation.py
   - command: python a1_evaluation.py

   it will fetch the result and upload the result in table a1_results

4. Second part of Part A
   - run the file a2_evaluation.py
   - command: python a2_evaluation.py

   it will fetch the result and upload the result in table a2_results
   
   Getting excess error while creating table in project ‘candidate-evaluation. So, I have created project candidate-eval.
