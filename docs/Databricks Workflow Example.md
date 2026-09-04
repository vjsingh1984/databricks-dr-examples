## Example: Creating a Databricks Workflow
In this section, we'll demonstrate how you can run one of the above scripts in a workflow fashion within Databricks. In this example we will be creating a workflow using the sync_catalogs_and_schemas.py. 

*This workflow assumes that you've done the prerequisites by ensuring that the external locations and credentials are already created/synced.*

1. In the Databricks UI, navigate to the Workspace Tab in the UI, click create Git Folder from the right hand side, and add in this repository

![gitfolder](images/workflow_gitfolder.png)


2. Configure non-secret settings through job environment variables or `common.py`. Prefer
   `SOURCE` and `TARGET` unified-auth profiles for an interactive job, or workload identity
   federation for automation. Do not put PATs or other credentials in `common.py`; the
   `source_pat` and `target_pat` fields are legacy-only migration fallbacks.

![common](images/workflow_commonpy.png)

3. Inside the git folder, navigate to the data directory and edit the catalog_mapping.csv and schema_mapping.csv directly or download into your csv editor. If you downloaded the files to edit, ensure you bring the newly editted file back into this folder.

![catalog](images/workflow_catalogmapping.png)
![schema](images/workflow_schemamapping.png)

4. Inside the Databricks UI, on the left hand side navigate to workflows, and create a new job with the following information:

![createjob](images/workflow_createjob.png)

5. In the above screenshot the task type is Python script, the source is Workspace, and the
   selected file is `sync_catalogs_and_schemas.py` in the Git folder. Serverless or classic
   compute can be used, but the compute must reach both workspace API origins.

6. Under Environments and Variables, install this repository as a package (which pins
   `databricks-sdk==0.123.0`) or install that SDK version explicitly. Add the required
   `DR_SYNC_*` variables without secret values; inject credentials through the Databricks
   secret/environment facilities. Click Confirm and Create Task.

![libraries](images/workflow_libraries.png)

7. Add `--dry-run` as a task parameter and click Run Now. Review the plan before running a
   second time without `--dry-run`:

![run](images/workflow_run.png)

8. The successful run should yield similar results:

![results](images/workflow_results.png)
