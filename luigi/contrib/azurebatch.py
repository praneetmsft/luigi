# -*- coding: utf-8 -*-
#
# Copyright 2018 Outlier Bio, LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
Azure Batch wrapper for Luigi

From the Azure website:

    Use Azure Batch to run large-scale parallel and high-performance computing (HPC) batch 
    jobs efficiently in Azure. Azure Batch creates and manages a pool of compute nodes (virtual machines), 
    installs the applications you want to run, and schedules jobs to run on the nodes. 
    Developers can use Batch as a platform service to build SaaS applications or client 
    apps where large-scale execution is required. For example, build a service with Batch 
    to run a Monte Carlo risk simulation for a financial services company, or a service to process many images.


See `Azure Batch Documents` for more details.

Requires:

- azure batch package: ``pip install azure-batch>=6.0.0``
- azure blob storage: ``pip install azure-storage-blob>=1.3.1``
- Azure Batch Account details

Written and maintained by:
    Praneet Solanki @praneetmsft
    Emmanuel Awa @awaemmanuel

"""

import luigi
import azure.batch.batch_auth as batch_auth
import azure.batch.batch_service_client as batch

class AzureBatchClient(luigi.Task):

    def __init__(self):
        self._BATCH_ACCOUNT_NAME ='' # Your batch account name 
        self._BATCH_ACCOUNT_KEY = '' # Your batch account key
        self._BATCH_ACCOUNT_URL = '' # Your batch account URL
        self._STORAGE_ACCOUNT_NAME = '' # Your storage account name
        self._STORAGE_ACCOUNT_KEY = '' # Your storage account key
        self._POOL_ID = '' # Your Pool ID
        self._POOL_NODE_COUNT = 2 # Pool node count
        self._POOL_VM_SIZE = '' # VM Type/Size
        self._JOB_ID = '' # Job ID
        self._STANDARD_OUT_FILE_NAME = 'stdout.txt' # Standard Output file
        self._credentials = batch_auth.SharedKeyCredentials(self._BATCH_ACCOUNT_NAME,
                                                 self._BATCH_ACCOUNT_KEY)
        self.batch_client = batch.BatchServiceClient(self._credentials,
        batch_url=self._BATCH_ACCOUNT_URL)

    @property
    def auth_method(self):
        """
        This returns the authentication for the Azure batch account
        """
        ...

    @property
    def max_retrials(self):
        """
        Maximum number of retrials in case of failure.
        """
        ...

    def submit_job_and_add_task(self):
        """Submits a job to the Azure Batch service and adds a simple task.

        :param batch_client: The batch client to use.
        :type batch_client: `batchserviceclient.BatchServiceClient`
        :param str job_id: The id of the job to create.
        """
        ...

    def execute_sample(global_config, sample_config):
        """Executes the sample with the specified configurations.

        :param global_config: The global configuration to use.
        :type global_config: `configparser.ConfigParser`
        :param sample_config: The sample specific configuration to use.
        :type sample_config: `configparser.ConfigParser`
        """
        ...

    def on_success(self):
        """
        Override for doing custom completion handling for a larger class of tasks
        This method gets called when :py:meth:`run` completes without raising any exceptions.
        The returned value is json encoded and sent to the scheduler as the `expl` argument.
        Default behavior is to send an None value"""
        pass

    def on_failure(self, exception):
        """
        Override for custom error handling.
        This method gets called if an exception is raised in :py:meth:`run`.
        The returned value of this method is json encoded and sent to the scheduler
        as the `expl` argument. Its string representation will be used as the
        body of the error email sent out if any.
        Default behavior is to return a string representation of the stack trace.
        """
        ...

    def get_tasks_status(self):
        """Get the status of all the tasks under one Job"""
        ...

    def print_task_output(self):
        """Prints the stdout.txt file for each task in the job.

        :param batch_client: The batch client to use.
        :type batch_client: `batchserviceclient.BatchServiceClient`
        :param str job_id: The id of the job with task output files to print.
        """
        ...

    def wait_for_tasks_to_complete(self):
        """
        Returns when all tasks in the specified job reach the Completed state.
        :param batch_service_client: A Batch service client.
        :type batch_service_client: `azure.batch.BatchServiceClient`
        :param str job_id: The id of the job whose tasks should be to monitored.
        :param timedelta timeout: The duration to wait for task completion. If all
        tasks in the specified job do not reach Completed state within this time
        period, an exception will be raised.
        """
        ...

    def print_batch_exception(self):
        """
        Prints the contents of the specified Batch exception.

        :param batch_exception:
        """
        ...
    