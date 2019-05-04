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
import os
import io
import sys
import time
import datetime
import luigi
import azure.storage.blob as azureblob
import azure.batch.batch_auth as batch_auth
import azure.batch.batch_service_client as batch
import azure.batch.models as batchmodels


RESOURCE_SUFFIX = datetime.datetime.utcnow().strftime("%Y%m%d-%H%M%S")
TASK_POOL_NODE_COUNT = 2
TASK_POOL_VM_SIZE = "STANDARD_D2_V2"
TASK_POOL_ID = "AzureBatch-Pool-Id-{}".format(RESOURCE_SUFFIX)
TASK_JOB_ID = "AzureBatch-Pool-Id-{}".format(RESOURCE_SUFFIX)
TASK_TIME_OUT = 30


class AzureBatchClient(object):
    def __init__(
        self,
        batch_account_name,
        batch_account_key,
        batch_account_url,
        storage_account_name,
        storage_account_key,
        output_path,
        pool_id,
        job_id,
        pool_node_count,
        pool_vm_size,
        **kwargs
    ):
        self._BATCH_ACCOUNT_NAME = batch_account_name  # Your batch account name
        self._BATCH_ACCOUNT_KEY = batch_account_key  # Your batch account key
        self._BATCH_ACCOUNT_URL = batch_account_url  # Your batch account URL
        self._STORAGE_ACCOUNT_NAME = storage_account_name  # Your storage account name
        self._STORAGE_ACCOUNT_KEY = storage_account_key  # Your storage account key
        self._OUTPUT_PATH = output_path  # Output files path on your storage account
        self._POOL_ID = pool_id  # Your Pool ID
        self._POOL_NODE_COUNT = pool_node_count  # Pool node count
        self._POOL_VM_SIZE = pool_vm_size  # VM Type/Size
        self._JOB_ID = job_id  # Job ID
        self._STANDARD_OUT_FILE_NAME = "stdout.txt"  # Standard Output file
        self.kwargs = kwargs

    @property
    def _credentials(self):
        """
        This returns the authentication for the Azure batch account
        """
        return batch_auth.SharedKeyCredentials(
            self._BATCH_ACCOUNT_NAME, self._BATCH_ACCOUNT_KEY
        )

    @property
    def client(self):
        """
        This returns a property of an Azure Batch Service Client
        """
        return batch.BatchServiceClient(
            self._credentials, batch_url=self._BATCH_ACCOUNT_URL
        )

    @property
    def blob_client(self):
        """
        This returns the authentication for the Azure batch account
        """
        return azureblob.BlockBlobService(
            account_name=self._STORAGE_ACCOUNT_NAME,
            account_key=self._STORAGE_ACCOUNT_KEY,
        )

    @property
    def max_retrials(self):
        """
        Maximum number of retrials in case of failure.
        """
        ...

    def upload_file_to_container(self, container_name, file_path):
        """
        Uploads a local file to an Azure Blob storage container.

        :param str container_name: The name of the Azure Blob storage container.
        :param str file_path: The local path to the file.
        :rtype: `azure.batch.models.ResourceFile`
        :return: A ResourceFile initialized with a SAS URL appropriate for Batch
        tasks.
        """
        blob_name = os.path.basename(file_path)

        print(
            "Uploading file {} to container [{}]...".format(file_path, container_name)
        )

        self.blob_client.create_blob_from_path(container_name, blob_name, file_path)

        sas_token = self.blob_client.generate_blob_shared_access_signature(
            container_name,
            blob_name,
            permission=azureblob.BlobPermissions.READ,
            expiry=datetime.datetime.utcnow() + datetime.timedelta(hours=2),
        )

        sas_url = self.blob_client.make_blob_url(
            container_name, blob_name, sas_token=sas_token
        )

        return batchmodels.ResourceFile(http_url=sas_url, file_path=blob_name)

    def get_container_sas_token(self, container_name, blob_permissions):
        """
        Obtains a shared access signature granting the specified permissions to the
        container.
        :param str container_name: The name of the Azure Blob storage container.
        :param BlobPermissions blob_permissions:
        :rtype: str
        :return: A SAS token granting the specified permissions to the container.
        """
        # Obtain the SAS token for the container, setting the expiry time and
        # permissions. In this case, no start time is specified, so the shared
        # access signature becomes valid immediately.
        container_sas_token = self.blob_client.generate_container_shared_access_signature(
            container_name,
            permission=blob_permissions,
            expiry=datetime.datetime.utcnow() + datetime.timedelta(hours=2),
        )

        return container_sas_token

    def create_pool(self):
        """
        Creates a pool of compute nodes with the specified OS settings.
        :param batch_service_client: A Batch service client.
        :type batch_service_client: `azure.batch.BatchServiceClient`
        :param str pool_id: An ID for the new pool.
        :param str publisher: Marketplace image publisher
        :param str offer: Marketplace image offer
        :param str sku: Marketplace image sku
        """
        print("Creating pool [{}]...".format(self._POOL_ID))

        # Create a new pool of Linux compute nodes using an Azure Virtual Machines
        # Marketplace image. For more information about creating pools of Linux
        # nodes, see:
        # https://azure.microsoft.com/documentation/articles/batch-linux-nodes/
        new_pool = batch.models.PoolAddParameter(
            id=self._POOL_ID,
            virtual_machine_configuration=batchmodels.VirtualMachineConfiguration(
                image_reference=batchmodels.ImageReference(
                    publisher="Canonical",
                    offer="UbuntuServer",
                    sku="18.04-LTS",
                    version="latest",
                ),
                node_agent_sku_id="batch.node.ubuntu 18.04",
            ),
            vm_size=self._POOL_VM_SIZE,
            target_dedicated_nodes=self._POOL_NODE_COUNT,
        )
        self.client.pool.add(new_pool)

    def create_job(self):
        """
        Creates a job with the specified ID, associated with the specified pool.
        :param batch_service_client: A Batch service client.
        :type batch_service_client: `azure.batch.BatchServiceClient`
        :param str job_id: The ID for the job.
        :param str pool_id: The ID for the pool.
        """
        print("Creating job [{}]...".format(self._JOB_ID))

        job = batch.models.JobAddParameter(
            id=self._JOB_ID,
            pool_info=batch.models.PoolInformation(pool_id=self._POOL_ID),
        )

        self.client.job.add(job)

    def add_tasks(self, input_files, command):
        """
        Adds a task for each input file in the collection to the specified job.
        :param batch_service_client: A Batch service client.
        :type batch_service_client: `azure.batch.BatchServiceClient`
        :param str job_id: The ID of the job to which to add the tasks.
        :param list input_files: A collection of input files. One task will be
         created for each input file.
        :param str command: command to run in each task e.g. "python main.py"
        :param output_container_sas_token: A SAS token granting write access to
        the specified Azure Blob storage container.
        """
        print("Adding {} tasks to job [{}]...".format(len(input_files), self._JOB_ID))
        tasks = list()

        if len(input_files) > 0:
            for idx, input_file in enumerate(input_files):
                # command = '/bin/bash -c "cat {}"'.format(input_file.file_path)
                tasks.append(
                    batch.models.TaskAddParameter(
                        id="Task-{}".format(idx),
                        command_line=command,
                        resource_files=[input_file],
                    )
                )
        else:
            tasks.append(
                batch.models.TaskAddParameter(
                    id="Task-{}".format(RESOURCE_SUFFIX), command_line=command
                )
            )

        self.client.task.add_collection(self._JOB_ID, tasks)

    def wait_for_tasks_to_complete(self, timeout):
        """
        Returns when all tasks in the specified job reach the Completed state.

        :param batch_service_client: A Batch service client.
        :type batch_service_client: `azure.batch.BatchServiceClient`
        :param str job_id: The id of the job whose tasks should be to monitored.
        :param timedelta timeout: The duration to wait for task completion. If all
        tasks in the specified job do not reach Completed state within this time
        period, an exception will be raised.
        """
        timeout_expiration = datetime.datetime.now() + timeout

        print(
            "Monitoring all tasks for 'Completed' state, timeout in {}...".format(
                timeout
            ),
            end="",
        )

        while datetime.datetime.now() < timeout_expiration:
            print(".", end="")
            sys.stdout.flush()
            tasks = self.client.task.list(self._JOB_ID)

            incomplete_tasks = [
                task for task in tasks if task.state != batchmodels.TaskState.completed
            ]
            if not incomplete_tasks:
                print()
                return True
            else:
                time.sleep(1)

        print()
        raise RuntimeError(
            "ERROR: Tasks did not reach 'Completed' state within "
            "timeout period of " + str(timeout)
        )

    def print_task_output(self, encoding=None):
        """Prints the stdout.txt file for each task in the job.

        :param batch_client: The batch client to use.
        :type batch_client: `batchserviceclient.BatchServiceClient`
        :param str job_id: The id of the job with task output files to print.
        """

        print("Printing task output...")

        tasks = self.client.task.list(self._JOB_ID)

        for task in tasks:

            node_id = self.client.task.get(self._JOB_ID, task.id).node_info.node_id
            print("Task: {}".format(task.id))
            print("Node: {}".format(node_id))

            stream = self.client.file.get_from_task(
                self._JOB_ID, task.id, self._STANDARD_OUT_FILE_NAME
            )

            file_text = self._read_stream_as_string(stream, encoding)
            print("Standard output:")
            print(file_text)

    def _read_stream_as_string(self, stream, encoding):
        """Read stream as string

        :param stream: input stream generator
        :param str encoding: The encoding of the file. The default is utf-8.
        :return: The file content.
        :rtype: str
        """
        output = io.BytesIO()
        try:
            for data in stream:
                output.write(data)
            if encoding is None:
                encoding = "utf-8"
            return output.getvalue().decode(encoding)
        finally:
            output.close()
        raise RuntimeError("could not write data to stream or decode bytes")

    def delete_blob_container(self, container_name):
        """ Clean up Azure Storage Container 
        :param: container_name (str): Container name with data for batch processing
        """
        try:
            self.blob_client.delete_container(container_name)
        except IOError:
            raise IOError("Failed to delete storage container")

    def submit_job_and_add_task(self):
        """Submits a job to the Azure Batch service and adds a simple task.

        :param batch_client: The batch client to use.
        :type batch_client: `batchserviceclient.BatchServiceClient`
        :param str job_id: The id of the job to create.
        """
        raise NotImplementedError("Yet to be implemented.")

    def execute_sample(self, global_config, sample_config):
        """Executes the sample with the specified configurations.

        :param global_config: The global configuration to use.
        :type global_config: `configparser.ConfigParser`
        :param sample_config: The sample specific configuration to use.
        :type sample_config: `configparser.ConfigParser`
        """
        raise NotImplementedError("Yet to be implemented.")

    def on_success(self):
        """
        Override for doing custom completion handling for a larger class of tasks
        This method gets called when :py:meth:`run` completes without raising any exceptions.
        The returned value is json encoded and sent to the scheduler as the `expl` argument.
        Default behavior is to send an None value"""
        raise NotImplementedError("Yet to be implemented.")

    def on_failure(self, exception):
        """
        Override for custom error handling.
        This method gets called if an exception is raised in :py:meth:`run`.
        The returned value of this method is json encoded and sent to the scheduler
        as the `expl` argument. Its string representation will be used as the
        body of the error email sent out if any.
        Default behavior is to return a string representation of the stack trace.
        """
        raise NotImplementedError("Yet to be implemented.")

    def get_tasks_status(self):
        """Get the status of all the tasks under one Job"""
        raise NotImplementedError("Yet to be implemented.")

    def print_batch_exception(self, batch_exception):
        """
        Prints the contents of the specified Batch exception.
        :param batch_exception:
        """
        print("-------------------------------------------")
        print("Exception encountered:")
        if (
            batch_exception.error
            and batch_exception.error.message
            and batch_exception.error.message.value
        ):
            print(batch_exception.error.message.value)
            if batch_exception.error.values:
                print()
                for mesg in batch_exception.error.values:
                    print("{}:\t{}".format(mesg.key, mesg.value))
        print("-------------------------------------------")


class AzureBatchTask(luigi.Task):
    """
    Base class for an Azure Batch job

    Azure Batch requires you to register "job definitions", which are JSON
    descriptions for how to issue the ``docker run`` command. This Luigi Task
    requires the azure batch account and storage account created before using this. 
    These are passed in as Luigi parameters

    :param job_definition (str): name of pre-registered jobDefinition
    :param job_name: name of specific job, for tracking in the queue and logs.
    :param 


    :param batch_account_name (str): name of pre-created azure batch account
    :param batch_account_key (str): master key for azure batch account
    :param batch_account_url (str): batch account url
    :param storage_account_name (str): name of pre-created storage account
    :param storage_account_key (str): storage account key
    :param input_path (str): path to data input on storage account
    :param output_path (str): path to result output
    :param pool_id (str): pool id for batch job
    :param job_id (str): job id for the batch job
    :param pool_node_count (str): number of nodes to create for the batch job; default is 2
    :param pool_vm_size (str): size of vm to use for the batch job

    """

    batch_account_name = luigi.Parameter()
    batch_account_key = luigi.Parameter()
    batch_account_url = luigi.Parameter()
    storage_account_name = luigi.Parameter()
    storage_account_key = luigi.Parameter()
    input_path = luigi.Parameter(default=" ")
    command = luigi.Parameter(default="echo Hello World")
    output_path = luigi.Parameter(default=" ")
    pool_id = luigi.Parameter(default=TASK_POOL_ID)
    job_id = luigi.Parameter(default=TASK_JOB_ID)
    pool_node_count = luigi.IntParameter(default=TASK_POOL_NODE_COUNT)
    pool_vm_size = luigi.Parameter(default=TASK_POOL_VM_SIZE)
    kwargs = luigi.DictParameter(default={})
    container_name = "luigitargetdata"

    def run(self):
        bc = AzureBatchClient(
            self.batch_account_name,
            self.batch_account_key,
            self.batch_account_url,
            self.storage_account_name,
            self.storage_account_key,
            self.output_path,
            self.pool_id,
            self.job_id,
            self.pool_node_count,
            self.pool_vm_size,
            # self.kwargs,
        )

        bc.blob_client.create_container(self.container_name, fail_on_exist=False)

        if os.path.exists(self.input_path):
            input_files = [
                bc.upload_file_to_container(
                    self.container_name, self.input_path + file_path
                )
                for file_path in os.listdir(self.input_path)
            ]
        else:
            input_files = []

        try:
            bc.create_pool()
            bc.create_job()
            bc.add_tasks(input_files, self.command)
            bc.wait_for_tasks_to_complete(datetime.timedelta(minutes=TASK_TIME_OUT))
            bc.print_task_output()
        except batchmodels.BatchErrorException as err:
            bc.print_batch_exception(err)
        finally:
            bc.delete_blob_container(self.container_name)
            bc.client.job.delete(self.job_id)
            bc.client.pool.delete(self.pool_id)
