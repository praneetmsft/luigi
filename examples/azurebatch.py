# -*- coding: utf-8 -*-
#
# Copyright 2015-2015 Spotify AB
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

import os
import luigi
from luigi import build
from luigi.contrib.azurebatch import AzureBatchTask


class AzureBatchHelloWorld(AzureBatchTask):
    """
    Prerequisites: 
    - Create an Azure Batch Service Account and Azure Storage Account
    - Provide the secrets/credentials for Storage Account and Azure Batch Service Account in a .env file 
    - Python Packages to install:
        - azure batch package: ``pip install azure-batch>=6.0.0``
        - azure blob storage: ``pip install azure-storage-blob>=1.3.1``
  
    This is the pass through class, showing how you can inherit the new AzureBatchTask.
    You need to override methods if you need a different functionality
    This task runs a :py:class:`luigi.contrib.azurebatch.AzureBatchTask` task.
    
    Provide the path of the root dir where the data and python scripts are kep locally, this task
    will upload the root dir to azure blob and download it on each compute node from blob to execute
    it on each node in parallel.

    Output results are stored on Azure storage account
    """

    batch_account_name = luigi.Parameter(os.getenv("BATCH_ACCOUNT_NAME"))
    batch_account_key = luigi.Parameter(os.getenv("BATCH_ACCOUNT_KEY"))
    batch_account_url = luigi.Parameter(os.getenv("BATCH_ACCOUNT_URL"))
    storage_account_name = luigi.Parameter(os.getenv("STORAGE_ACCOUNT_NAME"))
    storage_account_key = luigi.Parameter(os.getenv("STORAGE_ACCOUNT_KEY"))
    command = luigi.Parameter("echo Hello World")


if __name__ == "__main__":

    build([AzureBatchHelloWorld()], local_scheduler=True)
