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
    This is the pass through class, showing how you can inherit the new AzureBatchTask.
    You need to override methods if you need a different functionality
    This task runs a :py:class:`luigi.contrib.azurebatch.AzureBatchTask` task.
    
    Provide the path of the root dir where the data and python scripts are kep locally, this task
    will upload the root dir to azure blob and download it on each compute node from blob to execute
    it on each node in parallel.

    Output results are stored on Azure storage account
    """

    pass


if __name__ == "__main__":

    """
    Prerequisites: 
    - Create an Azure Batch Service Account and Azure Storage Account
    - Provide the secrets/credentials for Storage Account and Azure Batch Service Account in a .env file 
    - Python Packages to install:
        - azure batch package: ``pip install azure-batch>=6.0.0``
        - azure blob storage: ``pip install azure-storage-blob>=1.3.1``
    """

    batch_account_key = os.getenv("BATCH_ACCOUNT_KEY")
    batch_account_name = os.getenv("BATCH_ACCOUNT_NAME")
    batch_service_url = os.getenv("BATCH_ACCOUNT_URL")
    storage_account_key = os.getenv("STORAGE_ACCOUNT_KEY")
    storage_account_name = os.getenv("STORAGE_ACCOUNT_NAME")
    input_path = " "  # Path to the root dir where data & scripts are kept
    script_name = " "  # Name of the main script which will run on Batch, e.g main.py
    command = "echo Hello World"  # To run python script:  "python " + script_name

    print("batch account name", batch_account_name)
    build(
        [
            AzureBatchHelloWorld(
                batch_account_name,
                batch_account_key,
                batch_service_url,
                storage_account_name,
                storage_account_key,
                command,
            )
        ],
        local_scheduler=True,
    )
