# Databricks CI/CD 
This is a tool for building CI/CD pipelines for Databricks. It is a python package that
works in conjunction with a custom GIT repository (or a simple file structure) to validate 
and deploy content to databricks. Currently, it can handle the following content:
* **Workspace** - a collection of notebooks written in Scala, Python, R or SQL
* **Jobs** - list of Databricks jobs
* **Clusters**
* **Instance Pools**
* **DBFS** - an arbitrary collection of files that may be deployed on a Databricks workspace

# Installation
`pip install databricks-cicd`

# Requirements
To use this tool, you need a source directory structure (preferably as a private GIT repository) 
that has the following structure:
```
any_local_folder_or_git_repo/
├── workspace/
│   ├── some_notebooks_subdir
│   │   └── Notebook 1.py
│   ├── Notebook 2.sql
│   ├── Notebook 3.r
│   └── Notebook 4.scala
├── jobs/
│   ├── My first job.json
│   └── Side gig.json
├── clusters/
│   ├── orion.json
│   └── Another cluster.json
├── instance_pools/
│   ├── Pool 1.json
│   └── Pool 2.json
└── dbfs/
    ├── strawbery_jam.jar
    ├── subdir
    │   └── some_other.jar
    ├── some_python.egg
    └── Ice cream.jpeg
```

**_Note:_** All folder names represent the default and can be configured. This is just a sample.

# Usage
For the latest options and commands run:
```
cicd -h
```
A sample command could be:
```shell
cicd deploy \
   -w sample_12432.7.azuredatabricks.net \
   -u john.smith@domain.com \
   -t dapi_sample_token_0d5-2 \
   -lp '~/git/my-private-repo' \
   -tp /blabla \
   -c DEV.ini \
   --verbose
```
**_Note:_** Paths for windows need to be in double quotes

# Create content

#### Notebooks:
1. Add a notebook to source
   1. On the databricks UI go to your notebook. 
   1. Click on `File -> Export -> Source file`. 
   1. Add that file to the `workspace` folder of this repo **without changing the file name**.

#### Jobs:
1. Add a job to source
   1. Get the source of the job and write it to a file. You need to have the
      [Databricks CLI](https://docs.databricks.com/user-guide/dev-tools/databricks-cli.html#install-the-cli) 
      and [JQ](https://stedolan.github.io/jq/download/) installed. 
      For Windows, it is easier to rename the `jq-win64.exe` to `jq.exe` and place it 
      in `c:\Windows\System32` folder. Then on Windows/Linux/MAC: 
      ```
      databricks jobs get --job-id 74 | jq .settings > Job_Name.json
      ```
      This downloads the source JSON of the job from the databricks server and pulls only the settings from it, 
      then writes it in to a file.
      
      **_Note:_** The file name should be the same as the job name within the json file. Please, avoid spaces 
      in names.
   1. Add that file to the `jobs` folder
   
#### Clusters:
1. Add a cluster to source
   1. Get the source of the cluster and write it to a file. 
      ```
      databricks clusters get --cluster-name orion > orion.json
      ```
      **_Note:_** The file name should be the same as the cluster name within the json file. Please, avoid spaces 
      in names.
   1. Add that file to the `clusters` folder
   
#### Instance pools:
1. Add an instance pool to source
   1. Similar to clusters, just use `instance-pools` instead of `clusters`
   
#### DBFS:
1. Add a file to dbfs
   1. Just add a file to the the `dbfs` folder.
   
# TODO
* Improve validation. It is still a baby.
