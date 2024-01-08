<p align="center">
    <img src="asset/lotto.png" height=400>
</p>

<p align="center">
    <a href="https://arxiv.org/abs/xxxx.xxxxx"><img src="https://img.shields.io/badge/arxiv-xxxx.xxxxx-silver" alt="Paper"></a>
    <a href="https://github.com/SamuelGong/Lotto"><img src="https://img.shields.io/badge/-github-teal?logo=github" alt="github"></a>
    <a href="https://github.com/SamuelGong/Lotto/blob/main/LICENSE"><img src="https://img.shields.io/github/license/SamuelGong/Lotto?color=yellow" alt="License"></a>
    <img src="https://badges.toozhao.com/badges/01HD0PSS2V0P5C237GBFEG9ZVX/blue.svg" />
</p>

<h1 align="center">Lotto: Secure Participant Selection against Adversarial Servers in Federated Learning (arXiv 2023)</h1>

This repository contains the evaluation artifacts of our [preprint](#) paper titled 
"Lotto: Secure Participant Selection against Adversarial Servers in Federated Learning".

[Zhifeng Jiang](http://home.cse.ust.hk/~zjiangaj/), [Peng Ye](pyeac@cse.ust.hk), [Shiqi He](https://tctower.github.io/), 
[Wei Wang](https://home.cse.ust.hk/~weiwa/), [Ruichuan Chen](https://www.ruichuan.org/), [Bo Li](https://www.cse.ust.hk/~bli)

**Keywords**: Federated Learning, Client Selection, Secure Aggregation, Distributed Differential Privacy

<details> <summary><b>Abstract (Tab here to expand)</b></summary>

In federated Learning (FL), common privacy-preserving technologies, such as
secure aggregation and distributed differential privacy, rely on the
critical assumption of *an honest majority* among participants to withstand
various attacks. In practice, however, servers are not always trusted,
and an adversarial server can strategically select compromised clients to
create a dishonest majority, thereby undermining the system's security guarantees. 
In this paper, we present Lotto, an FL system that addresses this
fundamental, yet underexplored issue by providing secure participant
selection against an adversarial server. Lotto supports two
selection algorithms: *random* and *informed*. To ensure random
selection without a trusted server, Lotto enables each client to
autonomously determine their participation using *verifiable randomness*.
For informed selection, which is more vulnerable
to manipulation, Lotto approximates the algorithm by employing random
selection within a *refined client pool*. Our theoretical analysis
shows that Lotto effectively restricts the number of server-selected 
compromised clients, thus ensuring an honest majority among
participants. Large-scale experiments further reveal that Lotto
achieves time-to-accuracy performance comparable to that of insecure
selection methods, indicating a low computational overhead for secure selection. 

</details>

## Table of Contents
1. [Overview](#1-overview)
2. [Prerequisites](#2-prerequisites)
   * Necessary dependencies installation before anything begins.
3. [Simulation](#3-simulation)  
   * Learn how to run experiment in simulation mode.  
4. [Cluster Deployment](#4-cluster-deployment)
   * Learn how to run experiments in a distributed manner.
6. [Repo Structure](#5-repo-structure)
   * What are contained in the project root folder.
7. [Support](#6-support)
8. [License](#7-license)
9. [Citation](#8-citation)

## 1. Overview

The system supports two modes of operation:

1. **Simulation**:
This mode allows you to run experiments on a local machine or GPU server.
It is primarily used for validating functionality, privacy, or utility.
2. **Cluster Deployment**:
This mode enables you to run experiments on an AWS EC2 cluster 
funded by your own account. Alternatively, you can also run experiments on 
an existing cluster of Ubuntu nodes (currently undocumented). 
Cluster Deployment Mode is typically used for evaluating runtime performance.

## 2. Prerequisites

To work with the project, you need to have a Python 3 Anaconda environment set up 
in your host machine (Ubuntu system assumed) with specific dependencies installed. 
To simplify the setup process, we provide a shortcut:

```bash
# assumes you are working from the project folder
cd exploration/dev
bash standalone_install.sh
conda activate lotto
```

**Note**

1. Most the dependencies will be installed in a newly created environment called 
**lotto**, minimizing interference with your original system setup.
2. However, please note that the `redis-server` application needs to be installed 
at the **system** level with **sudo** previlige, as mentioned in the Line 49-52 of 
the `standalone_install.sh` script. If you do not have sudo privileges, you can 
follow the instructions provided [here](https://techmonger.github.io/40/redis-without-root/)
to install Redis without root access. In that case, you should comment out these 
lines before executing the command `bash standalone_install.sh`.

## 3. Simulation

### 3.1 Preparing Working Directory

Start by choosing a name for the working directory. 
For example, let's use `ae-simulator` in the following instructions.

```bash
# assumes you are working from the project folder
cd exploration
cp -r simulation_folder_template ae-simulator
cd exploration/ae-simulator
```

### 3.2 Run Experiments

To run an experiment with a specific configuration file in the background,
follow these steps:

```bash
bash simulator_run.sh start_a_task [target folder]/[target configuration file]
```

The primarily logged information will be output to the following file:

```
[target folder]/[timestamp]/lotto-coordinator/log.txt
```

**Note**

1. When you execute the above command, the command line will prompt you with `[timestamp]`, 
which represents the relevant timestamp and output folder.
2. You can use the simulator_run.sh script for task-related control.
You don't need to remember the commands because the prompt will inform you
whenever you start a task. Here are a few examples:
    ```bash
    # To kill a task halfway
    bash simulator_run.sh kill_a_task [target folder]/[timestamp]
    # To analyze the output and generate insightful figures/tables
    bash simulator_run.sh analyze_a_task [target folder]/[timestamp]
    ```
   
### 3.3 Batch Tasks to Run

The simulator also supports batching tasks to run. You can specify the tasks 
to run in the background by writing them in the `batch_plan.txt` file, 
as shown below:

```
[target folder]/[target configuration file]
[target folder]/[target configuration file]
[target folder]/[target configuration file]
```

To sequentially run the tasks in a batch, execute the following command:

```
bash batch_run.sh batch_plan.txt
```

The execution log will be available at `batch_log.txt`.

**Note**

1. To stop the batching logic halfway and prevent it from issuing any new tasks,
you can use the command `kill -9 [pid]`. The `[pid]` value can be found at the 
beginning of the file `batch_log.txt`.
2. If you want to stop a currently running task halfway, you can kill it using the
command `bash simulator_run.sh kill_a_task [...]`, as explained in the previous
subsection. The information needed to kill the job will also be available in the log.

## 4. Cluster Deployment

You can initiate the cluster deployment process either from your local host
machine (ensuring a stable network connection) or from a dedicated remote node
specifically designed for **coordination** purposes (we thus call it the coordinator node).
It is important to note that the remote node does not necessarily need to be a
powerful machine.

### 4.1 Install and Configure AWS CLI

Before proceeding, please ensure that you have an **AWS account**.
Additionally, on the coordinator node, it is essential to have the latest version of
**aws-cli** installed and properly configured with the necessary credentials.
This configuration will allow us to conveniently manage all the nodes in the cluster
remotely using command-line tools.

**Reference**

1. [Install AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html).
    * Example command for installing into Linux x86 (64-bit):
    ```bash
    # You can work from any directory, e.g., at your home directory
    curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
    sudo apt install unzip
    unzip awscliv2.zip
    sudo ./aws/install
    ```
2. [Configure AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-quickstart.html).
    * Example command for configuring one's AWS CLI:
    ```bash
    # You can work from any directory, e.g., at your home directory
    aws configure
    ```
    You will be prompted to enter your AWS Access Key ID, AWS Secret Access Key,
Default region name, and Default output format. Provide the required information as prompted.

### 4.2 Configure and Allocate a Cluster

To begin, let's choose a name for the working directory. For example, we can use `ae-cluster` 
as the name throughout this section (please note that this name should not be confused with
the above-mentioned simulator folder).

```bash
# assumes you are working from the project folder
cd exploration
cp -r cluster_folder_template ae-cluster
cd ae-cluster

# make some modifications that suit your need/budget
# 1. EC2-related
# relevant key:
#   BlockDeviceMappings/Ebs/VolumeSize: how large is the storage of each node
#   KeyName: the path to the key file (relative to ~/.ssh/) you plan to use to 
#            log into each node of the cluster from your coordinator node
vim ../dev/ec2_node_template.yml
# 2. cluster-related
# relevant key:
#     type and region for the server each client, and
#     count and bandwidth for each client
#     (the provided images are specifically for Ubuntu 18.04)
vim ec2_cluster_config.yml
# 3. minor
# relevant value: LOCAL_PRIVATE_KEY
#     set the value the same as KeyName mentioned above
vim manage_cluster.sh
```

Then one can launch a cluster from scratch:

```bash
bash manage_cluster.sh launch
```

**Note**

1. The `manage_cluster.sh` script provides a range of cluster-related controls for
your convenience. Here are some examples of how to use it:

    ```bash
    # start an already launch cluster
    bash manage_cluster.sh start
    # stop a running cluster
    bash manage_cluster.sh stop
    # restart a running cluster
    bash manage_cluster.sh reboot
    # terminate an already launch cluster
    bash manage_cluster.sh terminate
    # show the public IP addresses for each node
    bash manage_cluster.sh show
    # scale up/down an existing cluster 
    # (with the ec2_cluster_config.yml modified correspondingly)
    bash manage_cluster.sh scale
    # configure the bandwidth for each client node
    # (with the ec2_cluster_config.yml modified correspondingly)
    bash manage_cluster.sh limit_bandwidth
    # reset the bandwidth for each node
    bash manage_cluster.sh free_bandwidth
    ```

### 4.3 Setting Up the Cluster

Execute the following commands:

```bash
bash setup.sh install
bash setup.sh deploy_cluster
```

**Note**
1. Additionally, the `setup.sh` script offers a wide range of app-related controls.
Here are a couple of examples:
    ```bash
    # update all running nodes' Github repo
    bash setup.sh update
    # add pip package to the used conda environment for all running nodes
    bash setup.sh add_pip_dependency [package name (w/ or w/o =version)]
    # add apt package for all running nodes (recall that we are using Ubuntu)
    bash setup.sh add_apt_dependency [package name (w/ or w/o =version)]
    ```

### 4.4 Run Experiments

Like what in simulation, once you have a running cluster, you can start a task
with distributed deployment by running commands like:

```bash
bash cluster_run.sh start_a_task [target folder]/[target configuration file]
```

**Remark**
1. After you execute the above command, the command line will prompt you with `[timestamp]`, 
which represents the relevant timestamp and output folder.
2. To control the task, you can use the following commands with `cluster_run.sh`:
    ```bash
    # for killing the task halfway
    bash cluster_run.sh kill_a_task [target folder]/[timestamp]
    # for fetching logs from all running nodes to the coordinator
    # (i.e., the machine where you type this command)
    bash cluster_run.sh conclude_a_task [target folder]/[timestamp]
    # for analyzing the collected log to generate some insightful figures/tables
    # Do it only after the command ... conclude_a_task ... has been executed successfully
    bash cluster_run.sh analyze_a_task [target folder]/[timestamp]
    ```

### 4.5 Batch Tasks to Run

In addition to running tasks individually, the cluster mode also supports batch 
execution of tasks. To run a batch of tasks in the background, you need to specify
the tasks to be executed in the `batch_plan.txt` file using the following format:

```
[target folder]/[target configuration file]
[target folder]/[target configuration file]
[target folder]/[target configuration file]
```

Then you can sequentially execute them as a batch by running the following command:

```
bash batch_run.sh batch_plan.txt
```

The execution log will be generated and can be found at `batch_log.txt`.

**Note**

1. If you need to terminate the batch execution before completion, you can use the command
`kill -9 [pid]` to stop the batching logic. The process ID `pid` can be found at the
beginning of the log file.
2. If you want to stop a specific task that is currently running, you can use the command
`bash simulator_run.sh kill_a_task [...]` as explained in the subsection above. The
necessary information for killing a job, denoted by `[...]`, can also be found in the log file.

## 5. Repo Structure

```
Repo Root
|---- lotto                             # Core implementation
|---- infra                             # Federated learning infrastructure
|---- exploration                       # Evaluation
    |---- cluster_folder_template       # Necessities for cluster deployment
    |---- simulation_folder_template    # Necessities for single-node simulation
    |---- dev                           # Backend for experiment manager
    |---- analysis                      # Backend for resulting data processing
```

## 6. Support
If you need any help, please submit a Github issue, or contact Zhifeng Jiang via zjiangaj@cse.ust.hk.

## 7. License

The code included in this project is licensed under the [Apache 2.0 license](LICENSE).
If you wish to use the codes and models included in this project for commercial purposes, please sign this 
[document](https://docs.google.com/forms/d/e/1FAIpQLSflW4i3BhA24AzsqAcvjuutHtpbKftLjNLiZbbgGSVdxMSFIQ/viewform?usp=sf_link)
to obtain authorization.

## 8. Citation

If you find this repository useful, please consider giving ⭐ and citing our paper (preprint available [here](https://arxiv.org/pdf/xxxx.xxxxx.pdf)):

```bibtex
@inproceedings{jiang2023secure,
  author={Jiang, Zhifeng and Ye, Peng and He, Shiqi and Wang, Wei and Ruichuan, Chen and Li, Bo},
  title={Lotto: Secure Participant Selection against Adversarial Servers in Federated Learning},
  year={2023},
  booktitle={arXiv:xxxx.xxxxx},
}
```
