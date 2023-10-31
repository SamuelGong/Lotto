<p align="center">
    <img src="asset/lotto.png" height=400>
</p>

<p align="center">
    <a href="https://github.com/SamuelGong/Lotto-doc"><img src="https://img.shields.io/badge/-github-teal?logo=github" alt="github"></a>
    <a href="https://github.com/SamuelGong/Lotto-doc/blob/main/LICENSE"><img src="https://img.shields.io/github/license/SamuelGong/Lotto-doc?color=yellow" alt="License"></a>
    <img src="https://badges.toozhao.com/badges/01HE2YKN7CE242JHSFMTQ6JXE1/green.svg" />
</p>

<h1 align="center">Lotto: Secure Participant Selection against Adversarial Servers in Federated Learning</h1>

*Posted by Zhifeng Jiang, HKUST on Tuesday, October 31, 2023*

> This technical blog serves as a concise introduction to one of our recent research projects. Please note that the detailed manuscript and code for this project are currently confidential, as they are subject to Nokia Bell Labs' intelligent property protection policy.

## Project

Edge devices, such as smartphones and laptops, are becoming increasingly
powerful and can now collaboratively build high-quality machine learning
(ML) models using device-collected data.
To  protect data privacy, large companies like Google and Apple have
adopted [federated learning (FL)](http://proceedings.mlr.press/v54/mcmahan17a/mcmahan17a.pdf) for
tasks in computer vision (CV) and natural language processing (NLP) across
client devices.
In the [standard workflow](https://proceedings.mlsys.org/paper_files/paper/2019/file/7b770da633baf74895be22a8807f1a8f-Paper.pdf)
of FL, a server dynamically samples a small subset of clients from a large
population in each training round. The sampled clients, aka **participants**,
compute individual local updates using their own data and
upload only the updates to the server for global aggregation, without
revealing the training data.

## Support
If you need any further inquiry, feel free to submit a Github issue, or contact Zhifeng Jiang via zjiangaj@cse.ust.hk.
