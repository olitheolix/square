# Use Square As A Library
The Square CLI is only a thin wrapper around the Square library. The
examples here show how to integrate Square into your own Python projects.

* [basic_workflow.py](basic_workflow.py) implements the get/plan/apply workflow.
* [custom_callbacks.py](custom_callbacks.py) BYO code to strip manifests and pre-process them for patches.
* [multi_cluster.py](multi_cluster.py) manage multiple clusters concurrently.

## Setup Test Cluster
By default, the examples use the integration test cluster:

    $ cd integration-test-cluster; ./start_cluster.sh
