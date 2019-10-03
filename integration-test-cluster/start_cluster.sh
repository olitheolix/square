#!/bin/bash

set -e

# ------------------------------------------------------------------------------
#                            Bootstrap Kind Cluster
# ------------------------------------------------------------------------------
# Start the KIND container.
docker run -dit --privileged -p 8443:8443 -p 10080:10080 bsycorp/kind:latest-1.13

# Wait until the KIND cluster is operational.
printf "\n\n### Booting KIND Cluster: "
set +e
while true; do
    printf "."
    sleep 1

    # KIND provides an HTTP endpoint to ascertain whether or not it is ready yet.
    curl http://localhost:10080/kubernetes-ready > /dev/null 2>/dev/null
    if [ $? == 0 ]; then break; fi
done
set -e
printf "done\n"

# ------------------------------------------------------------------------------
#                              Download Kubeconfig
# ------------------------------------------------------------------------------
# Download the Kubeconfig file into the /tmp folder. Once again, KIND provides
# an HTTP endpoint to get it.
KUBECONFIG=/tmp/kubeconfig-kind.yaml
printf "### Downloading Kubeconfig to $KUBECONFIG: "
curl http://localhost:10080/config >$KUBECONFIG 2>/dev/null
printf "done\n"


# ------------------------------------------------------------------------------
#                          Deploy The Demo Resources
# ------------------------------------------------------------------------------
printf "### Deploy demo resources:\n"
set +e

# Apply the resource manifests until KIND accepts it. This may take a few
# iterations because KIND, despite claiming it was ready earlier, often does not
# accept the manifests straight away.
while true; do
    # Deploy the manifests.
    kubectl --kubeconfig $KUBECONFIG --context kind apply -f test-resources.yaml

    # Exit this loop if deployment succeeded. If not, then KIND is not yet ready
    # and we will try again shortly.
    if [ $? == 0 ]; then break; fi
    printf "\n   # Retry deployment...\n"
    sleep 1
done
set -e
printf "done\n"


printf "\n\n### KIND cluster now fully deployed (KUBECONF=$KUBECONFIG)\n"
