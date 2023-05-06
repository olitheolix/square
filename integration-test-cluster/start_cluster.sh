#!/bin/bash

set -e

KUBECONFIG=/tmp/kubeconfig-kind.yaml

# ------------------------------------------------------------------------------
#                            Bootstrap Kind Cluster
# ------------------------------------------------------------------------------
KINDCONFIG=/tmp/kind-config.yaml

# Create a KinD configuration file.
cat << EOF > $KINDCONFIG
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
nodes:
- role: control-plane
  image: kindest/node:v1.25.8@sha256:00d3f5314cc35327706776e95b2f8e504198ce59ac545d0200a89e69fce10b7f
EOF

# Create cluster, then delete its config file.
kind create cluster --config $KINDCONFIG --kubeconfig $KUBECONFIG
rm $KINDCONFIG

# ------------------------------------------------------------------------------
#                          Deploy The Demo Resources
# ------------------------------------------------------------------------------
printf "### Deploy test resources into the cluster:\n"
set +e

# Apply the resource manifests until KinD accepts all of them. This may take a
# few iterations because some resources, in particular CRDs, may require some
# time to boot before they expose their new API endpoints.
while true; do
    # Deploy the manifests.
    kubectl --kubeconfig $KUBECONFIG apply -f ./

    # Exit this loop if deployment succeeded. If not, then KIND is not yet ready
    # and we will try again shortly.
    if [ $? == 0 ]; then break; fi
    printf "\n   # Retry deployment...\n"
    sleep 1
done
set -e
printf "done\n"

printf "\n\n### KIND cluster now fully deployed (KUBECONF=$KUBECONFIG)\n"
