#!/usr/bin/env bash
# Renders the Kubernetes manifests with kustomize and validates them against the Kubernetes JSON schemas (no cluster
# needed). Requires kubectl (for "kubectl kustomize") and kubeconform (https://github.com/yannh/kubeconform).
# Usage: scripts/validate-k8s.sh            KUBECONFORM=/path/to/kubeconform scripts/validate-k8s.sh
set -euo pipefail
cd "$(dirname "$0")/.."
KUBECONFORM="${KUBECONFORM:-kubeconform}"
K8S_VERSION="${K8S_VERSION:-1.32.0}"
mkdir -p target/k8s
for dir in deploy/k8s deploy/k8s-overlays/rabbitmq-redis; do
  [ -d "$dir" ] || continue
  out="target/k8s/$(echo "$dir" | tr '/' '-').yaml"
  kubectl kustomize "$dir" > "$out"
  echo "rendered $dir -> $out ($(grep -c '^kind:' "$out") objects)"
  "$KUBECONFORM" -strict -summary -kubernetes-version "$K8S_VERSION" "$out"
done
