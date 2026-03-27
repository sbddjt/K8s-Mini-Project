#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
NAMESPACE=${1:-monitoring}
DASHBOARD_DIR="${SCRIPT_DIR}/grafana-dashboards"

apply_dashboard() {
  local configmap_name="$1"
  local source_file="$2"
  local target_name="$3"

  kubectl -n "${NAMESPACE}" create configmap "${configmap_name}" \
    --from-file="${target_name}=${source_file}" \
    --dry-run=client \
    -o yaml \
  | kubectl label --local -f - grafana_dashboard=1 -o yaml \
  | kubectl apply -f -
}

apply_dashboard \
  "mobility-ops-dashboard" \
  "${DASHBOARD_DIR}/mobility-ops-dashboard.json" \
  "mobility-ops-dashboard.json"

apply_dashboard \
  "mobility-service-deepdive-dashboard" \
  "${DASHBOARD_DIR}/mobility-service-deepdive.json" \
  "mobility-service-deepdive.json"

apply_dashboard \
  "alertmanager-overview-dashboard" \
  "${DASHBOARD_DIR}/alertmanager-overview.json" \
  "alertmanager-overview.json"

apply_dashboard \
  "coredns-dashboard" \
  "${DASHBOARD_DIR}/coredns.json" \
  "coredns.json"

echo "Grafana dashboards applied to namespace: ${NAMESPACE}"
