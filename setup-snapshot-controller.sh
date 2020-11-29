#!/bin/bash

print_usage() {
    printf "Usage: %s [-c|-d]\n" $0
    printf "  -c\tCreate manifests\n"
    printf "  -d\tDelete manifests\n"
}

declare -a manifests=(
    # Snapshot CRDs
    https://raw.githubusercontent.com/kubernetes-csi/external-snapshotter/release-2.1/config/crd/snapshot.storage.k8s.io_volumesnapshotclasses.yaml
    https://raw.githubusercontent.com/kubernetes-csi/external-snapshotter/release-2.1/config/crd/snapshot.storage.k8s.io_volumesnapshotcontents.yaml
    https://raw.githubusercontent.com/kubernetes-csi/external-snapshotter/release-2.1/config/crd/snapshot.storage.k8s.io_volumesnapshots.yaml

    # Snapshot controller RBACs and the controller itself
    https://raw.githubusercontent.com/kubernetes-csi/external-snapshotter/release-2.1/deploy/kubernetes/snapshot-controller/rbac-snapshot-controller.yaml
    https://raw.githubusercontent.com/kubernetes-csi/external-snapshotter/release-2.1/deploy/kubernetes/snapshot-controller/setup-snapshot-controller.yaml
)

create_manifests() {
    for (( i = 0; i < ${#manifests[@]}; i++ ));
    do
        kubectl create -f "${manifests[$i]}"
    done
}

delete_manifests() {
    for (( i = ${#manifests[@]}-1; i >= 0; i-- ));
    do
        kubectl delete -f "${manifests[$i]}"
    done
}

if [ "$#" -gt 0 ]; then
    while getopts 'cdh' flag; do
        case "${flag}" in
            c) create_manifests ;;
            d) delete_manifests ;;
            h|*) print_usage
                 exit 1 ;;
        esac
    done
else
    create_manifests
fi
