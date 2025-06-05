#!/usr/bin/env bash

set -euo pipefail
set -x

# Process UID
PROCESS_UID=0511bbec-3861-4a67-9a5e-a872ae161859
# Backup target FinBackup UID
BACKUP_TARGET_FINBACKUP_UID=0511bbec-3861-4a67-9a5e-a872ae161859
# Backup target pool
BACKUP_TARGET_POOL=replicapool
# Backup target image name
BACKUP_TARGET_IMAGE_NAME=csi-vol-d4527a69-ccd6-42ba-94a3-73ea85c2ad4c
if [ "$1" = "full" ] ; then
    # Full backup
    # Backup target snapshot id
    BACKUP_TARGET_SNAPSHOT_ID=3
    # Backup source snapshot id
    BACKUP_SOURCE_SNAPSHOT_ID=""
else
    # Incremental backup
    # Backup target snapshot id
    BACKUP_TARGET_SNAPSHOT_ID=4
    # Backup source snapshot id
    BACKUP_SOURCE_SNAPSHOT_ID=3
fi
# Backup target PVC name
BACKUP_TARGET_PVC_NAME=test-pvc
# Backup target PVC namespace
BACKUP_TARGET_PVC_NAMESPACE=default
# Backup target PVC UID
BACKUP_TARGET_PVC_UID=40dceae2-1592-4a09-bcc8-7ce8947877f0
# Maximum part size
MAX_PART_SIZE=104857600 # 100 MiB

# Test flag. Stop during saving differential data for full backup.
TEST_CANCEL_FULL_BACKUP_EXPORT_DIFF=0
# Test flag. Stop during applying differential data for full backup.
TEST_CANCEL_FULL_BACKUP_PATCH=0
# Test flag. Stop during saving differential data for incremental backup.
TEST_CANCEL_INCR_BACKUP_EXPORT_DIFF=0

TILDE=/tmp/testfin
FIN_PATCH=~/fin/main

echo "Check the action_status table in fin.sqlite3 and confirm if the process can proceed. Use the backup target FinBackup UID as the uid. If good, proceed to the next step. If not, wait for some time and retry from this step."
sqlite3 fin.sqlite3 <<EOS
CREATE TABLE IF NOT EXISTS action_status (
    uid TEXT NOT NULL PRIMARY KEY, -- (immutable) uid that uniquely identifies the process.
    action TEXT NOT NULL, -- (immutable) represents the type of process such as backup creation, deletion, restore. When action is determined, the data format of the private column can be uniquely specified.
    private_data BLOB, -- (mutable) holds the state of each process. The data format is determined during detailed design of each process.
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP, -- (immutable) record creation time
    updated_at DATETIME -- (mutable) record update time
);
CREATE TABLE IF NOT EXISTS backup_metadata (
    data BLOB, -- (mutable) holds JSON explained below.
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP, -- (immutable) record creation time
    updated_at DATETIME -- (mutable) record update time
);
EOS
PROCESS_UID_IN_DB=$(cat <<EOS | sqlite3 fin.sqlite3
SELECT uid FROM action_status;
EOS
)
if [ "${PROCESS_UID_IN_DB}" != "${PROCESS_UID}" ]; then
cat <<EOS | sqlite3 fin.sqlite3
INSERT INTO action_status (uid, action, private_data, created_at, updated_at)
VALUES ('${PROCESS_UID}', '', '{}', datetime('now'), datetime('now'));
SELECT * FROM action_status;
EOS
fi

echo "Update the action_status table in fin.sqlite3:"
cat <<EOS | sqlite3 fin.sqlite3
SELECT * FROM action_status;
UPDATE action_status SET action = 'backup';
SELECT * FROM action_status;
EOS

echo "If private_data is not empty, read the following values and assign them to variables of the same name. If no value exists, the variable content is empty."
MODE=$(
cat <<EOS | sqlite3 fin.sqlite3 | jq -r '.mode // ""'
SELECT private_data FROM action_status;
EOS
)
BACKUP_PART=$(
cat <<EOS | sqlite3 fin.sqlite3 | jq -r '.backupPart // ""'
SELECT private_data FROM action_status;
EOS
)
PATCH_PART=$(
cat <<EOS | sqlite3 fin.sqlite3 | jq -r '.patchPart // ""'
SELECT private_data FROM action_status;
EOS
)

if [ "${MODE}" = "" ]; then
    echo "If mode is empty:"
    echo "Determine whether to perform full backup or incremental backup. Assign the result to a new variable tmpMode:"
    if [ "${BACKUP_SOURCE_SNAPSHOT_ID}" = "" ]; then
        echo "If the input backup source snapshot id is an empty string:"
        echo "Determine as full backup and assign \"full\" to tmpMode."
        TMP_MODE="full"
    else
        echo "Otherwise"
        echo "If a record exists in the backup_metadata table of fin.sqlite3, determine as incremental backup and assign \"incremental\" to tmpMode."
        BACKUP_METADATA_COUNT=$(cat <<EOS | sqlite3 fin.sqlite3
SELECT COUNT(*) FROM backup_metadata
EOS
)
        if [ "${BACKUP_METADATA_COUNT}" -gt 0 ]; then
            TMP_MODE="incremental"
        else
            echo "Otherwise, determine as full backup and assign \"full\" to tmpMode."
            TMP_MODE="full"
        fi
    fi

    if [ "${TMP_MODE}" = "full" ]; then
        echo "If tmpMode == \"full\""
        echo "Confirm that there are no records in the backup_metadata table of fin.sqlite3. If records exist, it's an error."
        BACKUP_METADATA_COUNT=$(cat <<EOS | sqlite3 fin.sqlite3
SELECT COUNT(*) FROM backup_metadata
EOS
)
        if [ ${BACKUP_METADATA_COUNT} -gt 0 ]; then
            exit 1
        fi

        echo "Get the backup target PVC manifest from backup target PVC namespace and backup target PVC name."
        BACKUP_TARGET_PVC_MANIFEST=$(kubectl get pvc ${BACKUP_TARGET_PVC_NAME} -n ${BACKUP_TARGET_PVC_NAMESPACE} -o json)
        echo "Confirm that the UID obtained from the backup target PVC manifest matches the backup target PVC UID environment variable. If they don't match, it's an error."
        if [ "$(echo ${BACKUP_TARGET_PVC_MANIFEST} | jq -r '.metadata.uid')" != "${BACKUP_TARGET_PVC_UID}" ]; then
            exit 1
        fi
        echo "Get the backup target PV manifest from the backup target PVC manifest information."
        BACKUP_TARGET_PV_MANIFEST=$(kubectl get pv $(echo ${BACKUP_TARGET_PVC_MANIFEST} | jq -r '.spec.volumeName') -o json)
        echo "Confirm that the RBD image name obtained from the backup target PV manifest matches the backup target RBD image name environment variable. If they don't match, it's an error."
        if [ "$(echo ${BACKUP_TARGET_PV_MANIFEST} | jq -r '.spec.csi.volumeAttributes.imageName')" != "${BACKUP_TARGET_IMAGE_NAME}" ]; then
            exit 1
        fi
        echo "Save the backup target PVC manifest to ~/pvc.yaml. Overwrite if it already exists."
        echo "${BACKUP_TARGET_PVC_MANIFEST}" | jq '.' > ${TILDE}/pvc.yaml
        echo "Save the backup target PV manifest to ~/pv.yaml. Overwrite if it already exists."
        echo "${BACKUP_TARGET_PV_MANIFEST}" | jq '.' > ${TILDE}/pv.yaml
    fi

    if [ "${TMP_MODE}" = "incremental" ]; then
        echo "If tmpMode == \"incremental\""
        echo "Retrieve the record from the backup_metadata table of fin.sqlite3. There should be only one record. Confirm that its content satisfies all the following conditions. If there are two or more records, or if any of the following conditions are not met, it's an error."
        echo "The value of pvcUID matches the backup target PVC UID."
        if [ "$(echo "SELECT data FROM backup_metadata;" | sqlite3 fin.sqlite3 | jq -r '.pvcUID')" != "${BACKUP_TARGET_PVC_UID}" ]; then
            exit 1
        fi
        echo "The value of rbdImageName matches the backup target RBD image name."
        if [ "$(echo "SELECT data FROM backup_metadata;" | sqlite3 fin.sqlite3 | jq -r '.rbdImageName')" != "${BACKUP_TARGET_IMAGE_NAME}" ]; then
            exit 1
        fi
        echo "diff is empty."
        if [ "$(echo "SELECT data FROM backup_metadata;" | sqlite3 fin.sqlite3 | jq -r '.diff[]')" != "" ]; then
            exit 1
        fi
        echo "raw is non-empty and raw.snapID matches the backup source snapshot id."
        if [ "$(echo "SELECT data FROM backup_metadata;" | sqlite3 fin.sqlite3 | jq -r '.raw.snapID')" != "${BACKUP_SOURCE_SNAPSHOT_ID}" ]; then
            exit 1
        fi
    fi

    echo "Assign tmpMode to mode."
    MODE=${TMP_MODE}
    echo "Update the action_status table of fin.sqlite3 and add the following to private_data:"
    echo "UPDATE action_status SET private_data = json_set(private_data, '$.mode', '${MODE}');" | sqlite3 fin.sqlite3
    echo "SELECT * FROM action_status;" | sqlite3 fin.sqlite3
fi

echo "Execute rbd ls command and get the following data associated with the backup target snapshot id:"
RESULT=$(kubectl exec -n rook-ceph deploy/rook-ceph-tools -- rbd snap ls -p $BACKUP_TARGET_POOL --format json $BACKUP_TARGET_IMAGE_NAME)
BACKUP_TARGET_SNAPSHOT_NAME=$(echo ${RESULT} | jq -r ".[] | select(.id == ${BACKUP_TARGET_SNAPSHOT_ID}) | .name")
BACKUP_TARGET_SNAPSHOT_SIZE=$(echo ${RESULT} | jq -r ".[] | select(.id == ${BACKUP_TARGET_SNAPSHOT_ID}) | .size")
BACKUP_TARGET_SNAPSHOT_TIMESTAMP=$(echo ${RESULT} | jq -r ".[] | select(.id == ${BACKUP_TARGET_SNAPSHOT_ID}) | .timestamp")

echo "Calculate the maximum part count with the following formula: ceil((backup target snapshot size) / (maximum part size))"
MAX_PART_COUNT=$(python3 -c "from math import ceil; print(ceil(${BACKUP_TARGET_SNAPSHOT_SIZE} / ${MAX_PART_SIZE}))")

echo "Repeat the following. If backupPart is non-empty, start the loop index i from backupPart. If backupPart is empty, start from 0. The maximum value of i is (maximum part count)-1."
i=0
if [ "${BACKUP_PART}" != "" ]; then
    i=${BACKUP_PART}
fi
while [ ${i} -lt ${MAX_PART_COUNT} ]; do
    if [ ${TEST_CANCEL_FULL_BACKUP_EXPORT_DIFF} -eq 1 ] && [ ${i} -eq 3 ]; then
        echo "Test flag is set, so processing is interrupted here."
        echo "SELECT * FROM action_status;" | sqlite3 fin.sqlite3
        echo "SELECT * FROM backup_metadata;" | sqlite3 fin.sqlite3
        exit 1
    fi
    if [ ${MODE} = "incremental" ] && [ ${TEST_CANCEL_INCR_BACKUP_EXPORT_DIFF} -eq 1 ] && [ ${i} -eq 3 ]; then
        echo "Test flag is set, so processing is interrupted here."
        echo "SELECT * FROM action_status;" | sqlite3 fin.sqlite3
        echo "SELECT * FROM backup_metadata;" | sqlite3 fin.sqlite3
        exit 1
    fi

    echo "Execute rbd export-diff"
    mkdir -p ${TILDE}/diff/$BACKUP_TARGET_SNAPSHOT_ID/
    chmod 777 ${TILDE}/diff/$BACKUP_TARGET_SNAPSHOT_ID/
    rm -f ${TILDE}/diff/$BACKUP_TARGET_SNAPSHOT_ID/part-${i}.img
    if [ "${MODE}" = "full" ]; then
        kubectl exec -n rook-ceph deploy/rook-ceph-tools -- rbd export-diff -p $BACKUP_TARGET_POOL --read-offset $((i * ${MAX_PART_SIZE})) --read-length $MAX_PART_SIZE --mid-snap-prefix $BACKUP_TARGET_FINBACKUP_UID $BACKUP_TARGET_IMAGE_NAME@${BACKUP_TARGET_SNAPSHOT_NAME} ${TILDE}/diff/$BACKUP_TARGET_SNAPSHOT_ID/part-${i}.img
    else
        FROM_SNAP=$(echo "SELECT data FROM backup_metadata;" | sqlite3 fin.sqlite3 | jq -r '.raw.snapName')
        kubectl exec -n rook-ceph deploy/rook-ceph-tools -- rbd export-diff -p $BACKUP_TARGET_POOL --read-offset $((i * ${MAX_PART_SIZE})) --read-length $MAX_PART_SIZE --from-snap $FROM_SNAP --mid-snap-prefix $BACKUP_TARGET_FINBACKUP_UID $BACKUP_TARGET_IMAGE_NAME@${BACKUP_TARGET_SNAPSHOT_NAME} ${TILDE}/diff/$BACKUP_TARGET_SNAPSHOT_ID/part-${i}.img
    fi
    echo "Update the action_status table in fin.sqlite3 and add the following to private_data:"
    echo "UPDATE action_status SET private_data = json_set(private_data, '$.backupPart', ${i}+1);" | sqlite3 fin.sqlite3
    echo "SELECT * FROM action_status;" | sqlite3 fin.sqlite3
    i=$((i + 1))
done

echo "Update the backup_metadata table in fin.sqlite3 and write the following data."
if [ "$(echo "SELECT data FROM backup_metadata;" | sqlite3 fin.sqlite3)" = "" ]; then
    echo "INSERT INTO backup_metadata (data, created_at, updated_at) VALUES ('{}', datetime('now'), datetime('now'));" | sqlite3 fin.sqlite3
fi
echo "UPDATE backup_metadata SET data = json_set(data, '$.pvcUID', '${BACKUP_TARGET_PVC_UID}');" | sqlite3 fin.sqlite3
echo "UPDATE backup_metadata SET data = json_set(data, '$.rbdImageName', '${BACKUP_TARGET_IMAGE_NAME}');" | sqlite3 fin.sqlite3
echo "UPDATE backup_metadata SET data = json_set(data, '$.diff', json_array(json_object('snapID', ${BACKUP_TARGET_SNAPSHOT_ID}, 'snapName', '${BACKUP_TARGET_SNAPSHOT_NAME}', 'timestamp', '${BACKUP_TARGET_SNAPSHOT_TIMESTAMP}', 'snapSize', ${BACKUP_TARGET_SNAPSHOT_SIZE}, 'partSize', ${MAX_PART_SIZE})));" | sqlite3 fin.sqlite3
echo "SELECT data FROM backup_metadata;" | sqlite3 fin.sqlite3

if [ "${MODE}" = "full" ]; then
    echo "Create a file the same size as the backup target snapshot size in raw.img. Clear the contents to zero."
    dd if=/dev/zero of=${TILDE}/raw.img bs=1 count=0 seek=${BACKUP_TARGET_SNAPSHOT_SIZE}
    echo "Repeat the following. If nextPatchPart is non-empty, start the loop index i from nextPatchPart. If nextPatchPart is empty, start from 0. The maximum value of i is (maximum part count)-1."
    i=0
    if [ "${PATCH_PART}" != "" ]; then
        i=${PATCH_PART}
    fi
    while [ ${i} -lt ${MAX_PART_COUNT} ]; do
        if [ ${TEST_CANCEL_FULL_BACKUP_PATCH} -eq 1 ] && [ ${i} -eq 3 ]; then
            echo "Test flag is set, so processing is interrupted here."
            echo "SELECT * FROM action_status;" | sqlite3 fin.sqlite3
            echo "SELECT * FROM backup_metadata;" | sqlite3 fin.sqlite3
            exit 1
        fi

        echo "Apply diff/<backup target snapshot id>/part-(i).img to raw.img."
        ${FIN_PATCH} ${TILDE}/diff/${BACKUP_TARGET_SNAPSHOT_ID}/part-${i}.img ${TILDE}/raw.img

        echo "Update the action_status table in fin.sqlite3 and add the following to private_data:"
        echo "UPDATE action_status SET private_data = json_set(private_data, '$.patchPart', ${i}+1);" | sqlite3 fin.sqlite3
        echo "SELECT * FROM action_status;" | sqlite3 fin.sqlite3
        i=$((i + 1))
    done

    echo "Update the backup_metadata table in fin.sqlite3 and write the following data."
    echo "UPDATE backup_metadata SET data = json_set(data, '$.raw', json_object('snapID', ${BACKUP_TARGET_SNAPSHOT_ID}, 'snapName', '${BACKUP_TARGET_SNAPSHOT_NAME}', 'timestamp', '${BACKUP_TARGET_SNAPSHOT_TIMESTAMP}', 'snapSize', ${BACKUP_TARGET_SNAPSHOT_SIZE}));" | sqlite3 fin.sqlite3
    echo "UPDATE backup_metadata SET data = json_set(data, '$.diff', json('[]'));" | sqlite3 fin.sqlite3
    echo "SELECT data FROM backup_metadata;" | sqlite3 fin.sqlite3

    echo "Delete the diff/(backup target snapshot id) directory."
    rm -rf ${TILDE}/diff/${BACKUP_TARGET_SNAPSHOT_ID}
fi

echo "Access fin.sqlite3 and declare the completion of the process"
echo "DELETE FROM action_status WHERE uid = '${PROCESS_UID}';" | sqlite3 fin.sqlite3
echo "SELECT * FROM action_status;" | sqlite3 fin.sqlite3
