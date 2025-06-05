#!/usr/bin/env bash

set -euo pipefail
set -x

# Process UID
PROCESS_UID=45ed9578-36f3-4e60-9cee-b63673e9797d
# Backup target snapshot id
BACKUP_TARGET_SNAPSHOT_ID=3
# Backup target PVC name
BACKUP_TARGET_PVC_NAME=test-pvc
# Backup target PVC namespace
BACKUP_TARGET_PVC_NAMESPACE=default

# Test flag. Stop during applying incremental data.
TEST_CANCEL_PATCH_APPLICATION=0
# Test flag. Stop after backup_metadata is updated.
TEST_CANCEL_AFTER_BACKUP_METADATA_UPDATE=0

TILDE=/tmp/testfin
FIN_PATCH=~/fin/main

isempty(){
    v="$1"
    [ -z "${v}" ] || [ "${v}" = "null" ] || [ "${v}" = "[]" ] || [ "${v}" = "{}" ]
}

echo "Check the action_status table in fin.sqlite3 and confirm if the process can proceed. Use the process UID as the uid. If good, create a record in the action_status table and proceed to the next step. Set deletion for action. If not, wait for some time and retry from this step."
PROCESS_UID_IN_DB=$(cat <<EOS | sqlite3 fin.sqlite3
SELECT uid FROM action_status;
EOS
)
if isempty "${PROCESS_UID_IN_DB}"; then
cat <<EOS | sqlite3 fin.sqlite3
INSERT INTO action_status (uid, action, private_data, created_at, updated_at)
VALUES ('${PROCESS_UID}', 'deletion', '{}', datetime('now'), datetime('now'));
SELECT * FROM action_status;
EOS
elif [ "${PROCESS_UID_IN_DB}" != "${PROCESS_UID}" ]; then
    echo "Process UID does not match. Current process UID: ${PROCESS_UID_IN_DB}, Expected process UID: ${PROCESS_UID}"
    exit 1
fi

echo "Get the private_data from the action_status table in fin.sqlite3. If the value is not empty, read the following values and assign them to variables of the same name. If no value exists, the variable content is empty."
PRIVATE_DATA=$(cat <<EOS | sqlite3 fin.sqlite3
SELECT private_data FROM action_status;
EOS
)
RAW=$(echo "${PRIVATE_DATA}" | jq -r '.raw // ""')
DIFF0=$(echo "${PRIVATE_DATA}" | jq -r '.diff[0] // ""')
NEXT_PATCH_PART=$(echo "${PRIVATE_DATA}" | jq -r '.nextPatchPart // ""')

echo "If the private_data value was empty in the previous step, get the following values from the backup_metadata table and assign them to variables of the same name. If no value exists, the variable content is empty."
if isempty "${PRIVATE_DATA}"; then
BACKUP_METADATA=$(cat <<EOS | sqlite3 fin.sqlite3
SELECT data FROM backup_metadata
EOS
)
RAW=$(echo ${BACKUP_METADATA} | jq -r '.raw // ""')
DIFF0=$(echo ${BACKUP_METADATA} | jq -r '.diff[0] // ""')
fi

echo "If raw is empty and diff[0] is also empty, access fin.sqlite3 and declare the completion of the process, then exit normally."
if isempty "${RAW}" && isempty "${DIFF0}"; then
    echo "DELETE FROM action_status WHERE uid = '${PROCESS_UID}';" | sqlite3 fin.sqlite3
    echo "SELECT * FROM action_status;" | sqlite3 fin.sqlite3
    exit 0
fi

echo "If raw is empty and diff[0] is non-empty, it's an error."
if isempty "${RAW}" && ! isempty "${DIFF0}"; then
    exit 1
fi

echo "Compare raw.snapID with backup target snapshot id. If they match, do nothing and proceed to the next step. If raw.snapID is larger, access fin.sqlite3 and declare the completion of the process, then exit normally. If raw.snapID is smaller, access fin.sqlite3 and declare the completion of the process, then exit with an error."
RAW_SNAP_ID=$(echo "${RAW}" | jq -r '.snapID')
if [ "${RAW_SNAP_ID}" -eq "${BACKUP_TARGET_SNAPSHOT_ID}" ]; then
    : # Do nothing
elif [ "${RAW_SNAP_ID}" -gt "${BACKUP_TARGET_SNAPSHOT_ID}" ]; then
    echo "DELETE FROM action_status WHERE uid = '${PROCESS_UID}';" | sqlite3 fin.sqlite3
    echo "SELECT * FROM action_status;" | sqlite3 fin.sqlite3
    exit 0
else # -lt
    echo "DELETE FROM action_status WHERE uid = '${PROCESS_UID}';" | sqlite3 fin.sqlite3
    echo "SELECT * FROM action_status;" | sqlite3 fin.sqlite3
    exit 1
fi

echo "Update the action_status table in fin.sqlite3 and add the following to private_data:"
echo "UPDATE action_status SET private_data = json_set(private_data, '$.raw', json('${RAW}'));" | sqlite3 fin.sqlite3
echo "UPDATE action_status SET private_data = json_set(private_data, '$.diff', json('[${DIFF0}]'));" | sqlite3 fin.sqlite3
echo "SELECT * FROM action_status;" | sqlite3 fin.sqlite3

echo "If diff[0] is empty:"
if isempty "${DIFF0}"; then
    echo "Delete raw.img. If it doesn't exist, proceed to the next step."
    rm -f "${TILDE}/raw.img"

    echo "Update the backup_metadata table in fin.sqlite3 and write the following data. Fields not specified are kept with their existing values."
    echo "UPDATE backup_metadata SET data = json_set(data, '$.raw', json('null'));" | sqlite3 fin.sqlite3
else
    echo "Calculate the maximum part count with the following formula: ceil(diff[0].snapSize / diff[0].partSize)"
    MAX_PART_COUNT=$(python3 -c "from math import ceil; print(ceil($(echo "${DIFF0}" | jq -r '.snapSize') / $(echo "${DIFF0}" | jq -r '.partSize')))")

    echo "Repeat the following. If nextPatchPart is non-empty, start the loop index i from nextPatchPart. If nextPatchPart is empty, start from 0. The maximum value of i is (maximum part count)-1."
    i=0
    if ! isempty "${NEXT_PATCH_PART}"; then
        i=${NEXT_PATCH_PART}
    fi
    while [ ${i} -lt ${MAX_PART_COUNT} ]; do
        if [ ${TEST_CANCEL_PATCH_APPLICATION} -eq 1 ] && [ ${i} -eq 3 ]; then
            echo "Test flag is set, so processing is interrupted here."
            echo "SELECT * FROM action_status;" | sqlite3 fin.sqlite3
            echo "SELECT * FROM backup_metadata;" | sqlite3 fin.sqlite3
            exit 1
        fi

        echo "Apply diff/<backup target snapshot id>/part-(i).img to raw.img."
        ${FIN_PATCH} ${TILDE}/diff/$(echo "${DIFF0}" | jq -r '.snapID')/part-${i}.img ${TILDE}/raw.img

        echo "Update the action_status table in fin.sqlite3 and add the following to private_data:"
        echo "UPDATE action_status SET private_data = json_set(private_data, '$.nextPatchPart', ${i}+1);" | sqlite3 fin.sqlite3
        echo "SELECT * FROM action_status;" | sqlite3 fin.sqlite3
        i=$((i + 1))
    done
    rm -rf "${TILDE}/diff/$(echo "${DIFF0}" | jq -r '.snapID')"
    echo "UPDATE backup_metadata SET data = json_set(data, '$.raw', json('${DIFF0}'));" | sqlite3 fin.sqlite3
    echo "UPDATE backup_metadata SET data = json_set(data, '$.diff', json('[]'));" | sqlite3 fin.sqlite3
fi

if [ ${TEST_CANCEL_AFTER_BACKUP_METADATA_UPDATE} -eq 1 ]; then
    echo "Test flag is set, so processing is interrupted here."
    echo "SELECT * FROM action_status;" | sqlite3 fin.sqlite3
    echo "SELECT * FROM backup_metadata;" | sqlite3 fin.sqlite3
    exit 1
fi

echo "Access fin.sqlite3 and declare the completion of the process, then exit normally."
echo "DELETE FROM action_status WHERE uid = '${PROCESS_UID}';" | sqlite3 fin.sqlite3
echo "SELECT * FROM action_status;" | sqlite3 fin.sqlite3
echo "SELECT * FROM backup_metadata;" | sqlite3 fin.sqlite3
sha256sum "${TILDE}/raw.img"

exit 0
