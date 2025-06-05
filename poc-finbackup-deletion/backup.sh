#!/usr/bin/env bash

set -euo pipefail
set -x

#処理 UID
PROCESS_UID=0511bbec-3861-4a67-9a5e-a872ae161859
#バックアップ対象 FinBackup UID
BACKUP_TARGET_FINBACKUP_UID=0511bbec-3861-4a67-9a5e-a872ae161859
#バックアップ対象 pool
BACKUP_TARGET_POOL=replicapool
#バックアップ対象 image name
BACKUP_TARGET_IMAGE_NAME=csi-vol-d4527a69-ccd6-42ba-94a3-73ea85c2ad4c
if [ "$1" = "full" ] ; then
    # フルバックアップ
    #バックアップ対象 snapshot id
    BACKUP_TARGET_SNAPSHOT_ID=3
    #バックアップ基準 snapshot id
    BACKUP_SOURCE_SNAPSHOT_ID=""
else
    # 増分バックアップ
    #バックアップ対象 snapshot id
    BACKUP_TARGET_SNAPSHOT_ID=4
    #バックアップ基準 snapshot id
    BACKUP_SOURCE_SNAPSHOT_ID=3
fi
#バックアップ対象 PVC name
BACKUP_TARGET_PVC_NAME=test-pvc
#バックアップ対象 PVC namespace
BACKUP_TARGET_PVC_NAMESPACE=default
#バックアップ対象 PVC UID
BACKUP_TARGET_PVC_UID=40dceae2-1592-4a09-bcc8-7ce8947877f0
#最大 part サイズ
MAX_PART_SIZE=104857600 # 100 MiB

#テスト用のフラグ。フルバックアップの増分データの保存を途中で止める。
TEST_CANCEL_FULL_BACKUP_EXPORT_DIFF=0
#テスト用のフラグ。フルバックアップの増分データの適用を途中で止める。
TEST_CANCEL_FULL_BACKUP_PATCH=0
#テスト用のフラグ。増分バックアップの増分データの保存を途中で止める。
TEST_CANCEL_INCR_BACKUP_EXPORT_DIFF=0

TILDE=/tmp/testfin
FIN_PATCH=~/fin/main

echo "fin.sqlite3 の action_status テーブルを確認し、処理を先に進めてよいか確認する。この際 uid としてバックアップ対象 FinBackup UID を用いる。良い場合は次に進む。ダメな場合は適当な時間待った後に再度このステップからやり直す。"
sqlite3 fin.sqlite3 <<EOS
CREATE TABLE IF NOT EXISTS action_status (
    uid TEXT NOT NULL PRIMARY KEY, -- (immutable) 処理を一意に識別する uid をあらわす。
    action TEXT NOT NULL, -- (immutable) バックアップ作成、削除、リストアといった処理の種類をあらわす。action が決まれば private カラムの データフォーマットが一意に特定できる。
    private_data BLOB, -- (mutable) 各処理の状態を保持する。データ形式は各処理の詳細設計時に決定する。
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP, -- (immutable) レコード作成時刻
    updated_at DATETIME -- (mutable) レコード更新時刻
);
CREATE TABLE IF NOT EXISTS backup_metadata (
    data BLOB, -- (mutable) 以下で説明する JSON を保持する。
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP, -- (immutable) レコード作成時刻
    updated_at DATETIME -- (mutable) レコード更新時刻
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

echo "fin.sqlite3 の action_status テーブルを更新する："
cat <<EOS | sqlite3 fin.sqlite3
SELECT * FROM action_status;
UPDATE action_status SET action = 'backup';
SELECT * FROM action_status;
EOS

echo "private_data が空でない場合、以下の値を読みとり、同名の変数に入れる。値が無い場合、変数の中身は空とする。"
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
    echo "mode が空の場合："
    echo "フルバックアップと増分バックアップのどちらを実施するかを判定する。結果は新しい変数 tmpMode に代入する："
    if [ "${BACKUP_SOURCE_SNAPSHOT_ID}" = "" ]; then
        echo "入力のバックアップ基準 snapshot id が空文字列の場合："
        echo "フルバックアップと判定し tmpMode に "full" を代入する。"
        TMP_MODE="full"
    else
        echo "それ以外の場合"
        echo "fin.sqlite3 の backup_metadata テーブルにレコードが存在する場合、増分バックアップと判定し tmpMode に "incremental" を代入する。"
        BACKUP_METADATA_COUNT=$(cat <<EOS | sqlite3 fin.sqlite3
SELECT COUNT(*) FROM backup_metadata
EOS
)
        if [ "${BACKUP_METADATA_COUNT}" -gt 0 ]; then
            TMP_MODE="incremental"
        else
            echo "それ以外の場合、フルバックアップと判定し tmpMode に "full" を代入する。"
            TMP_MODE="full"
        fi
    fi

    if [ "${TMP_MODE}" = "full" ]; then
        echo "tmpMode == "full" の場合"
        echo "fin.sqlite3 の backup_metadata テーブルにレコードが一件もないことを確認する。レコードが存在した場合はエラーとする。"
        BACKUP_METADATA_COUNT=$(cat <<EOS | sqlite3 fin.sqlite3
SELECT COUNT(*) FROM backup_metadata
EOS
)
        if [ ${BACKUP_METADATA_COUNT} -gt 0 ]; then
            exit 1
        fi

        echo "バックアップ対象 PVC namespace と バックアップ対象 PVC name から バックアップ対象 PVC のマニフェストを取得する。"
        BACKUP_TARGET_PVC_MANIFEST=$(kubectl get pvc ${BACKUP_TARGET_PVC_NAME} -n ${BACKUP_TARGET_PVC_NAMESPACE} -o json)
        echo "バックアップ対象 PVC のマニフェストから取得した UID と、環境変数のバックアップ対象 PVC UID が一致することを確認する。不一致の場合はエラーとする。"
        if [ "$(echo ${BACKUP_TARGET_PVC_MANIFEST} | jq -r '.metadata.uid')" != "${BACKUP_TARGET_PVC_UID}" ]; then
            exit 1
        fi
        echo "バックアップ対象 PVC のマニフェストの情報から、バックアップ対象 PV のマニフェストを取得する。"
        BACKUP_TARGET_PV_MANIFEST=$(kubectl get pv $(echo ${BACKUP_TARGET_PVC_MANIFEST} | jq -r '.spec.volumeName') -o json)
        echo "バックアップ対象 PV のマニフェストから取得した RBD image name と、環境変数のバックアップ対象 RBD image name が一致することを確認する。不一致の場合はエラーとする。"
        if [ "$(echo ${BACKUP_TARGET_PV_MANIFEST} | jq -r '.spec.csi.volumeAttributes.imageName')" != "${BACKUP_TARGET_IMAGE_NAME}" ]; then
            exit 1
        fi
        echo "~/pvc.yaml に バックアップ対象 PVC のマニフェストを保存する。既に存在する場合は上書きする。"
        echo "${BACKUP_TARGET_PVC_MANIFEST}" | jq '.' > ${TILDE}/pvc.yaml
        echo "~/pv.yaml に バックアップ対象 PV のマニフェストを保存する。既に存在する場合は上書きする。"
        echo "${BACKUP_TARGET_PV_MANIFEST}" | jq '.' > ${TILDE}/pv.yaml
    fi

    if [ "${TMP_MODE}" = "incremental" ]; then
        echo "tmpMode == "incremental" の場合"
        echo "fin.sqlite3 の backup_metadata テーブルのレコードを取得する。これは一件のみあるはずである。その内容が以下の全ての条件を満たすことを確認する。レコードが二件以上ある場合や、以下のいずれかの条件が満たされない場合はエラーとする。"
        echo "pvcUID の値がバックアップ対象 PVC UID と一致する。"
        if [ "$(echo "SELECT data FROM backup_metadata;" | sqlite3 fin.sqlite3 | jq -r '.pvcUID')" != "${BACKUP_TARGET_PVC_UID}" ]; then
            exit 1
        fi
        echo "rbdImageName の値が バックアップ対象 RBD image name と一致する。"
        if [ "$(echo "SELECT data FROM backup_metadata;" | sqlite3 fin.sqlite3 | jq -r '.rbdImageName')" != "${BACKUP_TARGET_IMAGE_NAME}" ]; then
            exit 1
        fi
        echo "diff が空である。"
        if [ "$(echo "SELECT data FROM backup_metadata;" | sqlite3 fin.sqlite3 | jq -r '.diff[]')" != "" ]; then
            exit 1
        fi
        echo "raw が非空で raw.snapID がバックアップ基準 snapshot id と一致する。"
        if [ "$(echo "SELECT data FROM backup_metadata;" | sqlite3 fin.sqlite3 | jq -r '.raw.snapID')" != "${BACKUP_SOURCE_SNAPSHOT_ID}" ]; then
            exit 1
        fi
    fi

    echo "mode に tmpMode を代入する。"
    MODE=${TMP_MODE}
    echo "fin.sqlite3 の action_status テーブルを更新し、private_data に以下を追記する："
    echo "UPDATE action_status SET private_data = json_set(private_data, '$.mode', '${MODE}');" | sqlite3 fin.sqlite3
    echo "SELECT * FROM action_status;" | sqlite3 fin.sqlite3
fi

echo "rbd ls コマンドを実行し、バックアップ対象 snapshot id に紐づく以下のデータを取得する："
RESULT=$(kubectl exec -n rook-ceph deploy/rook-ceph-tools -- rbd snap ls -p $BACKUP_TARGET_POOL --format json $BACKUP_TARGET_IMAGE_NAME)
BACKUP_TARGET_SNAPSHOT_NAME=$(echo ${RESULT} | jq -r ".[] | select(.id == ${BACKUP_TARGET_SNAPSHOT_ID}) | .name")
BACKUP_TARGET_SNAPSHOT_SIZE=$(echo ${RESULT} | jq -r ".[] | select(.id == ${BACKUP_TARGET_SNAPSHOT_ID}) | .size")
BACKUP_TARGET_SNAPSHOT_TIMESTAMP=$(echo ${RESULT} | jq -r ".[] | select(.id == ${BACKUP_TARGET_SNAPSHOT_ID}) | .timestamp")

echo "最大 part 数を以下の式で計算する: ceil((バックアップ対象 snapshot サイズ) / (最大 part サイズ))"
MAX_PART_COUNT=$(python3 -c "from math import ceil; print(ceil(${BACKUP_TARGET_SNAPSHOT_SIZE} / ${MAX_PART_SIZE}))")

echo "以下を繰り返す。backupPart が非空の場合、繰り返しの添え字 i は backupPart から始める。backupPart が空の場合は 0 から始める。i の最大値は (最大 part 数)-1 である。"
i=0
if [ "${BACKUP_PART}" != "" ]; then
    i=${BACKUP_PART}
fi
while [ ${i} -lt ${MAX_PART_COUNT} ]; do
    if [ ${TEST_CANCEL_FULL_BACKUP_EXPORT_DIFF} -eq 1 ] && [ ${i} -eq 3 ]; then
        echo "テスト用のフラグが立っているので、ここで処理を中断する。"
        echo "SELECT * FROM action_status;" | sqlite3 fin.sqlite3
        echo "SELECT * FROM backup_metadata;" | sqlite3 fin.sqlite3
        exit 1
    fi
    if [ ${MODE} = "incremental" ] && [ ${TEST_CANCEL_INCR_BACKUP_EXPORT_DIFF} -eq 1 ] && [ ${i} -eq 3 ]; then
        echo "テスト用のフラグが立っているので、ここで処理を中断する。"
        echo "SELECT * FROM action_status;" | sqlite3 fin.sqlite3
        echo "SELECT * FROM backup_metadata;" | sqlite3 fin.sqlite3
        exit 1
    fi

    echo "rbd export-diff を実行する"
    mkdir -p ${TILDE}/diff/$BACKUP_TARGET_SNAPSHOT_ID/
    chmod 777 ${TILDE}/diff/$BACKUP_TARGET_SNAPSHOT_ID/
    rm -f ${TILDE}/diff/$BACKUP_TARGET_SNAPSHOT_ID/part-${i}.img
    if [ "${MODE}" = "full" ]; then
        kubectl exec -n rook-ceph deploy/rook-ceph-tools -- rbd export-diff -p $BACKUP_TARGET_POOL --read-offset $((i * ${MAX_PART_SIZE})) --read-length $MAX_PART_SIZE --mid-snap-prefix $BACKUP_TARGET_FINBACKUP_UID $BACKUP_TARGET_IMAGE_NAME@${BACKUP_TARGET_SNAPSHOT_NAME} ${TILDE}/diff/$BACKUP_TARGET_SNAPSHOT_ID/part-${i}.img
    else
        FROM_SNAP=$(echo "SELECT data FROM backup_metadata;" | sqlite3 fin.sqlite3 | jq -r '.raw.snapName')
        kubectl exec -n rook-ceph deploy/rook-ceph-tools -- rbd export-diff -p $BACKUP_TARGET_POOL --read-offset $((i * ${MAX_PART_SIZE})) --read-length $MAX_PART_SIZE --from-snap $FROM_SNAP --mid-snap-prefix $BACKUP_TARGET_FINBACKUP_UID $BACKUP_TARGET_IMAGE_NAME@${BACKUP_TARGET_SNAPSHOT_NAME} ${TILDE}/diff/$BACKUP_TARGET_SNAPSHOT_ID/part-${i}.img
    fi
    echo "fin.sqlite3 の action_status テーブルを更新し、private_data に以下を追記する："
    echo "UPDATE action_status SET private_data = json_set(private_data, '$.backupPart', ${i}+1);" | sqlite3 fin.sqlite3
    echo "SELECT * FROM action_status;" | sqlite3 fin.sqlite3
    i=$((i + 1))
done

echo "fin.sqlite3 の backup_metadata テーブルを更新し以下のデータを書き込む。"
if [ "$(echo "SELECT data FROM backup_metadata;" | sqlite3 fin.sqlite3)" = "" ]; then
    echo "INSERT INTO backup_metadata (data, created_at, updated_at) VALUES ('{}', datetime('now'), datetime('now'));" | sqlite3 fin.sqlite3
fi
echo "UPDATE backup_metadata SET data = json_set(data, '$.pvcUID', '${BACKUP_TARGET_PVC_UID}');" | sqlite3 fin.sqlite3
echo "UPDATE backup_metadata SET data = json_set(data, '$.rbdImageName', '${BACKUP_TARGET_IMAGE_NAME}');" | sqlite3 fin.sqlite3
echo "UPDATE backup_metadata SET data = json_set(data, '$.diff', json_array(json_object('snapID', ${BACKUP_TARGET_SNAPSHOT_ID}, 'snapName', '${BACKUP_TARGET_SNAPSHOT_NAME}', 'timestamp', '${BACKUP_TARGET_SNAPSHOT_TIMESTAMP}', 'snapSize', ${BACKUP_TARGET_SNAPSHOT_SIZE}, 'partSize', ${MAX_PART_SIZE})));" | sqlite3 fin.sqlite3
echo "SELECT data FROM backup_metadata;" | sqlite3 fin.sqlite3

if [ "${MODE}" = "full" ]; then
    echo "raw.img にバックアップ対象 snapshot サイズと同じ大きさのファイルを作成する。中身はゼロクリアする。"
    dd if=/dev/zero of=${TILDE}/raw.img bs=1 count=0 seek=${BACKUP_TARGET_SNAPSHOT_SIZE}
    echo "以下を繰り返す。nextPatchPart が非空の場合繰り返しの添え字 i は nextPatchPart から始める。nextPatchPart が空の場合は 0 から始める。i の最大値は (最大 part 数)-1 である。"
    i=0
    if [ "${PATCH_PART}" != "" ]; then
        i=${PATCH_PART}
    fi
    while [ ${i} -lt ${MAX_PART_COUNT} ]; do
        if [ ${TEST_CANCEL_FULL_BACKUP_PATCH} -eq 1 ] && [ ${i} -eq 3 ]; then
            echo "テスト用のフラグが立っているので、ここで処理を中断する。"
            echo "SELECT * FROM action_status;" | sqlite3 fin.sqlite3
            echo "SELECT * FROM backup_metadata;" | sqlite3 fin.sqlite3
            exit 1
        fi

        echo "raw.img に diff/<バックアップ対象 snapshot id>/part-(i).img を適用する。"
        ${FIN_PATCH} ${TILDE}/diff/${BACKUP_TARGET_SNAPSHOT_ID}/part-${i}.img ${TILDE}/raw.img

        echo "fin.sqlite3 の action_status テーブルを更新し、private_data に以下を追記する："
        echo "UPDATE action_status SET private_data = json_set(private_data, '$.patchPart', ${i}+1);" | sqlite3 fin.sqlite3
        echo "SELECT * FROM action_status;" | sqlite3 fin.sqlite3
        i=$((i + 1))
    done

    echo "fin.sqlite3 の backup_metadata テーブルを更新し以下のデータを書き込む。"
    echo "UPDATE backup_metadata SET data = json_set(data, '$.raw', json_object('snapID', ${BACKUP_TARGET_SNAPSHOT_ID}, 'snapName', '${BACKUP_TARGET_SNAPSHOT_NAME}', 'timestamp', '${BACKUP_TARGET_SNAPSHOT_TIMESTAMP}', 'snapSize', ${BACKUP_TARGET_SNAPSHOT_SIZE}));" | sqlite3 fin.sqlite3
    echo "UPDATE backup_metadata SET data = json_set(data, '$.diff', json('[]'));" | sqlite3 fin.sqlite3
    echo "SELECT data FROM backup_metadata;" | sqlite3 fin.sqlite3

    echo "diff/(バックアップ対象 snapshot id) ディレクトリを削除する。"
    rm -rf ${TILDE}/diff/${BACKUP_TARGET_SNAPSHOT_ID}
fi

echo "fin.sqlite3 にアクセスし、処理の完了を宣言する"
echo "DELETE FROM action_status WHERE uid = '${PROCESS_UID}';" | sqlite3 fin.sqlite3
echo "SELECT * FROM action_status;" | sqlite3 fin.sqlite3
