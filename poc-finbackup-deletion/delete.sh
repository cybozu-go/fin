#!/usr/bin/env bash

set -euo pipefail
set -x

#処理 UID
PROCESS_UID=45ed9578-36f3-4e60-9cee-b63673e9797d
#バックアップ対象 snapshot id
BACKUP_TARGET_SNAPSHOT_ID=3
#バックアップ対象 PVC name
BACKUP_TARGET_PVC_NAME=test-pvc
#バックアップ対象 PVC namespace
BACKUP_TARGET_PVC_NAMESPACE=default

#テスト用のフラグ。増分データの適用を途中で止める。
TEST_CANCEL_PATCH_APPLICATION=0
#テスト用のフラグ。backup_metadata の書き換えが済んだ後で止める。
TEST_CANCEL_AFTER_BACKUP_METADATA_UPDATE=0

TILDE=/tmp/testfin
FIN_PATCH=~/fin/main

isempty(){
    v="$1"
    [ -z "${v}" ] || [ "${v}" = "null" ] || [ "${v}" = "[]" ] || [ "${v}" = "{}" ]
}

echo "fin.sqlite3 の action_status テーブルを確認し、処理を先に進めてよいか確認する。この際 uid として処理 UID を用いる。良い場合は action_status テーブルにレコードを作成した上で次に進む。action には deletion を設定する。ダメな場合は適当な時間待った後に再度このステップからやり直す。"
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
    echo "処理 UID が一致しません。現在の処理 UID: ${PROCESS_UID_IN_DB}、期待される処理 UID: ${PROCESS_UID}"
    exit 1
fi

echo "fin.sqlite3 の action_status テーブルの private_data を取得する。値が空でない場合、以下の値を読みとり、同名の変数に入れる。値が無い場合、変数の中身は空とする。"
PRIVATE_DATA=$(cat <<EOS | sqlite3 fin.sqlite3
SELECT private_data FROM action_status;
EOS
)
RAW=$(echo "${PRIVATE_DATA}" | jq -r '.raw // ""')
DIFF0=$(echo "${PRIVATE_DATA}" | jq -r '.diff[0] // ""')
NEXT_PATCH_PART=$(echo "${PRIVATE_DATA}" | jq -r '.nextPatchPart // ""')

echo "前のステップで private_data の値が空だった場合 backup_metadata テーブルから以下の値を取得し同名の変数に入れる。値が存在しない場合、変数の中身は空とする。"
if isempty "${PRIVATE_DATA}"; then
BACKUP_METADATA=$(cat <<EOS | sqlite3 fin.sqlite3
SELECT data FROM backup_metadata
EOS
)
RAW=$(echo ${BACKUP_METADATA} | jq -r '.raw // ""')
DIFF0=$(echo ${BACKUP_METADATA} | jq -r '.diff[0] // ""')
fi

echo "raw が空、かつ diff[0] が空の場合、fin.sqlite3 にアクセスし処理の完了を宣言した後、正常終了する。"
if isempty "${RAW}" && isempty "${DIFF0}"; then
    echo "DELETE FROM action_status WHERE uid = '${PROCESS_UID}';" | sqlite3 fin.sqlite3
    echo "SELECT * FROM action_status;" | sqlite3 fin.sqlite3
    exit 0
fi

echo "raw が空、かつ diff[0] が非空の場合、エラーとする。"
if isempty "${RAW}" && ! isempty "${DIFF0}"; then
    exit 1
fi

echo "raw.snapID とバックアップ対象 snapshot id を比較する。一致する場合は何もせず次のステップに進む。raw.snapID がより大きい場合は fin.sqlite3 にアクセスし処理の完了を宣言した後、正常終了する。raw.snapID がより小さい場合は fin.sqlite3 にアクセスし処理の完了を宣言した後、エラーとする。"
RAW_SNAP_ID=$(echo "${RAW}" | jq -r '.snapID')
if [ "${RAW_SNAP_ID}" -eq "${BACKUP_TARGET_SNAPSHOT_ID}" ]; then
    : # 何もしない
elif [ "${RAW_SNAP_ID}" -gt "${BACKUP_TARGET_SNAPSHOT_ID}" ]; then
    echo "DELETE FROM action_status WHERE uid = '${PROCESS_UID}';" | sqlite3 fin.sqlite3
    echo "SELECT * FROM action_status;" | sqlite3 fin.sqlite3
    exit 0
else # -lt
    echo "DELETE FROM action_status WHERE uid = '${PROCESS_UID}';" | sqlite3 fin.sqlite3
    echo "SELECT * FROM action_status;" | sqlite3 fin.sqlite3
    exit 1
fi

echo "fin.sqlite3 の action_status テーブルを更新し、private_data に以下を追記する："
echo "UPDATE action_status SET private_data = json_set(private_data, '$.raw', json('${RAW}'));" | sqlite3 fin.sqlite3
echo "UPDATE action_status SET private_data = json_set(private_data, '$.diff', json('[${DIFF0}]'));" | sqlite3 fin.sqlite3
echo "SELECT * FROM action_status;" | sqlite3 fin.sqlite3

echo "diff[0] が空の場合："
if isempty "${DIFF0}"; then
    echo "raw.img を削除する。存在しない場合は次のステップに進む。"
    rm -f "${TILDE}/raw.img"

    echo "fin.sqlite3 の backup_metadata テーブルを更新し以下のデータを書き込む。明記しないフィールドはそのままの値を保持する。"
    echo "UPDATE backup_metadata SET data = json_set(data, '$.raw', json('null'));" | sqlite3 fin.sqlite3
else
    echo "最大 part 数を以下の式で計算する: ceil(diff[0].snapSize / diff[0].partSize)"
    MAX_PART_COUNT=$(python3 -c "from math import ceil; print(ceil($(echo "${DIFF0}" | jq -r '.snapSize') / $(echo "${DIFF0}" | jq -r '.partSize')))")

    echo "以下を繰り返す。nextPatchPart が非空の場合繰り返しの添え字 i は nextPatchPart から始める。nextPatchPart が空の場合は 0 から始める。i の最大値は (最大 part 数)-1 である。"
    i=0
    if ! isempty "${NEXT_PATCH_PART}"; then
        i=${NEXT_PATCH_PART}
    fi
    while [ ${i} -lt ${MAX_PART_COUNT} ]; do
        if [ ${TEST_CANCEL_PATCH_APPLICATION} -eq 1 ] && [ ${i} -eq 3 ]; then
            echo "テスト用のフラグが立っているので、ここで処理を中断する。"
            echo "SELECT * FROM action_status;" | sqlite3 fin.sqlite3
            echo "SELECT * FROM backup_metadata;" | sqlite3 fin.sqlite3
            exit 1
        fi

        echo "raw.img に diff/<バックアップ対象 snapshot id>/part-(i).img を適用する。"
        ${FIN_PATCH} ${TILDE}/diff/$(echo "${DIFF0}" | jq -r '.snapID')/part-${i}.img ${TILDE}/raw.img

        echo "fin.sqlite3 の action_status テーブルを更新し、private_data に以下を追記する："
        echo "UPDATE action_status SET private_data = json_set(private_data, '$.nextPatchPart', ${i}+1);" | sqlite3 fin.sqlite3
        echo "SELECT * FROM action_status;" | sqlite3 fin.sqlite3
        i=$((i + 1))
    done
    rm -rf "${TILDE}/diff/$(echo "${DIFF0}" | jq -r '.snapID')"
    echo "UPDATE backup_metadata SET data = json_set(data, '$.raw', json('${DIFF0}'));" | sqlite3 fin.sqlite3
    echo "UPDATE backup_metadata SET data = json_set(data, '$.diff', json('[]'));" | sqlite3 fin.sqlite3
fi

if [ ${TEST_CANCEL_AFTER_BACKUP_METADATA_UPDATE} -eq 1 ]; then
    echo "テスト用のフラグが立っているので、ここで処理を中断する。"
    echo "SELECT * FROM action_status;" | sqlite3 fin.sqlite3
    echo "SELECT * FROM backup_metadata;" | sqlite3 fin.sqlite3
    exit 1
fi

echo "fin.sqlite3 にアクセスし、処理の完了を宣言したあと、正常終了する。"
echo "DELETE FROM action_status WHERE uid = '${PROCESS_UID}';" | sqlite3 fin.sqlite3
echo "SELECT * FROM action_status;" | sqlite3 fin.sqlite3
echo "SELECT * FROM backup_metadata;" | sqlite3 fin.sqlite3
sha256sum "${TILDE}/raw.img"

exit 0
