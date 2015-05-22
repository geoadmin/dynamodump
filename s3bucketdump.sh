#/bin/bash


BUCKET_NAME=public.geo.admin.ch


TARGET_DIR=/var/backups/dynamodb/${BUCKET_NAME}


if [[ ! -d "${TARGET_DIR}" ]]; then
   mkdir -p ${TARGET_DIR}
fi

${HOME}/dynamodump/venv/bin/s3cmd   --no-check-md5  --config ${HOME}/.s3cfg  sync s3://${BUCKET_NAME}  ${TARGET_DIR}

