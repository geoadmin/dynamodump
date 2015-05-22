#/bin/bash


BUCKET_NAME=public.geo.admin.ch


TARGET_DIR=/var/backups/dynamodb/${BUCKET_NAME}


if [[ ! -d "${TARGET_DIR}" ]]; then
   mkdir -p ${TARGET_DIR}
fi

~/dynamodump/venv/bin/s3cmd   --config ~/.s3cfg  --no-check-md5 --no-guess-mime-type   --mime-type='application/vnd.google-earth.kml+xml'  sync    /var/backups/dynamodb/${BUCKET_NAME}   s3://${BUCKET_NAME} 

