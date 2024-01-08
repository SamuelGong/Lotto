#!/bin/bash

ORIGINAL_DIR=$(pwd)
ADAPTER="ens5"

case "$1" in
    limit_both)
      sudo wondershaper -a "${ADAPTER}" -u "$2" -d "$3"
      ;;
    limit_download)
      sudo wondershaper -a "${ADAPTER}" -d "$2"
      ;;
    limit_upload)
      sudo wondershaper -a "${ADAPTER}" -u "$2"
      ;;
    clean_limit)
      sudo wondershaper -c -a "${ADAPTER}"
      ;;
    *)
      echo "Unknown command!"
      ;;
esac

cd ${ORIGINAL_DIR}