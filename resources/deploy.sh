#!/bin/bash

HOST=10.16.45.170
PORT=9200
CRAWL_JOB=$1

INDEX_ADDR=http://${HOST}:${PORT}/${CRAWL_JOB}

sed -e 's/{{ crawl_job }}/${CRAWL_JOB}/g' _indexer > /tmp/_indexer.conf
scp /tmp/_indexer.conf ${HOST}:/etc/init/${CRAWL_JOB}_indexer.conf
scp run.sh ${HOST}:/usr/local/packages/atrax_indexer/run.sh

#curl -X POST '${INDEX_ADDR}/_settings' -d @sel.crawler.indexer/settings.json
#curl -X POST '${INDEX_ADDR}/_mappings' -d @sel.crawler.indexer/text_document_mappings.json
