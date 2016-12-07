set HOST=10.16.45.170
set PORT=9200
set CRAWL_JOB=%1
set INDEX_ADDR=http://%HOST%:%PORT%/%CRAWL_JOB%

curl -X POST "%INDEX_ADDR%" -d @sel/crawler/indexer/settings.json
curl -X PUT "%INDEX_ADDR%/_mapping/text_document" -d @sel/crawler/indexer/text_document_mappings.json
