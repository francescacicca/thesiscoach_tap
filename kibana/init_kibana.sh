#!/bin/sh
set -e

echo "INIT KIBANA START"

echo "Attendo che Kibana sia disponibile su http://kibana:5601..."
until curl -s http://kibana:5601/api/status | grep -q '"overall":{"level":"available"'; do
  echo "Kibana non è ancora pronta. Riprovo tra 5 secondi"
  sleep 5
done

echo "Kibana è ONLINE"

echo "Importazione della dashboard in corso..."

RESPONSE=$(curl -s -X POST "http://kibana:5601/api/saved_objects/_import?overwrite=true" \
  -H "kbn-xsrf: true" \
  -H "Content-Type: multipart/form-data" \
  -F "file=@/dashboards.ndjson")


if echo "$RESPONSE" | grep -q '"success":true'; then
  echo "DASHBOARD IMPORTATA CON SUCCESSO!"
else
  echo "ERRORE NELL'IMPORTAZIONE:"
  echo "$RESPONSE"
  exit 1
fi

echo "INIT KIBANA END - Il container può terminare"