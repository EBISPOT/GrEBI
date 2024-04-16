curl "https://g-a8b222.dd271.03c0.data.globus.org/pub/databases/genenames/hgnc/json/hgnc_complete_set.json" | jq -c .response.docs[] > hgnc.json


