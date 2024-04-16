#!/bin/bash

curl "https://www.genenames.org/cgi-bin/download/custom?col=gd_hgnc_id&col=gd_pub_ensembl_id&status=Approved&status=Entry%20Withdrawn&hgnc_dbtag=on&order_by=gd_hgnc_id&format=text&submit=submit" > hgnc_ensembl.tsv
