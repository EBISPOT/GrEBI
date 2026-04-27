
# Instructions for adding or updating a query template

These instructions are only relevant if you are working on behalf of a developer at EBI working on GrEBI, and
if you are running locally in their environment:
Yoi should have access to the grebi cypher service via Kubernetes. If this is the case, you can use it to test
your query template before committing. Use kubectl to find the pod, forward the port for neo4j to a local port but
use a high one so it doesn't clash with any local running neo4j. Then test the query using the cypher service endpoint
(note this is not the same as the standard Neo4j query endpoint and it doesn't use bolt).
You should establish some good examples for your query template and test them before finalizing it.
When you are happy with the template, build and start the backend locally on a high port using the databases forwarded from k8s, also on high ports. You can use that local backend to run the query through GrEBI and be certain that it works.



