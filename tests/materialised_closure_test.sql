-- Self-contained validation of the closure-at-query-time SQL used to serve
-- materialised parameterised templates (docs/materialise-query-templates.md).
--
-- Reproduces the descendant/ancestor/exact closure over the biolink:broad_match
-- transitive closure and the typed `"<col>_nid" = ANY(node ids)` row filter
-- (each materialised query is its own typed matq_ table; GraphNodeId columns
-- store the base node's id), against a tiny A<-B<-C<-D hierarchy (matching
-- tests/expected_output/test_ubergraph).
--
-- Run:  psql -f tests/materialised_closure_test.sql   (raises on any mismatch)

BEGIN;

CREATE TEMP TABLE "nodes_x" ("grebi:nodeId" TEXT, "grebi:sourceIds" TEXT[]) ON COMMIT DROP;
CREATE TEMP TABLE "edges_x" ("grebi:type" TEXT, "grebi:fromNodeId" TEXT, "grebi:toNodeId" TEXT) ON COMMIT DROP;
CREATE TEMP TABLE "matq_x_q1" (row_number INT, cell_nid TEXT, "_count" BIGINT) ON COMMIT DROP;

-- nodeId distinct from curie, to exercise the curie<->nodeId mapping; X is isolated
INSERT INTO "nodes_x" VALUES
 ('grp_A', ARRAY['ex:A','EX:A']),('grp_B', ARRAY['ex:B']),
 ('grp_C', ARRAY['ex:C']),('grp_D', ARRAY['ex:D']),('grp_X', ARRAY['ex:X']);

-- broad_match points descendant -> ancestor (full transitive closure)
INSERT INTO "edges_x" VALUES
 ('biolink:broad_match','grp_B','grp_A'),('biolink:broad_match','grp_C','grp_A'),
 ('biolink:broad_match','grp_C','grp_B'),('biolink:broad_match','grp_D','grp_A'),
 ('biolink:broad_match','grp_D','grp_B'),('biolink:broad_match','grp_D','grp_C');

INSERT INTO "matq_x_q1" VALUES
 (1, 'grp_A', 10),
 (2, 'grp_B', 20),
 (3, 'grp_C', 30),
 (4, 'grp_D', 40),
 (5, 'grp_X', 99);

-- closure(queried, kind) -> matching row count and summed _count, mirroring
-- GrebiPostgresClient.closureNodeIdSet + the `cell_nid = ANY(node ids)` filter.
CREATE FUNCTION pg_temp.hits(queried TEXT, kind TEXT, OUT n BIGINT, OUT total BIGINT) AS $$
  WITH pnodes AS (SELECT "grebi:nodeId" AS nid FROM "nodes_x" WHERE "grebi:sourceIds" && ARRAY[queried]),
       nn AS (
         SELECT nid FROM pnodes
         UNION
         SELECT CASE WHEN kind='descendants' THEN "grebi:fromNodeId" ELSE "grebi:toNodeId" END
         FROM "edges_x"
         WHERE kind IN ('descendants','ancestors') AND "grebi:type"='biolink:broad_match'
           AND (CASE WHEN kind='descendants' THEN "grebi:toNodeId" ELSE "grebi:fromNodeId" END)
               IN (SELECT nid FROM pnodes)
       )
  SELECT count(*), COALESCE(SUM("_count"),0)
  FROM "matq_x_q1"
  WHERE cell_nid IN (SELECT nid FROM nn);
$$ LANGUAGE sql;

DO $$
DECLARE r RECORD;
BEGIN
  r := pg_temp.hits('ex:A','descendants'); ASSERT r.n=4  AND r.total=100, 'descendants(A)=A,B,C,D';
  r := pg_temp.hits('ex:B','descendants'); ASSERT r.n=3  AND r.total=90,  'descendants(B)=B,C,D';
  r := pg_temp.hits('ex:C','descendants'); ASSERT r.n=2  AND r.total=70,  'descendants(C)=C,D';
  r := pg_temp.hits('ex:D','descendants'); ASSERT r.n=1  AND r.total=40,  'descendants(D)=D';
  r := pg_temp.hits('ex:X','descendants'); ASSERT r.n=1  AND r.total=99,  'isolated X -> just X';
  r := pg_temp.hits('ex:UNKNOWN','descendants'); ASSERT r.n=0, 'unknown node -> no rows';
  r := pg_temp.hits('ex:B','exact');       ASSERT r.n=1  AND r.total=20,  'exact(B)=B';
  r := pg_temp.hits('EX:A','exact');       ASSERT r.n=1  AND r.total=10,  'exact via clique member EX:A';
  r := pg_temp.hits('ex:D','ancestors');   ASSERT r.n=4  AND r.total=100, 'ancestors(D)=D,A,B,C';
  r := pg_temp.hits('ex:B','ancestors');   ASSERT r.n=2  AND r.total=30,  'ancestors(B)=B,A';
  RAISE NOTICE 'materialised_closure_test: ALL ASSERTIONS PASSED';
END $$;

ROLLBACK;
