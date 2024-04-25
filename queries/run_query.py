
import sys
import os
from pathlib import Path
from pandas import DataFrame

f = open(sys.argv[1], "r")
query = f.read()

from py2neo import Graph
graph = Graph("bolt://localhost:7687")
df = DataFrame(graph.run(query).data())

df.to_csv(Path(sys.argv[1]).with_suffix('.csv'), index=False)


