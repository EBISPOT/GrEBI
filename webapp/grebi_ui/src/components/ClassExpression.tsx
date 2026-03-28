import { Fragment } from "react";
import { asArray, randomString } from "../app/util";
import GraphNode from "../model/GraphNode";
import React from "react";
import encodeNodeId from "../encodeNodeId";
import { Link } from "react-router-dom";
import Refs from "../model/Refs";

export default function ClassExpression({
  subgraph,
  refs,
  expr
}: {
  subgraph:string,
  refs:Refs|undefined,
  expr: any;
}) {


  if (typeof expr !== "object") {
    let mapped_value = refs?.get(expr);
    if(mapped_value) {
      return <Link className="link-default" style={{color:'black'}} to={"/graphs/" + subgraph + "/nodes/" + encodeNodeId(expr)}>{mapped_value.getName()}</Link>
    } else {
      return <Link className="link-default" style={{color:'black'}} to={"https://www.ebi.ac.uk/ols4/search?q=" + encodeURIComponent(expr)}>expr</Link>
    }
  }

  const types = asArray(expr['type'])

  if(types && types.indexOf('datatype') !== -1) {
    // rdfs:Datatype
    let equivClass = expr['owl:equivalentClass'];
    if(equivClass) {
      return <Fragment>
        { expr['label'] && <span>{expr['label']} </span> }
        <ClassExpression refs={refs} subgraph={subgraph} expr={equivClass}  />
        </Fragment>
    }
  }


  ///
  /// 1. owl:Class expressions
  ///
  const intersectionOf = asArray(
    expr["owl:intersectionOf"]
  );
  if (intersectionOf.length > 0) {
    let nodes: JSX.Element[] = [
      <span key={randomString()} className="text-neutral-default">
        &#40;
      </span>,
    ];

    for (const subExpr of intersectionOf) {
      if (nodes.length > 1) {
        nodes.push(
          <span
            key={randomString()}
            className="px-1 text-neutral-default italic"
          >
            and
          </span>
        );
      }
      nodes.push(
        <ClassExpression
          refs={refs}
          subgraph={subgraph}
          key={randomString()}
          expr={subExpr}
          
        />
      );
    }

    nodes.push(
      <span key={randomString()} className="text-neutral-default">
        &#41;
      </span>
    );

    return <span>{nodes}</span>;
  }

  const unionOf = asArray(expr["owl:unionOf"]);
  if (unionOf.length > 0) {
    let nodes: JSX.Element[] = [
      <span key={randomString()} className="text-neutral-default">
        &#40;
      </span>,
    ];

    for (const subExpr of unionOf) {
      if (nodes.length > 1) {
        nodes.push(
          <span
            key={randomString()}
            className="px-1 text-neutral-default italic"
          >
            or
          </span>
        );
      }
      nodes.push(
        <ClassExpression
        refs={refs}
        subgraph={subgraph}
          key={randomString()}
          expr={subExpr}
          
        />
      );
    }

    nodes.push(
      <span key={randomString()} className="text-neutral-default">
        &#41;
      </span>
    );

    return <span>{nodes}</span>;
  }

  const complementOf = asArray(
    expr["owl:complementOf"]
  )[0];
  if (complementOf) {
    return (
      <span>
        <span className="pr-1 text-neutral-default italic">not</span>
        <ClassExpression
        refs={refs}
        subgraph={subgraph}
	  expr={complementOf}
	   />
      </span>
    );
  }

  const oneOf = asArray(expr["owl:oneOf"]);
  if (oneOf.length > 0) {
    let nodes: JSX.Element[] = [
      <span key={randomString()} className="text-neutral-default">
        &#123;
      </span>,
    ];

    for (const subExpr of oneOf) {
      if (nodes.length > 1) {
        nodes.push(
          <span key={randomString()} className="text-neutral-default">
            &#44;&nbsp;
          </span>
        );
      }
      nodes.push(
        <ClassExpression
        refs={refs}
        subgraph={subgraph}
          key={randomString()}
          expr={subExpr}
          
        />
      );
    }

    nodes.push(
      <span key={randomString()} className="text-neutral-default">
        &#125;
      </span>
    );

    return <span>{nodes}</span>;
  }

  let inverseOf = expr["owl:inverseOf"];

  if(inverseOf) {
	return (
		<span>
		<span className="px-1 text-embl-purple-default italic">inverse</span>
		<span>
		{"("}
		<ClassExpression refs={refs} subgraph={subgraph} expr={inverseOf}  />
		{")"}
		</span>
		</span>
	);
  }

  ///
  /// 2. owl:Restriction on datatype
  ///
  const onDatatype = expr["owl:onDatatype"];

  if(onDatatype) {

	const withRestrictions = asArray(expr["owl:withRestrictions"]);

	let res:JSX.Element[] = [
		<ClassExpression refs={refs} subgraph={subgraph} expr={onDatatype}  />
	]

	if(withRestrictions.length > 0) {
		res.push(<Fragment>[</Fragment>);
		let isFirst = true;
		for(let restriction of withRestrictions) {
			if(isFirst)
				isFirst = false;
			else
				res.push(<Fragment>, </Fragment>);


			let minExclusive = restriction['http://www.w3.org/2001/XMLSchema#minExclusive'];

			if(minExclusive) {
				res.push(<Fragment>&gt; {minExclusive}</Fragment>);
			}

			let minInclusive = restriction['http://www.w3.org/2001/XMLSchema#minInclusive'];

			if(minInclusive) {
				res.push(<Fragment>≥ {minInclusive}</Fragment>);
			}

			let maxExclusive = restriction['http://www.w3.org/2001/XMLSchema#maxExclusive'];

			if(maxExclusive) {
				res.push(<Fragment>&lt; {maxExclusive}</Fragment>);
			}

			let maxInclusive = restriction['http://www.w3.org/2001/XMLSchema#maxInclusive'];

			if(maxInclusive) {
				res.push(<Fragment>≤ {maxInclusive}</Fragment>);
			}
			
		}
		res.push(<Fragment>]</Fragment>);
	}

	return <span children={res} />
  }



  ///
  /// 3. owl:Restriction on property
  ///
  const onProperty = expr["owl:onProperty"];
  // let onProperties = expr['owl:onProperties'])

  if (!onProperty) {
    return (
      <span className="text-embl-red-default italic">
        unknown class expression {JSON.stringify(expr)}
      </span>
    );
  }

  const someValuesFrom = asArray(
    expr["owl:someValuesFrom"]
  )[0];
  if (someValuesFrom) {
    return (
      <span>
        <ClassExpression refs={refs} subgraph={subgraph}   expr={onProperty} />
        <span className="px-1 text-embl-purple-default italic">some</span>
        <ClassExpression refs={refs} subgraph={subgraph}   expr={someValuesFrom} />
      </span>
    );
  }

  const allValuesFrom = asArray(
    expr["owl:allValuesFrom"]
  )[0];
  if (allValuesFrom) {
    return (
      <span>
        <ClassExpression refs={refs} subgraph={subgraph}   expr={onProperty} />
        <span className="px-1 text-embl-purple-default italic">only</span>
        <ClassExpression refs={refs} subgraph={subgraph}   expr={allValuesFrom} />
      </span>
    );
  }

  const hasValue = asArray(expr["owl:hasValue"])[0];
  if (hasValue) {
    return (
      <span>
        <ClassExpression refs={refs} subgraph={subgraph}    expr={onProperty} />
        <span className="px-1 text-embl-purple-default italic">value</span>
        <ClassExpression refs={refs} subgraph={subgraph}    expr={hasValue} />
      </span>
    );
  }

  const minCardinality = asArray(
    expr["owl:minCardinality"]
  )[0];
  if (minCardinality) {
    return (
      <span>
        <ClassExpression  refs={refs} subgraph={subgraph}    expr={onProperty} />
        <span className="px-1 text-embl-purple-default italic">min</span>
        <ClassExpression  refs={refs} subgraph={subgraph}    expr={minCardinality} />
      </span>
    );
  }

  let maxCardinality = asArray(
    expr["owl:maxCardinality"]
  )[0];
  if (maxCardinality) {
    return (
      <span>
        <ClassExpression refs={refs} subgraph={subgraph}    expr={onProperty} />
        <span className="px-1 text-embl-purple-default italic">max</span>
        <ClassExpression refs={refs} subgraph={subgraph}    expr={maxCardinality} />
      </span>
    );
  }
  let exactCardinality = asArray(
    expr["owl:cardinality"]
  )[0];
  if (exactCardinality) {
    return (
      <span>
        <ClassExpression  refs={refs} subgraph={subgraph}    expr={onProperty} />
        <span className="px-1 text-embl-purple-default italic">exactly</span>
        <ClassExpression  refs={refs} subgraph={subgraph}    expr={exactCardinality} />
      </span>
    );
  }

  let hasSelf = asArray(expr["owl:hasSelf"])[0];
  if (hasSelf) {
    return (
      <span>
        <ClassExpression refs={refs} subgraph={subgraph}     expr={onProperty} />
        <span className="px-1 text-embl-purple-default italic">Self</span>
      </span>
    );
  }


  ///
  /// 4. owl:Restriction qualified cardinalities (property and class)
  ///
  const onClass = expr["owl:onClass"];

  if(onClass) {
    let minQualifiedCardinality = asArray(
      expr["owl:minQualifiedCardinality"]
    )[0];
    if (minQualifiedCardinality) {
      return (
        <span>
          <ClassExpression refs={refs} subgraph={subgraph}    expr={onProperty} />
          <span className="px-1 text-embl-purple-default italic">min</span>
          <ClassExpression refs={refs} subgraph={subgraph}
            expr={minQualifiedCardinality}
          />
          &nbsp;
          <ClassExpression refs={refs} subgraph={subgraph}
            expr={onClass}
          />
        </span>
      );
    }

    let maxQualifiedCardinality = asArray(
      expr["owl:maxQualifiedCardinality"]
    )[0];
    if (maxQualifiedCardinality) {
      return (
        <span>
          <ClassExpression refs={refs} subgraph={subgraph}    expr={onProperty} />
          <span className="px-1 text-embl-purple-default italic">max</span>
          <ClassExpression refs={refs} subgraph={subgraph}
            expr={maxQualifiedCardinality}
            
          />
          &nbsp;
          <ClassExpression refs={refs} subgraph={subgraph}
            expr={onClass}
            
          />
        </span>
      );
    }

    let exactQualifiedCardinality = asArray(
      expr["owl:qualifiedCardinality"]
    )[0];
    if (exactQualifiedCardinality) {
      return (
        <span>
          <ClassExpression refs={refs} subgraph={subgraph}    expr={onProperty} />
          <span className="px-1 text-embl-purple-default italic">exactly</span>
          <ClassExpression refs={refs} subgraph={subgraph}
            expr={exactQualifiedCardinality}
            
          />
          &nbsp;
          <ClassExpression refs={refs} subgraph={subgraph}
            expr={onClass}
            
          />
        </span>
      );
    }
  }


    return (
      <span className="text-embl-red-default italic">
        unknown class expression {JSON.stringify(expr)}
      </span>
    );
}
