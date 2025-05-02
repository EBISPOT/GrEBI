import moment from "moment";
import { useEffect, useState } from "react";
import { Link } from "react-router-dom";
import React, { Fragment } from "react";
import { Box, MenuItem, Select, Tab, Tabs, Typography } from "@mui/material";
import { get } from "../../../app/api";
import EkgHeader from "../EkgHeader";
import SearchBox from "../../../components/SearchBox";


export default function EkgHomePage() {

  let [tab, setTab] = useState<string>('exposures');

  let getAdditionalSearchParams = () => {
    let params = new URLSearchParams()
    if(tab === 'exposures') {
      params.append('grebi:type', 'cheminf:DrugBank_identifier')
      params.append('grebi:type', 'biolink:ChemicalEntity')
    } else {
      params.append('grebi:type', 'biolink:Disease')
    }
    return params
  }

  return (
    <div>
      <EkgHeader section="home" />
      <main className="container mx-auto px-4 h-fit">
        <div className="grid grid-cols-2 lg:grid-cols-1 lg:gap-8">
          <div className="lg:col-span-3">
            <div className="bg-gradient-to-r from-neutral-light to-white rounded-lg my-8 p-8">
              <div className="text-3xl mb-4 text-neutral-black font-bold">
                Welcome to ExposomeKG
              </div>
              <Tabs orientation="horizontal" value={tab} onChange={(e, v) => setTab(v)}>
                <Tab label="Search by Exposure" value="exposures" />
                <Tab label="Search by Disease/Phenotype" value="phenotypes" />
              </Tabs>
              <TabPanel value={tab} index="exposures">
                <div className="flex flex-nowrap gap-4 mb-4">
                  <SearchBox subgraph={process.env.REACT_APP_EXPOSOMEKG_SUBGRAPH!} placeholder="Search by exposure..." showExact={false} additionalParams={getAdditionalSearchParams()} />
                </div>
                <div className="grid md:grid-cols-2 grid-cols-1 gap-2">
                  <div className="text-neutral-black">
                    <span>
                      Examples:&nbsp;
                      <Link to={"/search?q=PFAS"} className="link-default">
                        PFAS
                      </Link>
                      &#44;&nbsp;
                      <Link to={"/search?q=metformin"} className="link-default">
                        metformin
                      </Link>
                      &#44;&nbsp;
                      <Link to={"/search?q=fipronil"} className="link-default">
                        fipronil
                      </Link>
                    </span>
                  </div>
                </div>
              </TabPanel>
              <TabPanel value={tab} index="phenotypes">
                <div className="flex flex-nowrap gap-4 mb-4">
                  <SearchBox subgraph={process.env.REACT_APP_EXPOSOMEKG_SUBGRAPH!} placeholder="Search by disease or phenotype..." showExact={false} additionalParams={getAdditionalSearchParams()} />
                </div>
                <div className="grid md:grid-cols-2 grid-cols-1 gap-2">
                  <div className="text-neutral-black">
                    <span>
                      Examples:&nbsp;
                      <Link to={"/search?q=diabetes"} className="link-default">
                        diabetes
                      </Link>
                      &#44;&nbsp;
                      <Link to={"/search?q=hypertension"} className="link-default">
                        hypertension
                      </Link>
                    </span>
                  </div>
                </div>
              </TabPanel>
            </div>
          </div>
          </div>
          <div>
                <p className="mb-3">
                  This website aggregates biological data with potential application to the study of the human exposome in the form of a knowledge graph. It was developed as an exploratory project for the Human Ecosystems Traversal Theme (HETT) at the European Molecular Biology Laboratory (EMBL).
                </p>
                <p className="mb-3">
                  Datasources in the knowledge graph so far include the <a className="link-default" href="http://ctdbase.org" target="_blank">Comparative Toxicogenomics Database (CTD)</a> and the <a className="link-default" href="http://aopwiki.org">Collaborative Adverse Outcome Pathway Wiki (AOP-Wiki)</a>.
                </p>
          </div>
            <div>
              <img src="exposomekg.png" style={{maxWidth: "80%"}}/>
            </div>
      </main>
    </div>
  );
}

interface TabPanelProps {
  children?: React.ReactNode;
  index: string;
  value: string;
}

function TabPanel(props: TabPanelProps) {
  const { children, value, index, ...other } = props;

  return (
    <div
      role="tabpanel"
      hidden={value !== index}
      id={`vertical-tabpanel-${index}`}
      aria-labelledby={`vertical-tab-${index}`}
      {...other}
    >
      {value === index && (
        <Box sx={{ p: 3 }}>
          <Typography>{children}</Typography>
        </Box>
      )}
    </div>
  );
}