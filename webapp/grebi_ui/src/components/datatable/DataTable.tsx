import { ArrowDownward, ArrowUpward, KeyboardArrowDown, SwapVert } from "@mui/icons-material";
import { Pagination } from "../Pagination";
import React, { Fragment, useEffect, useState } from "react";

export interface Column {
  id: string;
  name: React.ReactNode;
  minWidth?: number;
  align?: "right";
  selector: (row: any, key:string) => any;
  sortable?: boolean;
}

export default function DataTable({
  columns,
  defaultSelector,
  addColumnsFromData,
  hideColumns,
  data,
  dataCount,
  placeholder,
  onSelectRow,
  page,
  rowsPerPage,
  onPageChange,
  onRowsPerPageChange,
  onFilter,
  sortColumn,
  setSortColumn,
  sortDir,
  setSortDir,
  maxRowHeight
}: {
  columns?: readonly Column[]|undefined;
  defaultSelector:undefined|((row:any, key:string)=>any);
  data: any[];
  addColumnsFromData?:boolean,
  hideColumns?: string[]|undefined,
  dataCount?: number;
  placeholder?: string;
  onSelectRow?: (row: any) => void;
  page?: number;
  rowsPerPage?: number;
  onPageChange?: (page: number) => void;
  onRowsPerPageChange?: (rowsPerPage: number) => void;
  onFilter?: (key: string) => void;
  sortColumn?: string,
  setSortColumn?: (sortColumn: string) => void,
  sortDir?: 'asc'|'desc',
  setSortDir?: (sortDir: 'asc'|'desc') => void,
  maxRowHeight?:string|undefined
}) {

  let [autoAddedColumns, setAutoAddedColumns] = useState<Column[]>([])

  useEffect(() => {

    if(addColumnsFromData) {

      if(!defaultSelector) {
        throw new Error("need a defaultSelector to automatically add columns from data")
      }

      let newCols:Column[] = []

      for(let row of data) {
        for(let key in row) {
          if(hideColumns && hideColumns.includes(key)) {
            continue
          }
          if(columns && columns.find((c) => c.id === key)) {
            continue
          }
          if(newCols.find((c) => c.id === key)) {
            continue
          }
          newCols.push({
            id: key,
            name: key,
            selector: defaultSelector,
            sortable: true
          })
        }
      }

      if(JSON.stringify(newCols) !== JSON.stringify(autoAddedColumns)) {
        setAutoAddedColumns(newCols)
      }
    }

  }, [data, addColumnsFromData]);


  const allColumns: Column[] = [...(columns || []), ...autoAddedColumns]

  return (
    <div>
      <div className="grid grid-cols-2">
        {rowsPerPage !== undefined &&
        rowsPerPage > 0 &&
        onRowsPerPageChange !== undefined ? (
          <div className="justify-self-start px-2 mb-2">
            <div className="flex group relative text-md">
              <label className="self-center px-3">Show</label>
              <select
                className="input-default appearance-none pr-7 z-20 bg-transparent"
                onChange={(e) => {
                  onRowsPerPageChange(parseInt(e.target.value));
                }}
              >
                <option value={10}>10</option>
                <option value={25}>25</option>
                <option value={100}>100</option>
              </select>
              <div className="absolute right-2 top-2 z-10 text-neutral-default group-focus:text-neutral-dark group-hover:text-neutral-dark">
                <KeyboardArrowDown fontSize="medium" />
              </div>
            </div>
          </div>
        ) : null}
        {onFilter !== undefined ? (
          <div className="justify-self-end group relative w-3/4 px-2 mb-2">
            <input
              type="text"
              placeholder={placeholder ? placeholder : "Search..."}
              className="input-default text-md pl-10"
              onChange={(e) => {
                onFilter(e.target.value);
              }}
            />
            <div className="absolute left-5 top-2 z-10">
              <i className="icon icon-common icon-search text-xl text-neutral-default group-focus:text-neutral-dark group-hover:text-neutral-dark" />
            </div>
          </div>
        ) : null}
      </div>
      <div className="mx-2 overflow-x-auto">
        <table className="table-auto border-collapse border-spacing-1 w-full mb-2">
          <thead>
            <tr className="border-b-2 border-grey-default">
              {allColumns.map((column) => (
                <td
                  className="text-lg font-bold py-2 px-4"
                  key={column.id}
                >
                  <div className="flex justify-between">
                  <div>
                  {column.name}
                  </div>
                  <div>
                  {column.sortable && setSortDir && setSortColumn && sortColumn !== column.id &&
                    <SwapVert color={'disabled'} className="cursor-pointer" onClick={() => {
                        setSortColumn(column.id)
                        setSortDir('asc')
                      }}
                    />
                  }
                  {column.sortable && setSortDir && setSortColumn && sortColumn === column.id && sortDir === 'asc' &&
                      <ArrowDownward color={'primary'} className="cursor-pointer" onClick={() => { setSortDir('desc') }} />
                  }
                  {column.sortable && setSortDir && setSortColumn && sortColumn === column.id && sortDir === 'desc' &&
                      <ArrowUpward color={'primary'} className="cursor-pointer" onClick={() => { setSortDir('asc') }} />
                  }
                  </div>
                  </div>
                </td>
              ))}
            </tr>
          </thead>
          <tbody>
            {data.map((row: any, rowIndex: number) => {
              return (
                <tr
                  tabIndex={-1}
                  key={rowIndex}
                  onClick={() => {
                    if (onSelectRow) onSelectRow(row);
                  }}
                  className={`even:bg-grey-50 group hover:bg-neutral-50 ${
                    onSelectRow ? "cursor-pointer grebi-row-highlight" : ""
                  }`}
                >
                  {allColumns.map((column: any) => {
                    const cell = column.selector(row, column.id);
                    return (
                      <td
                        className="text-md align-top py-2 px-4"
                        key={column.id}
                      >
                        <div className={column.className||""} style={{ ...( maxRowHeight ? {maxHeight: maxRowHeight, overflowY:"auto"} : {}) }}>
                        {cell === undefined || cell === null || cell === ""
                          ? "(no data)"
                          : cell}
                        </div>
                      </td>
                    );
                  })}
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
      {page !== undefined &&
      page >= 0 &&
      onPageChange !== undefined &&
      rowsPerPage !== undefined &&
      rowsPerPage > 0 ? (
        <Pagination
          page={page}
          onPageChange={onPageChange}
          rowsPerPage={rowsPerPage}
          dataCount={dataCount || 0}
        ></Pagination>
      ) : null}
    </div>
  );
}
