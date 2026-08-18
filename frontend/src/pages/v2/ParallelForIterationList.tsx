/*
 * Copyright 2026 The Kubeflow Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import {
  ButtonBase,
  InputAdornment,
  MenuItem,
  Paper,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TablePagination,
  TableRow,
  TableSortLabel,
  TextField,
} from '@material-ui/core';
import SearchIcon from '@material-ui/icons/Search';
import * as React from 'react';
import { useEffect, useMemo, useState } from 'react';
import { Node } from 'react-flow-renderer';
import SubDagLayer from 'src/components/graph/SubDagLayer';
import { FlowElementDataBase } from 'src/components/graph/Constants';
import { color } from 'src/Css';
import { ParallelForIteration } from 'src/lib/v2/DynamicFlow';
import { Execution } from 'src/third_party/mlmd';

const DEFAULT_ROWS_PER_PAGE = 50;
const ALL_STATUSES = 'All statuses';
const STATUS_OPTIONS = [
  'Not started',
  'Pending',
  'Running',
  'Complete',
  'Failed',
  'Cached',
  'Canceled',
  'Unknown',
];

type SortField = 'item' | 'iteration' | 'status';
type SortDirection = 'asc' | 'desc';

export interface ParallelForIterationListProps {
  iterations: ParallelForIteration[];
  layers: string[];
  onLayersUpdate: (layers: string[]) => void;
  onIterationOpen: (node: Node<FlowElementDataBase>) => void;
}

export default function ParallelForIterationList({
  iterations,
  layers,
  onLayersUpdate,
  onIterationOpen,
}: ParallelForIterationListProps) {
  const [search, setSearch] = useState('');
  const [statusFilter, setStatusFilter] = useState(ALL_STATUSES);
  const [sortField, setSortField] = useState<SortField>('iteration');
  const [sortDirection, setSortDirection] = useState<SortDirection>('asc');
  const [page, setPage] = useState(0);
  const [rowsPerPage, setRowsPerPage] = useState(DEFAULT_ROWS_PER_PAGE);

  const visibleIterations = useMemo(() => {
    const normalizedSearch = search.trim().toLocaleLowerCase();
    const filteredIterations = iterations.filter(iteration => {
      const itemMatches = iteration.node.data?.label.toLocaleLowerCase().includes(normalizedSearch);
      const statusMatches =
        statusFilter === ALL_STATUSES || getIterationStatus(iteration.node) === statusFilter;
      return itemMatches && statusMatches;
    });

    return filteredIterations.sort((left, right) => {
      let comparison = 0;
      if (sortField === 'iteration') {
        comparison = left.iterationIndex - right.iterationIndex;
      } else if (sortField === 'item') {
        comparison = left.node.data!.label.localeCompare(right.node.data!.label, undefined, {
          numeric: true,
        });
      } else {
        comparison = getIterationStatus(left.node).localeCompare(getIterationStatus(right.node));
      }
      if (comparison === 0) {
        comparison = left.iterationIndex - right.iterationIndex;
      }
      return sortDirection === 'asc' ? comparison : -comparison;
    });
  }, [iterations, search, sortDirection, sortField, statusFilter]);

  useEffect(() => {
    const lastPage = Math.max(0, Math.ceil(visibleIterations.length / rowsPerPage) - 1);
    setPage(currentPage => Math.min(currentPage, lastPage));
  }, [rowsPerPage, visibleIterations.length]);

  const changeSort = (field: SortField) => {
    if (sortField === field) {
      setSortDirection(sortDirection === 'asc' ? 'desc' : 'asc');
      return;
    }
    setSortField(field);
    setSortDirection('asc');
  };

  const pagedIterations = visibleIterations.slice(
    page * rowsPerPage,
    page * rowsPerPage + rowsPerPage,
  );

  return (
    <div
      data-testid='ParallelForIterationList'
      style={{
        display: 'flex',
        flex: '1 1 0',
        flexDirection: 'column',
        minHeight: 0,
        overflow: 'hidden',
      }}
    >
      <SubDagLayer layers={layers} onLayersUpdate={onLayersUpdate} />
      <div
        style={{
          background: color.lightGrey,
          boxSizing: 'border-box',
          display: 'flex',
          flex: '1 1 0',
          minHeight: 0,
          overflow: 'hidden',
          padding: 24,
        }}
      >
        <Paper
          style={{
            display: 'flex',
            flex: '1 1 0',
            flexDirection: 'column',
            minHeight: 0,
            overflow: 'hidden',
          }}
        >
          <div
            style={{
              alignItems: 'center',
              display: 'flex',
              flexWrap: 'wrap',
              gap: 16,
              justifyContent: 'space-between',
              padding: 16,
            }}
          >
            <div style={{ display: 'flex', flexWrap: 'wrap', gap: 16 }}>
              <TextField
                InputProps={{
                  startAdornment: (
                    <InputAdornment position='start'>
                      <SearchIcon />
                    </InputAdornment>
                  ),
                }}
                inputProps={{ 'aria-label': 'Search items' }}
                onChange={event => {
                  setSearch(event.target.value);
                  setPage(0);
                }}
                placeholder='Search unit ID'
                size='small'
                value={search}
                variant='outlined'
              />
              <TextField
                SelectProps={{ inputProps: { 'aria-label': 'Filter by status' } }}
                label='Status'
                onChange={event => {
                  setStatusFilter(event.target.value);
                  setPage(0);
                }}
                select={true}
                size='small'
                value={statusFilter}
                variant='outlined'
              >
                <MenuItem value={ALL_STATUSES}>{ALL_STATUSES}</MenuItem>
                {STATUS_OPTIONS.map(status => (
                  <MenuItem key={status} value={status}>
                    {status}
                  </MenuItem>
                ))}
              </TextField>
            </div>
            <span aria-live='polite'>
              {visibleIterations.length} of {iterations.length} items
            </span>
          </div>
          <TableContainer
            data-testid='ParallelForIterationScrollContainer'
            style={{
              flex: '1 1 0',
              minHeight: 0,
              overflow: 'auto',
              overscrollBehavior: 'contain',
            }}
          >
            <Table aria-label='ParallelFor items' stickyHeader={true}>
              <TableHead>
                <TableRow>
                  <TableCell sortDirection={sortField === 'iteration' ? sortDirection : false}>
                    <TableSortLabel
                      active={sortField === 'iteration'}
                      direction={sortField === 'iteration' ? sortDirection : 'asc'}
                      onClick={() => changeSort('iteration')}
                    >
                      Iteration
                    </TableSortLabel>
                  </TableCell>
                  <TableCell sortDirection={sortField === 'item' ? sortDirection : false}>
                    <TableSortLabel
                      active={sortField === 'item'}
                      direction={sortField === 'item' ? sortDirection : 'asc'}
                      onClick={() => changeSort('item')}
                    >
                      Item
                    </TableSortLabel>
                  </TableCell>
                  <TableCell sortDirection={sortField === 'status' ? sortDirection : false}>
                    <TableSortLabel
                      active={sortField === 'status'}
                      direction={sortField === 'status' ? sortDirection : 'asc'}
                      onClick={() => changeSort('status')}
                    >
                      Status
                    </TableSortLabel>
                  </TableCell>
                </TableRow>
              </TableHead>
              <TableBody>
                {pagedIterations.map(iteration => (
                  <TableRow
                    hover={true}
                    key={iteration.node.id}
                    onClick={() => onIterationOpen(iteration.node)}
                    style={{ cursor: 'pointer' }}
                  >
                    <TableCell>{iteration.iterationIndex}</TableCell>
                    <TableCell>
                      <ButtonBase
                        aria-label={`Open Sub-DAG for ${iteration.node.data!.label}`}
                        focusRipple={true}
                        onClick={event => {
                          event.stopPropagation();
                          onIterationOpen(iteration.node);
                        }}
                        style={{
                          color: color.link,
                          fontWeight: 500,
                          justifyContent: 'flex-start',
                          textAlign: 'left',
                          width: '100%',
                          wordBreak: 'break-word',
                        }}
                      >
                        {iteration.node.data!.label}
                      </ButtonBase>
                    </TableCell>
                    <TableCell>{getIterationStatus(iteration.node)}</TableCell>
                  </TableRow>
                ))}
                {pagedIterations.length === 0 && (
                  <TableRow>
                    <TableCell align='center' colSpan={3}>
                      No items match the current filters.
                    </TableCell>
                  </TableRow>
                )}
              </TableBody>
            </Table>
          </TableContainer>
          <TablePagination
            component='div'
            count={visibleIterations.length}
            onPageChange={(_, nextPage) => setPage(nextPage)}
            onRowsPerPageChange={event => {
              setRowsPerPage(Number(event.target.value));
              setPage(0);
            }}
            page={page}
            rowsPerPage={rowsPerPage}
            rowsPerPageOptions={[20, 50, 100]}
            style={{ flexShrink: 0 }}
          />
        </Paper>
      </div>
    </div>
  );
}

export function getIterationStatus(node: Node<FlowElementDataBase>): string {
  switch (node.data?.state as Execution.State | undefined) {
    case undefined:
      return 'Not started';
    case Execution.State.UNKNOWN:
      return 'Unknown';
    case Execution.State.NEW:
      return 'Pending';
    case Execution.State.RUNNING:
      return 'Running';
    case Execution.State.COMPLETE:
      return 'Complete';
    case Execution.State.FAILED:
      return 'Failed';
    case Execution.State.CACHED:
      return 'Cached';
    case Execution.State.CANCELED:
      return 'Canceled';
    default:
      return 'Unknown';
  }
}
