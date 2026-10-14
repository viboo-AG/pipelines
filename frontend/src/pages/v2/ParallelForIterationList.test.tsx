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

import { render, screen, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import * as React from 'react';
import { ParallelForIteration } from 'src/lib/v2/DynamicFlow';
import { NodeTypeNames, TaskType } from 'src/lib/v2/StaticFlow';
import { testBestPractices } from 'src/TestUtils';
import { Execution } from 'src/third_party/mlmd';
import ParallelForIterationList from './ParallelForIterationList';

testBestPractices();

describe('ParallelForIterationList', () => {
  function makeIterations(count: number): ParallelForIteration[] {
    return Array.from({ length: count }, (_, iterationIndex) => ({
      iterationIndex,
      node: {
        id: `task.for-loop-1.${iterationIndex}`,
        data: {
          label: `unit-${iterationIndex}`,
          mlmdId: iterationIndex + 100,
          state: iterationIndex % 3 === 0 ? Execution.State.FAILED : Execution.State.COMPLETE,
          taskType: TaskType.DAG,
        },
        position: { x: 0, y: 0 },
        type: NodeTypeNames.SUB_DAG,
      },
    }));
  }

  function renderList(
    iterations: ParallelForIteration[] = makeIterations(205),
    statusByIteration?: Map<number, 'Complete' | 'Failed' | 'Running'>,
  ) {
    const onIterationOpen = vi.fn();
    render(
      <ParallelForIterationList
        iterations={iterations}
        layers={['root', 'for-loop-1']}
        onIterationOpen={onIterationOpen}
        onLayersUpdate={vi.fn()}
        statusByIteration={statusByIteration}
      />,
    );
    return onIterationOpen;
  }

  it('paginates large loops at 50 items per page', () => {
    renderList();

    expect(screen.getByText('205 of 205 items')).not.toBeNull();
    expect(screen.getByText('unit-49')).not.toBeNull();
    expect(screen.queryByText('unit-50')).toBeNull();
    expect(screen.getAllByRole('row')).toHaveLength(51);

    userEvent.click(screen.getByLabelText('Next page'));

    expect(screen.getByText('unit-50')).not.toBeNull();
    expect(screen.queryByText('unit-49')).toBeNull();
  });

  it('searches by the exact displayed unit ID and opens its Sub-DAG', () => {
    const onIterationOpen = renderList();

    userEvent.type(screen.getByLabelText('Search items'), 'unit-173');

    expect(screen.getByText('1 of 205 items')).not.toBeNull();
    const row = screen.getByText('unit-173').closest('tr');
    expect(row).not.toBeNull();
    userEvent.click(within(row!).getByRole('button', { name: 'Open Sub-DAG for unit-173' }));

    expect(onIterationOpen).toHaveBeenCalledWith(
      expect.objectContaining({
        data: expect.objectContaining({ label: 'unit-173', mlmdId: 273 }),
        id: 'task.for-loop-1.173',
      }),
    );
  });

  it('opens a Sub-DAG when the row is clicked', () => {
    const onIterationOpen = renderList(makeIterations(2));

    userEvent.click(screen.getByText('unit-1').closest('tr')!);

    expect(onIterationOpen).toHaveBeenCalledWith(
      expect.objectContaining({ id: 'task.for-loop-1.1' }),
    );
  });

  it('uses one explicit scrolling container for the table rows', () => {
    renderList();

    const list = screen.getByTestId('ParallelForIterationList');
    const scrollContainer = screen.getByTestId('ParallelForIterationScrollContainer');
    expect(list.style.overflow).toBe('hidden');
    expect(list.style.minHeight).toBe('0');
    expect(scrollContainer.style.overflow).toBe('auto');
    expect(scrollContainer.style.minHeight).toBe('0');
  });

  it('filters iterations by runtime status', () => {
    renderList(makeIterations(12));

    userEvent.click(screen.getByLabelText('Filter by status'));
    userEvent.click(screen.getByRole('option', { name: 'Failed' }));

    expect(screen.getByText('4 of 12 items')).not.toBeNull();
    expect(screen.getByText('unit-0')).not.toBeNull();
    expect(screen.getByText('unit-9')).not.toBeNull();
    expect(screen.queryByText('unit-1')).toBeNull();
  });

  it('shows counts for the four summary statuses and filters through each count', () => {
    const iterations = makeIterations(5);
    iterations[0].node.data!.state = undefined;
    iterations[1].node.data!.state = Execution.State.RUNNING;
    iterations[2].node.data!.state = Execution.State.COMPLETE;
    iterations[3].node.data!.state = Execution.State.FAILED;
    iterations[4].node.data!.state = Execution.State.NEW;
    renderList(iterations);

    expect(screen.getByRole('button', { name: 'Not started: 1' })).not.toBeNull();
    expect(screen.getByRole('button', { name: 'Running: 1' })).not.toBeNull();
    expect(screen.getByRole('button', { name: 'Complete: 1' })).not.toBeNull();
    expect(screen.getByRole('button', { name: 'Failed: 1' })).not.toBeNull();
    expect(screen.queryByRole('button', { name: /Pending:/ })).toBeNull();

    userEvent.click(screen.getByRole('button', { name: 'Running: 1' }));
    expect(screen.getByText('1 of 5 items')).not.toBeNull();
    expect(screen.getByText('unit-1')).not.toBeNull();
    expect(screen.queryByText('unit-2')).toBeNull();
    expect(screen.getByLabelText('Filter by status')).toHaveTextContent('Running');
    expect(screen.getByRole('button', { name: 'Running: 1' })).toHaveAttribute(
      'aria-pressed',
      'true',
    );

    userEvent.click(screen.getByRole('button', { name: 'Not started: 1' }));
    expect(screen.getByText('unit-0')).not.toBeNull();
    expect(screen.getByLabelText('Filter by status')).toHaveTextContent('Not started');

    userEvent.click(screen.getByRole('button', { name: 'Complete: 1' }));
    expect(screen.getByText('unit-2')).not.toBeNull();

    userEvent.click(screen.getByRole('button', { name: 'Failed: 1' }));
    expect(screen.getByText('unit-3')).not.toBeNull();
  });

  it('keeps counts across filters and allows selecting an empty status', () => {
    renderList(makeIterations(2));

    userEvent.click(screen.getByRole('button', { name: 'Running: 0' }));
    expect(screen.getByText('0 of 2 items')).not.toBeNull();
    expect(screen.getByText('No items match the current filters.')).not.toBeNull();
    expect(screen.getByLabelText('Filter by status')).toHaveTextContent('Running');
    expect(screen.getByRole('button', { name: 'Complete: 1' })).not.toBeNull();
    expect(screen.getByRole('button', { name: 'Failed: 1' })).not.toBeNull();
  });

  it('uses Workflow phases for rows, counts, and filters when MLMD disagrees', () => {
    const iterations = makeIterations(2);
    iterations[0].node.data!.state = Execution.State.COMPLETE;
    iterations[1].node.data!.state = Execution.State.COMPLETE;
    renderList(
      iterations,
      new Map([
        [0, 'Failed'],
        [1, 'Running'],
      ]),
    );

    expect(screen.getByRole('button', { name: 'Complete: 0' })).not.toBeNull();
    expect(screen.getByRole('button', { name: 'Failed: 1' })).not.toBeNull();
    userEvent.click(screen.getByRole('button', { name: 'Failed: 1' }));
    expect(screen.getByText('unit-0')).not.toBeNull();
    expect(screen.queryByText('unit-1')).toBeNull();
    expect(screen.getByText('Failed', { selector: 'td' })).not.toBeNull();
  });

  it('sorts items while retaining the iteration as a stable tie-breaker', () => {
    renderList();

    userEvent.click(screen.getByRole('button', { name: 'Item' }));
    userEvent.click(screen.getByRole('button', { name: 'Item' }));

    const rows = screen.getAllByRole('row');
    expect(within(rows[1]).getByText('unit-204')).not.toBeNull();
  });
});
