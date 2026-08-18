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

  function renderList(iterations: ParallelForIteration[] = makeIterations(205)) {
    const onIterationClick = vi.fn();
    render(
      <ParallelForIterationList
        iterations={iterations}
        layers={['root', 'for-loop-1']}
        onIterationClick={onIterationClick}
        onLayersUpdate={vi.fn()}
      />,
    );
    return onIterationClick;
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

  it('searches by the exact displayed unit ID and opens its details', () => {
    const onIterationClick = renderList();

    userEvent.type(screen.getByLabelText('Search items'), 'unit-173');

    expect(screen.getByText('1 of 205 items')).not.toBeNull();
    const row = screen.getByText('unit-173').closest('tr');
    expect(row).not.toBeNull();
    userEvent.click(within(row!).getByRole('button', { name: 'View details for unit-173' }));

    expect(onIterationClick).toHaveBeenCalledWith(
      expect.objectContaining({
        data: expect.objectContaining({ label: 'unit-173', mlmdId: 273 }),
        id: 'task.for-loop-1.173',
      }),
    );
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

  it('sorts items while retaining the iteration as a stable tie-breaker', () => {
    renderList();

    userEvent.click(screen.getByRole('button', { name: 'Item' }));
    userEvent.click(screen.getByRole('button', { name: 'Item' }));

    const rows = screen.getAllByRole('row');
    expect(within(rows[1]).getByText('unit-204')).not.toBeNull();
  });
});
