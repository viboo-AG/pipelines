import { Workflow } from 'src/third_party/mlmd/argo_template';
import { getWorkflowIterationStatuses } from './WorkflowIterationStatus';

describe('getWorkflowIterationStatuses', () => {
  const workflow = ({
    metadata: { name: 'training-abc' },
    status: {
      nodes: {
        root: { id: 'root', type: 'DAG', name: 'training-abc.root', displayName: 'root' },
        loop: {
          id: 'loop',
          type: 'DAG',
          name: 'training-abc.root.for-loop-1',
          displayName: 'for-loop-1',
          boundaryID: 'root',
        },
        zero: {
          id: 'zero',
          type: 'DAG',
          name: 'training-abc.root.for-loop-1.iteration-iterations(0:0)',
          displayName: 'iteration-iterations(0:0)',
          boundaryID: 'loop',
          phase: 'Succeeded',
        },
        one: {
          id: 'one',
          type: 'DAG',
          name: 'training-abc.root.for-loop-1.iteration-iterations(1:1)',
          displayName: 'iteration-iterations(1:1)',
          boundaryID: 'loop',
          phase: 'Error',
        },
        two: {
          id: 'two',
          type: 'DAG',
          name: 'training-abc.root.for-loop-1.iteration-iterations(2:2)',
          displayName: 'iteration-iterations(2:2)',
          boundaryID: 'loop',
          phase: 'Running',
        },
        retriedPod: {
          id: 'retriedPod',
          type: 'Pod',
          name:
            'training-abc.root.for-loop-1.iteration-iterations(0:0).iteration-item.train-model.executor(0)',
          displayName: 'executor(0)',
          boundaryID: 'zero',
          phase: 'Failed',
        },
      },
    },
  } as unknown) as Workflow;

  it('uses the final iteration DAG phase, including an Argo Error', () => {
    const statuses = getWorkflowIterationStatuses(workflow, ['root', 'for-loop-1'], 4);
    expect([...statuses!]).toEqual([
      [0, 'Complete'],
      [1, 'Failed'],
      [2, 'Running'],
      [3, 'Not started'],
    ]);
  });

  it('does not borrow statuses from another loop or an unrelated DAG', () => {
    expect(getWorkflowIterationStatuses(workflow, ['root', 'missing-loop'], 3)).toBeUndefined();
  });

  it('keeps skipped iterations distinct from canceled and failed work', () => {
    workflow.status.nodes.one.phase = 'Skipped';
    expect(getWorkflowIterationStatuses(workflow, ['root', 'for-loop-1'], 2)?.get(1)).toBe(
      'Skipped',
    );
  });
});
