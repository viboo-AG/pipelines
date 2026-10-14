import { Workflow, NodeStatus } from 'src/third_party/mlmd/argo_template';

export type IterationStatus =
  | 'Not started'
  | 'Pending'
  | 'Running'
  | 'Complete'
  | 'Failed'
  | 'Skipped'
  | 'Canceled'
  | 'Unknown';

function statusForNode(node: NodeStatus): IterationStatus {
  switch (node.phase) {
    case 'Succeeded':
      return 'Complete';
    case 'Failed':
    case 'Error':
      return 'Failed';
    case 'Running':
      return 'Running';
    case 'Pending':
      return 'Pending';
    case 'Skipped':
    case 'Omitted':
      return 'Skipped';
    default:
      return 'Unknown';
  }
}

/** Resolve the selected KFP DAG through the Argo node tree, then read its direct iterations. */
export function getWorkflowIterationStatuses(
  workflow: Workflow | undefined,
  layers: string[],
  iterationCount: number,
): Map<number, IterationStatus> | undefined {
  const nodes = Object.values(workflow?.status?.nodes || {});
  const root = nodes.find(
    node => node.type === 'DAG' && node.name === `${workflow?.metadata?.name}.root`,
  );
  if (!root) {
    return undefined;
  }

  let current = root;
  for (const layer of layers.slice(1)) {
    const iterationMatch = /^(.+)\.(\d+)$/.exec(layer);
    const candidates = nodes.filter(
      node =>
        node.type === 'DAG' &&
        node.name.startsWith(`${current.name}.`) &&
        (iterationMatch
          ? node.boundaryID === current.id &&
            node.displayName.match(/\((\d+):/)?.[1] === iterationMatch[2]
          : node.displayName === layer),
    );
    // The shortest descendant is the task in this layer, rather than a nested task with the same name.
    candidates.sort((left, right) => left.name.length - right.name.length);
    if (!candidates[0]) {
      return undefined;
    }
    current = candidates[0];
  }

  const statuses = new Map<number, IterationStatus>();
  for (const node of nodes) {
    if (node.type !== 'DAG' || node.boundaryID !== current.id) {
      continue;
    }
    const match = /\((\d+):/.exec(node.displayName);
    if (!match) {
      continue;
    }
    const index = Number(match[1]);
    if (index >= 0 && index < iterationCount) {
      statuses.set(index, statusForNode(node));
    }
  }
  for (let index = 0; index < iterationCount; index++) {
    if (!statuses.has(index)) {
      statuses.set(index, 'Not started');
    }
  }
  return statuses;
}
