// Copyright 2021 The Kubeflow Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

import { Node } from 'react-flow-renderer';
import { Struct } from 'google-protobuf/google/protobuf/struct_pb';
import { FlowElementDataBase } from 'src/components/graph/Constants';
import { PipelineSpec } from 'src/generated/pipeline_spec';
import { Artifact, Event, Execution, Value } from 'src/third_party/mlmd';
import {
  getParallelForIterations,
  getNodeMlmdInfo,
  ITERATION_COUNT_KEY,
  ITERATION_INDEX_KEY,
  PARENT_DAG_ID_KEY,
  TASK_NAME_KEY,
  updateFlowElementsState,
} from './DynamicFlow';
import { convertFlowElements, getTaskKeyFromNodeKey, NodeTypeNames } from './StaticFlow';
import v2YamlTemplateString from 'src/data/test/lightweight_python_functions_v2_pipeline_rev.yaml?raw';
import jsyaml from 'js-yaml';

describe('DynamicFlow', () => {
  function parallelForSpec(items: object): PipelineSpec {
    return PipelineSpec.fromJSON({
      components: {
        'comp-for-loop-1': {
          dag: { tasks: {} },
        },
      },
      pipelineInfo: { name: 'parallel-for-test' },
      root: {
        dag: {
          tasks: {
            'for-loop-1': {
              componentRef: { name: 'comp-for-loop-1' },
              parameterIterator: {
                itemInput: 'unit_id',
                items,
              },
              taskInfo: { name: 'for-loop-1' },
            },
          },
        },
      },
    });
  }

  function execution(
    id: number,
    taskName: string,
    parentDagId?: number,
    iterationIndex?: number,
    state: Execution.State = Execution.State.RUNNING,
  ): Execution {
    const result = new Execution().setId(id).setLastKnownState(state);
    result.getCustomPropertiesMap().set(TASK_NAME_KEY, new Value().setStringValue(taskName));
    if (parentDagId !== undefined) {
      result.getCustomPropertiesMap().set(PARENT_DAG_ID_KEY, new Value().setIntValue(parentDagId));
    }
    if (iterationIndex !== undefined) {
      result
        .getCustomPropertiesMap()
        .set(ITERATION_INDEX_KEY, new Value().setIntValue(iterationIndex));
    }
    return result;
  }

  function setInputs(target: Execution, inputs: object): void {
    target
      .getCustomPropertiesMap()
      .set('inputs', new Value().setStructValue(Struct.fromJavaScript(inputs)));
  }

  describe('getParallelForIterations', () => {
    it('maps a large raw item list to iteration nodes and execution state', () => {
      const unitIds = Array.from({ length: 500 }, (_, index) => `unit-${index}`);
      const spec = parallelForSpec({ raw: JSON.stringify(unitIds) });
      const root = execution(1, '');
      const loop = execution(2, 'for-loop-1', 1);
      loop
        .getCustomPropertiesMap()
        .set(ITERATION_COUNT_KEY, new Value().setIntValue(unitIds.length));
      const iteration = execution(3, 'for-loop-1', 2, 123, Execution.State.COMPLETE);

      const result = getParallelForIterations(
        spec,
        ['root', 'for-loop-1'],
        [root, loop, iteration],
      );

      expect(result).toHaveLength(500);
      expect(result?.[123]).toMatchObject({
        iterationIndex: 123,
        node: {
          data: {
            label: 'unit-123',
            mlmdId: 3,
            state: Execution.State.COMPLETE,
          },
          id: 'task.for-loop-1.123',
          type: NodeTypeNames.SUB_DAG,
        },
      });
      expect(result?.[499].node.data?.label).toBe('unit-499');
    });

    it('reads items from the ParallelFor execution inputs', () => {
      const spec = parallelForSpec({ inputParameter: 'unit_ids' });
      const root = execution(1, '');
      const loop = execution(2, 'for-loop-1', 1);
      loop.getCustomPropertiesMap().set(ITERATION_COUNT_KEY, new Value().setIntValue(3));
      setInputs(loop, { unit_ids: ['alpha', 'beta', 'gamma'] });

      const result = getParallelForIterations(spec, ['root', 'for-loop-1'], [root, loop]);

      expect(result?.map(item => item.node.data?.label)).toEqual(['alpha', 'beta', 'gamma']);
    });

    it('falls back to iteration inputs and then the internal iteration name', () => {
      const spec = parallelForSpec({ inputParameter: 'unit_ids' });
      const root = execution(1, '');
      const loop = execution(2, 'for-loop-1', 1);
      loop.getCustomPropertiesMap().set(ITERATION_COUNT_KEY, new Value().setIntValue(2));
      const firstIteration = execution(3, 'for-loop-1', 2, 0);
      setInputs(firstIteration, { unit_id: 'unit-from-iteration' });

      const result = getParallelForIterations(
        spec,
        ['root', 'for-loop-1'],
        [root, loop, firstIteration],
      );

      expect(result?.map(item => item.node.data?.label)).toEqual([
        'unit-from-iteration',
        'for-loop-1.1',
      ]);
    });

    it('returns undefined outside a ParallelFor layer', () => {
      expect(
        getParallelForIterations(parallelForSpec({ raw: '[]' }), ['root'], []),
      ).toBeUndefined();
    });
  });

  describe('updateFlowElementsState', () => {
    it('update node status based on MLMD', () => {
      // Prepare MLMD objects.
      const EXECUTION_ROOT = new Execution().setId(2).setLastKnownState(Execution.State.COMPLETE);
      EXECUTION_ROOT.getCustomPropertiesMap().set(TASK_NAME_KEY, new Value().setStringValue(''));
      const EXECUTION_PREPROCESS = new Execution()
        .setId(3)
        .setLastKnownState(Execution.State.COMPLETE);
      EXECUTION_PREPROCESS.getCustomPropertiesMap()
        .set(TASK_NAME_KEY, new Value().setStringValue('preprocess'))
        .set(PARENT_DAG_ID_KEY, new Value().setIntValue(2));
      const EXECUTION_TRAIN = new Execution().setId(4).setLastKnownState(Execution.State.FAILED);
      EXECUTION_TRAIN.getCustomPropertiesMap()
        .set(TASK_NAME_KEY, new Value().setStringValue('train'))
        .set(PARENT_DAG_ID_KEY, new Value().setIntValue(2));

      const ARTIFACT_OUTPUT_DATA_ONE = new Artifact().setId(1).setState(Artifact.State.LIVE);
      const ARTIFACT_OUTPUT_DATA_TWO = new Artifact().setId(2).setState(Artifact.State.PENDING);
      const ARTIFACT_MODEL = new Artifact().setId(3).setState(Artifact.State.DELETED);

      const EVENT_PREPROCESS_OUTPUT_DATA_ONE = new Event()
        .setExecutionId(3)
        .setArtifactId(1)
        .setType(Event.Type.OUTPUT)
        .setPath(
          new Event.Path().setStepsList([new Event.Path.Step().setKey('output_dataset_one')]),
        );
      const EVENT_PREPROCESS_OUTPUT_DATA_TWO = new Event()
        .setExecutionId(3)
        .setArtifactId(2)
        .setType(Event.Type.OUTPUT)
        .setPath(
          new Event.Path().setStepsList([new Event.Path.Step().setKey('output_dataset_two_path')]),
        );
      const EVENT_OUTPUT_DATA_ONE_TRAIN = new Event().setExecutionId(4).setArtifactId(1);
      const EVENT_OUTPUT_DATA_TWO_TRAIN = new Event().setExecutionId(4).setArtifactId(2);
      const EVENT_TRAIN_MODEL = new Event()
        .setExecutionId(4)
        .setArtifactId(3)
        .setType(Event.Type.OUTPUT)
        .setPath(new Event.Path().setStepsList([new Event.Path.Step().setKey('model')]));

      // Converts to static graph first, its type is Elements<any>.
      const yamlObject = jsyaml.safeLoad(v2YamlTemplateString);
      const pipelineSpec = PipelineSpec.fromJSON(yamlObject);
      const graph = convertFlowElements(pipelineSpec);

      // MLMD objects to provide node states.
      const executions: Execution[] = [EXECUTION_ROOT, EXECUTION_PREPROCESS, EXECUTION_TRAIN];
      const events: Event[] = [
        EVENT_PREPROCESS_OUTPUT_DATA_ONE,
        EVENT_PREPROCESS_OUTPUT_DATA_TWO,
        EVENT_OUTPUT_DATA_ONE_TRAIN,
        EVENT_OUTPUT_DATA_TWO_TRAIN,
        EVENT_TRAIN_MODEL,
      ];
      const artifacts: Artifact[] = [
        ARTIFACT_OUTPUT_DATA_ONE,
        ARTIFACT_OUTPUT_DATA_TWO,
        ARTIFACT_MODEL,
      ];

      updateFlowElementsState(['root'], graph, executions, events, artifacts);
      for (let element of graph) {
        graph
          .filter(e => e.id === element.id)
          .forEach(e => {
            if (e.id === 'task.preprocess') {
              expect(e.data.state).toEqual(EXECUTION_PREPROCESS.getLastKnownState());
            } else if (e.id === 'task.train') {
              expect(e.data.state).toEqual(EXECUTION_TRAIN.getLastKnownState());
            } else if (e.id === 'artifact.preprocess.output_dataset_one') {
              expect(e.data.state).toEqual(ARTIFACT_OUTPUT_DATA_ONE.getState());
            } else if (e.id === 'artifact.preprocess.output_dataset_two_path') {
              expect(e.data.state).toEqual(ARTIFACT_OUTPUT_DATA_TWO.getState());
            } else if (e.id === 'artifact.train.model') {
              expect(e.data.state).toEqual(ARTIFACT_MODEL.getState());
            }
          });
      }
    });
  });

  describe('getNodeMlmdInfo', () => {
    it('execution not exist', () => {
      const elem: Node<FlowElementDataBase> = {
        id: 'task.exec',
        type: NodeTypeNames.EXECUTION,
        position: { x: 1, y: 2 },
      };

      const nodeMlmdInfo = getNodeMlmdInfo(elem, [], [], []);
      expect(nodeMlmdInfo).toEqual({});
    });

    it('execution found', () => {
      const elem: Node<FlowElementDataBase> = {
        id: 'task.exec',
        data: {
          mlmdId: 1,
        },
        type: NodeTypeNames.EXECUTION,
        position: { x: 1, y: 2 },
      };

      const execution = new Execution();
      execution.setId(1);
      execution.getCustomPropertiesMap().set(TASK_NAME_KEY, new Value().setStringValue('exec'));
      const nodeMlmdInfo = getNodeMlmdInfo(elem, [execution], [], []);
      expect(nodeMlmdInfo).toEqual({ execution });
    });

    it('execution found with custom name', () => {
      const label = 'custom-label';
      const elem: Node<FlowElementDataBase> = {
        id: 'task.exec',
        data: {
          label: label,
          mlmdId: 1,
        },
        type: NodeTypeNames.EXECUTION,
        position: { x: 1, y: 2 },
      };

      const execution = new Execution();
      execution.setId(1);
      execution
        .getCustomPropertiesMap()
        .set(TASK_NAME_KEY, new Value().setStringValue(getTaskKeyFromNodeKey(elem.id)));
      const nodeMlmdInfo = getNodeMlmdInfo(elem, [execution], [], []);
      expect(nodeMlmdInfo).toEqual({ execution });
    });

    it('sub-DAG execution found by MLMD ID when its node ID includes an iteration', () => {
      const elem: Node<FlowElementDataBase> = {
        id: 'task.for-loop-1.42',
        data: {
          label: 'unit-42',
          mlmdId: 7,
        },
        type: NodeTypeNames.SUB_DAG,
        position: { x: 1, y: 2 },
      };
      const matchingExecution = execution(7, 'for-loop-1', 2, 42);

      expect(getNodeMlmdInfo(elem, [matchingExecution], [], [])).toEqual({
        execution: matchingExecution,
      });
    });

    it('artifact not exist', () => {
      const elem: Node<FlowElementDataBase> = {
        id: 'artifact.exec.arti',
        type: NodeTypeNames.ARTIFACT,
        position: { x: 1, y: 2 },
      };

      const nodeMlmdInfo = getNodeMlmdInfo(elem, [], [], []);
      expect(nodeMlmdInfo).toEqual({});
    });

    it('artifact found', () => {
      const elem: Node<FlowElementDataBase> = {
        id: 'artifact.exec.arti',
        type: NodeTypeNames.ARTIFACT,
        position: { x: 1, y: 2 },
      };

      const execution = new Execution();
      execution.setId(1);
      execution.getCustomPropertiesMap().set(TASK_NAME_KEY, new Value().setStringValue('exec'));

      const artifact = new Artifact();
      artifact.setId(2);

      const event = new Event();
      event.setExecutionId(1);
      event.setArtifactId(2);
      event.setType(Event.Type.OUTPUT);
      event.setPath(new Event.Path().setStepsList([new Event.Path.Step().setKey('arti')]));

      const nodeMlmdInfo = getNodeMlmdInfo(elem, [execution], [event], [artifact]);
      expect(nodeMlmdInfo).toEqual({ execution, linkedArtifact: { event, artifact } });
    });
  });
});
