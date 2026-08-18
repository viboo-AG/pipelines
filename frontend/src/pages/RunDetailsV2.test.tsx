/*
 * Copyright 2021 The Kubeflow Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { act, queryByText, render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { Struct } from 'google-protobuf/google/protobuf/struct_pb';

import * as React from 'react';
import { V2beta1Run, V2beta1RuntimeState } from 'src/apisv2beta1/run';
import { V2beta1Experiment, V2beta1ExperimentStorageState } from 'src/apisv2beta1/experiment';
import { RoutePage, RouteParams } from 'src/components/Router';
import { Apis } from 'src/lib/Apis';
import { Api } from 'src/mlmd/Api';
import { KFP_V2_RUN_CONTEXT_TYPE } from 'src/mlmd/MlmdUtils';
import { expectErrors, mockResizeObserver, testBestPractices } from 'src/TestUtils';
import { CommonTestWrapper } from 'src/TestWrapper';
import {
  Context,
  Execution,
  GetContextByTypeAndNameRequest,
  GetContextByTypeAndNameResponse,
  GetExecutionsByContextResponse,
  Value,
} from 'src/third_party/mlmd';
import * as metadataStoreServicePb from 'src/third_party/mlmd/generated/ml_metadata/proto/metadata_store_service_pb';
import { PageProps } from './Page';
import { RunDetailsInternalProps } from './RunDetails';
import { RunDetailsV2 } from './RunDetailsV2';
import v2YamlTemplateString from 'src/data/test/lightweight_python_functions_v2_pipeline_rev.yaml?raw';
import {
  ITERATION_COUNT_KEY,
  ITERATION_INDEX_KEY,
  PARENT_DAG_ID_KEY,
  TASK_NAME_KEY,
} from 'src/lib/v2/DynamicFlow';

vi.mock('src/components/Editor', () => ({
  default: ({ value }: { value?: string }) => <pre data-testid='Editor'>{value}</pre>,
}));

testBestPractices();
describe('RunDetailsV2', () => {
  const RUN_ID = '1';

  let updateBannerSpy: any;
  let updateDialogSpy: any;
  let updateSnackbarSpy: any;
  let updateToolbarSpy: any;
  let historyPushSpy: any;

  function generateProps(): RunDetailsInternalProps & PageProps {
    const pageProps: PageProps = {
      history: { push: historyPushSpy } as any,
      location: '' as any,
      match: {
        params: {
          [RouteParams.runId]: RUN_ID,
        },
        isExact: true,
        path: '',
        url: '',
      },
      toolbarProps: { actions: {}, breadcrumbs: [], pageTitle: '' },
      updateBanner: updateBannerSpy,
      updateDialog: updateDialogSpy,
      updateSnackbar: updateSnackbarSpy,
      updateToolbar: updateToolbarSpy,
    };
    return Object.assign(pageProps, {
      gkeMetadata: {},
    });
  }
  const TEST_RUN: V2beta1Run = {
    created_at: new Date(2018, 8, 5, 4, 3, 2),
    scheduled_at: new Date(2018, 8, 6, 4, 3, 2),
    finished_at: new Date(2018, 8, 7, 4, 3, 2),
    description: 'test run description',
    experiment_id: 'some-experiment-id',
    run_id: 'test-run-id',
    display_name: 'test run',
    pipeline_spec: {
      pipeline_id: 'some-pipeline-id',
      pipeline_manifest: '{some-template-string}',
    },
    runtime_config: { parameters: { param1: 'value1' } },
    state: V2beta1RuntimeState.SUCCEEDED,
  };
  const TEST_EXPERIMENT: V2beta1Experiment = {
    created_at: '2021-01-24T18:03:08Z',
    description: 'All runs will be grouped here.',
    experiment_id: 'some-experiment-id',
    display_name: 'Default',
    storage_state: V2beta1ExperimentStorageState.AVAILABLE,
  };
  beforeEach(() => {
    mockResizeObserver();

    updateBannerSpy = vi.fn();
    updateToolbarSpy = vi.fn();

    const contextResponse = new GetContextByTypeAndNameResponse();
    contextResponse.setContext(new Context());
    vi.spyOn(Api.getInstance().metadataStoreService, 'getContextByTypeAndName').mockResolvedValue(
      contextResponse,
    );
    vi.spyOn(Api.getInstance().metadataStoreService, 'getExecutionsByContext').mockResolvedValue(
      new GetExecutionsByContextResponse(),
    );
    vi.spyOn(Api.getInstance().metadataStoreService, 'getArtifactsByContext').mockResolvedValue(
      new metadataStoreServicePb.GetArtifactsByContextResponse(),
    );
    vi.spyOn(Api.getInstance().metadataStoreService, 'getEventsByExecutionIDs').mockResolvedValue(
      new metadataStoreServicePb.GetEventsByExecutionIDsResponse(),
    );
    vi.spyOn(Api.getInstance().metadataStoreService, 'getArtifactTypes').mockResolvedValue(
      new metadataStoreServicePb.GetArtifactTypesResponse(),
    );
    vi.spyOn(Api.getInstance().metadataStoreService, 'getArtifactsByID').mockResolvedValue(
      new metadataStoreServicePb.GetArtifactsByIDResponse(),
    );
    vi.spyOn(Apis.experimentServiceApiV2, 'getExperiment').mockResolvedValue({});
  });

  it('Render detail page with reactflow', async () => {
    render(
      <CommonTestWrapper>
        <RunDetailsV2
          pipeline_job={v2YamlTemplateString}
          run={TEST_RUN}
          {...generateProps()}
        ></RunDetailsV2>
      </CommonTestWrapper>,
    );
    expect(screen.getByTestId('DagCanvas')).not.toBeNull();
  });

  it('replaces an expanded ParallelFor graph with a searchable item list', async () => {
    const unitIds = Array.from({ length: 205 }, (_, index) => `unit-${index}`);
    const pipelineWithLoop = `
pipelineInfo:
  name: parallel-for-test
schemaVersion: 2.1.0
sdkVersion: kfp-2.16.0
deploymentSpec: {}
components:
  comp-for-loop-1:
    dag:
      tasks: {}
root:
  dag:
    tasks:
      for-loop-1:
        componentRef:
          name: comp-for-loop-1
        parameterIterator:
          itemInput: unit_id
          items:
            inputParameter: unit_ids
        taskInfo:
          name: for-loop-1
`;
    const root = new Execution().setId(1).setLastKnownState(Execution.State.RUNNING);
    root.getCustomPropertiesMap().set(TASK_NAME_KEY, new Value().setStringValue(''));
    const loop = new Execution().setId(2).setLastKnownState(Execution.State.RUNNING);
    loop
      .getCustomPropertiesMap()
      .set(TASK_NAME_KEY, new Value().setStringValue('for-loop-1'))
      .set(PARENT_DAG_ID_KEY, new Value().setIntValue(1))
      .set(ITERATION_COUNT_KEY, new Value().setIntValue(unitIds.length))
      .set('inputs', new Value().setStructValue(Struct.fromJavaScript({ unit_ids: unitIds })));
    const iteration = new Execution().setId(3).setLastKnownState(Execution.State.COMPLETE);
    iteration
      .getCustomPropertiesMap()
      .set(TASK_NAME_KEY, new Value().setStringValue('for-loop-1'))
      .set(PARENT_DAG_ID_KEY, new Value().setIntValue(2))
      .set(ITERATION_INDEX_KEY, new Value().setIntValue(173));
    const executionsResponse = new GetExecutionsByContextResponse();
    executionsResponse.setExecutionsList([root, loop, iteration]);
    vi.spyOn(Api.getInstance().metadataStoreService, 'getExecutionsByContext').mockResolvedValue(
      executionsResponse,
    );
    vi.spyOn(Apis.experimentServiceApiV2, 'getExperiment').mockResolvedValue(TEST_EXPERIMENT);

    render(
      <CommonTestWrapper>
        <RunDetailsV2
          pipeline_job={pipelineWithLoop}
          run={TEST_RUN}
          {...generateProps()}
        ></RunDetailsV2>
      </CommonTestWrapper>,
    );

    const loopNode = await screen.findByTitle('for-loop-1');
    const icons = loopNode.querySelectorAll('svg');
    const expandButton = icons[icons.length - 1].parentElement?.parentElement;
    expect(expandButton).not.toBeNull();
    userEvent.click(expandButton!);

    await waitFor(() => expect(screen.getByTestId('ParallelForIterationList')).not.toBeNull());
    expect(screen.getByText('205 of 205 items')).not.toBeNull();
    userEvent.type(screen.getByLabelText('Search items'), 'unit-173');
    userEvent.click(screen.getByRole('button', { name: 'Open Sub-DAG for unit-173' }));

    await waitFor(() => expect(screen.getByTestId('DagCanvas')).not.toBeNull());
    expect(screen.queryByTestId('ParallelForIterationList')).toBeNull();
    expect(screen.getByText('for-loop-1.173')).not.toBeNull();
  });

  it('Shows error banner when disconnected from MLMD', async () => {
    const assertErrors = expectErrors();
    vi.spyOn(Api.getInstance().metadataStoreService, 'getContextByTypeAndName').mockRejectedValue(
      new Error('Not connected to MLMD'),
    );

    render(
      <CommonTestWrapper>
        <RunDetailsV2
          pipeline_job={v2YamlTemplateString}
          run={TEST_RUN}
          {...generateProps()}
        ></RunDetailsV2>
      </CommonTestWrapper>,
    );

    await waitFor(() =>
      expect(updateBannerSpy).toHaveBeenLastCalledWith(
        expect.objectContaining({
          additionalInfo:
            'Cannot find context with {"typeName":"system.PipelineRun","contextName":"1"}: Not connected to MLMD',
          message: 'Cannot get MLMD objects from Metadata store.',
          mode: 'error',
        }),
      ),
    );
    assertErrors();
  });

  it('Shows no banner when connected from MLMD', async () => {
    vi.spyOn(Api.getInstance().metadataStoreService, 'getContextByTypeAndName').mockImplementation(
      (request: GetContextByTypeAndNameRequest) => {
        const response = new GetContextByTypeAndNameResponse();
        if (
          request.getTypeName() === KFP_V2_RUN_CONTEXT_TYPE &&
          request.getContextName() === RUN_ID
        ) {
          response.setContext(new Context());
        }
        return response;
      },
    );
    vi.spyOn(Api.getInstance().metadataStoreService, 'getExecutionsByContext').mockResolvedValue(
      new GetExecutionsByContextResponse(),
    );
    vi.spyOn(Api.getInstance().metadataStoreService, 'getArtifactsByContext').mockResolvedValue(
      new metadataStoreServicePb.GetArtifactsByContextResponse(),
    );
    vi.spyOn(Api.getInstance().metadataStoreService, 'getEventsByExecutionIDs').mockResolvedValue(
      new metadataStoreServicePb.GetEventsByExecutionIDsResponse(),
    );

    render(
      <CommonTestWrapper>
        <RunDetailsV2
          pipeline_job={v2YamlTemplateString}
          run={TEST_RUN}
          {...generateProps()}
        ></RunDetailsV2>
      </CommonTestWrapper>,
    );

    await waitFor(() => expect(updateBannerSpy).toHaveBeenLastCalledWith({}));
  });

  it("shows run title and experiments' links", async () => {
    const getRunSpy = vi.spyOn(Apis.runServiceApiV2, 'getRun');
    getRunSpy.mockResolvedValue(TEST_RUN);
    const getExperimentSpy = vi.spyOn(Apis.experimentServiceApiV2, 'getExperiment');
    getExperimentSpy.mockResolvedValue(TEST_EXPERIMENT);

    vi.spyOn(Api.getInstance().metadataStoreService, 'getContextByTypeAndName').mockImplementation(
      (request: GetContextByTypeAndNameRequest) => {
        const response = new GetContextByTypeAndNameResponse();
        response.setContext(new Context());
        return response;
      },
    );
    vi.spyOn(Api.getInstance().metadataStoreService, 'getExecutionsByContext').mockResolvedValue(
      new GetExecutionsByContextResponse(),
    );
    vi.spyOn(Api.getInstance().metadataStoreService, 'getArtifactsByContext').mockResolvedValue(
      new metadataStoreServicePb.GetArtifactsByContextResponse(),
    );
    vi.spyOn(Api.getInstance().metadataStoreService, 'getEventsByExecutionIDs').mockResolvedValue(
      new metadataStoreServicePb.GetEventsByExecutionIDsResponse(),
    );

    await act(async () => {
      render(
        <CommonTestWrapper>
          <RunDetailsV2
            pipeline_job={v2YamlTemplateString}
            run={TEST_RUN}
            {...generateProps()}
          ></RunDetailsV2>
        </CommonTestWrapper>,
      );
    });

    await waitFor(() =>
      expect(updateToolbarSpy).toHaveBeenCalledWith(
        expect.objectContaining({
          pageTitleTooltip: 'test run',
        }),
      ),
    );
    await waitFor(() =>
      expect(updateToolbarSpy).toHaveBeenCalledWith(
        expect.objectContaining({
          breadcrumbs: [
            { displayName: 'Experiments', href: RoutePage.EXPERIMENTS },
            {
              displayName: 'Default',
              href: `/experiments/details/some-experiment-id`,
            },
          ],
        }),
      ),
    );
  });

  it('shows top bar buttons', async () => {
    const getRunSpy = vi.spyOn(Apis.runServiceApiV2, 'getRun');
    getRunSpy.mockResolvedValue(TEST_RUN);
    const getExperimentSpy = vi.spyOn(Apis.experimentServiceApiV2, 'getExperiment');
    getExperimentSpy.mockResolvedValue(TEST_EXPERIMENT);

    vi.spyOn(Api.getInstance().metadataStoreService, 'getContextByTypeAndName').mockImplementation(
      () => {
        const response = new GetContextByTypeAndNameResponse();
        response.setContext(new Context());
        return response;
      },
    );
    vi.spyOn(Api.getInstance().metadataStoreService, 'getExecutionsByContext').mockResolvedValue(
      new GetExecutionsByContextResponse(),
    );
    vi.spyOn(Api.getInstance().metadataStoreService, 'getArtifactsByContext').mockResolvedValue(
      new metadataStoreServicePb.GetArtifactsByContextResponse(),
    );
    vi.spyOn(Api.getInstance().metadataStoreService, 'getEventsByExecutionIDs').mockResolvedValue(
      new metadataStoreServicePb.GetEventsByExecutionIDsResponse(),
    );

    await act(async () => {
      render(
        <CommonTestWrapper>
          <RunDetailsV2
            pipeline_job={v2YamlTemplateString}
            run={TEST_RUN}
            {...generateProps()}
          ></RunDetailsV2>
        </CommonTestWrapper>,
      );
    });

    await waitFor(() =>
      expect(updateToolbarSpy).toHaveBeenCalledWith(
        expect.objectContaining({
          actions: expect.objectContaining({
            archive: expect.objectContaining({ disabled: false, title: 'Archive' }),
            retry: expect.objectContaining({ disabled: true, title: 'Retry' }),
            terminateRun: expect.objectContaining({ disabled: true, title: 'Terminate' }),
            cloneRun: expect.objectContaining({ disabled: false, title: 'Clone run' }),
          }),
        }),
      ),
    );
  });

  describe('topbar tabs', () => {
    it('switches to Detail tab', async () => {
      render(
        <CommonTestWrapper>
          <RunDetailsV2
            pipeline_job={v2YamlTemplateString}
            run={TEST_RUN}
            {...generateProps()}
          ></RunDetailsV2>
        </CommonTestWrapper>,
      );

      userEvent.click(screen.getByText('Detail'));

      screen.getByText('Run details');
      screen.getByText('Run ID');
      screen.getByText('Workflow name');
      screen.getByText('Status');
      screen.getByText('Description');
      screen.getByText('Created at');
      screen.getByText('Started at');
      screen.getByText('Finished at');
      screen.getByText('Duration');
    });

    it('shows content in Detail tab', async () => {
      render(
        <CommonTestWrapper>
          <RunDetailsV2
            pipeline_job={v2YamlTemplateString}
            run={TEST_RUN}
            {...generateProps()}
          ></RunDetailsV2>
        </CommonTestWrapper>,
      );

      userEvent.click(screen.getByText('Detail'));

      screen.getByText('test-run-id'); // 'Run ID'
      screen.getByText('test run'); // 'Workflow name'
      screen.getByText('test run description'); // 'Description'
      screen.getByText('9/5/2018, 4:03:02 AM'); //'Created at'
      screen.getByText('9/6/2018, 4:03:02 AM'); // 'Started at'
      screen.getByText('9/7/2018, 4:03:02 AM'); // 'Finished at'
      screen.getByText('48:00:00'); // 'Duration'
    });

    it('handles no creation time', async () => {
      const noCreateTimeRun: V2beta1Run = {
        // created_at: new Date(2018, 8, 5, 4, 3, 2),
        scheduled_at: new Date(2018, 8, 6, 4, 3, 2),
        finished_at: new Date(2018, 8, 7, 4, 3, 2),
        experiment_id: 'some-experiment-id',
        run_id: 'test-run-id',
        display_name: 'test run',
        description: 'test run description',
        state: V2beta1RuntimeState.SUCCEEDED,
      };
      render(
        <CommonTestWrapper>
          <RunDetailsV2
            pipeline_job={v2YamlTemplateString}
            run={noCreateTimeRun}
            {...generateProps()}
          ></RunDetailsV2>
        </CommonTestWrapper>,
      );

      userEvent.click(screen.getByText('Detail'));

      expect(screen.getAllByText('-').length).toEqual(2); // create time and duration are empty.
    });

    it('handles no finish time', async () => {
      const noFinsihTimeRun: V2beta1Run = {
        created_at: new Date(2018, 8, 5, 4, 3, 2),
        scheduled_at: new Date(2018, 8, 6, 4, 3, 2),
        // finished_at: new Date(2018, 8, 7, 4, 3, 2),
        experiment_id: 'some-experiment-id',
        run_id: 'test-run-id',
        display_name: 'test run',
        description: 'test run description',
        state: V2beta1RuntimeState.SUCCEEDED,
      };
      render(
        <CommonTestWrapper>
          <RunDetailsV2
            pipeline_job={v2YamlTemplateString}
            run={noFinsihTimeRun}
            {...generateProps()}
          ></RunDetailsV2>
        </CommonTestWrapper>,
      );

      userEvent.click(screen.getByText('Detail'));

      expect(screen.getAllByText('-').length).toEqual(2); // finish time and duration are empty.
    });

    it('shows run parameters', () => {
      render(
        <CommonTestWrapper>
          <RunDetailsV2
            pipeline_job={v2YamlTemplateString}
            run={TEST_RUN}
            {...generateProps()}
          ></RunDetailsV2>
        </CommonTestWrapper>,
      );

      userEvent.click(screen.getByText('Detail'));

      screen.getByText('param1'); // 'Parameter name'
      screen.getByText('value1'); // 'Parameter value'
    });

    it('switches to Pipeline Spec tab', async () => {
      render(
        <CommonTestWrapper>
          <RunDetailsV2
            pipeline_job={v2YamlTemplateString}
            run={TEST_RUN}
            {...generateProps()}
          ></RunDetailsV2>
        </CommonTestWrapper>,
      );

      userEvent.click(screen.getByText('Pipeline Spec'));
      screen.findByTestId('spec-ir');
    });

    it('shows Execution Sidepanel', async () => {
      const getRunSpy = vi.spyOn(Apis.runServiceApiV2, 'getRun');
      getRunSpy.mockResolvedValue(TEST_RUN);
      const getExperimentSpy = vi.spyOn(Apis.experimentServiceApiV2, 'getExperiment');
      getExperimentSpy.mockResolvedValue(TEST_EXPERIMENT);

      render(
        <CommonTestWrapper>
          <RunDetailsV2
            pipeline_job={v2YamlTemplateString}
            run={TEST_RUN}
            {...generateProps()}
          ></RunDetailsV2>
        </CommonTestWrapper>,
      );

      // Default view has no side panel.
      expect(screen.queryByText('Input/Output')).toBeNull();
      expect(screen.queryByText('Task Details')).toBeNull();

      // Select execution to open side panel.
      userEvent.click(screen.getByText('preprocess'));
      screen.getByText('Input/Output');
      screen.getByText('Task Details');

      // Close side panel.
      userEvent.click(screen.getByLabelText('close'));
      expect(screen.queryByText('Input/Output')).toBeNull();
      expect(screen.queryByText('Task Details')).toBeNull();
    });

    it('shows Artifact Sidepanel', async () => {
      const getRunSpy = vi.spyOn(Apis.runServiceApiV2, 'getRun');
      getRunSpy.mockResolvedValue(TEST_RUN);
      const getExperimentSpy = vi.spyOn(Apis.experimentServiceApiV2, 'getExperiment');
      getExperimentSpy.mockResolvedValue(TEST_EXPERIMENT);

      render(
        <CommonTestWrapper>
          <RunDetailsV2
            pipeline_job={v2YamlTemplateString}
            run={TEST_RUN}
            {...generateProps()}
          ></RunDetailsV2>
        </CommonTestWrapper>,
      );

      // Default view has no side panel.
      expect(screen.queryByText('Artifact Info')).toBeNull();
      expect(screen.queryByText('Visualization')).toBeNull();

      // Select artifact to open side panel.
      userEvent.click(screen.getByText('model'));
      screen.getByText('Artifact Info');
      screen.getByText('Visualization');

      // Close side panel.
      userEvent.click(screen.getByLabelText('close'));
      expect(screen.queryByText('Artifact Info')).toBeNull();
      expect(screen.queryByText('Visualization')).toBeNull();
    });
  });
});
