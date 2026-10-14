// Copyright 2026 The Kubeflow Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package resource

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	workflowapi "github.com/argoproj/argo-workflows/v3/pkg/apis/workflow/v1alpha1"
	"github.com/argoproj/argo-workflows/v3/util/file"
	"github.com/kubeflow/pipelines/backend/src/apiserver/client"
	"github.com/kubeflow/pipelines/backend/src/apiserver/common"
	"github.com/kubeflow/pipelines/backend/src/apiserver/model"
	"github.com/kubeflow/pipelines/backend/src/common/util"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

type fakeOffloadedNodeStatusReader struct {
	nodes   workflowapi.Nodes
	err     error
	uid     string
	version string
}

func (f *fakeOffloadedNodeStatusReader) Get(uid, version string) (workflowapi.Nodes, error) {
	f.uid, f.version = uid, version
	return f.nodes, f.err
}

func offloadedRunWorkflow(t *testing.T, manager *ResourceManager, run *model.Run) *util.Workflow {
	t.Helper()
	execution, err := manager.getWorkflowClient(common.GetPodNamespace()).Get(context.Background(), run.K8SName, metav1.GetOptions{})
	require.NoError(t, err)
	workflow := execution.(*util.Workflow)
	workflow.UID = types.UID("workflow-uid")
	workflow.Status.Phase = workflowapi.WorkflowRunning
	workflow.Status.Nodes = nil
	workflow.Status.CompressedNodes = ""
	workflow.Status.OffloadNodeStatusVersion = "fnv:123"
	return workflow
}

func assertHydratedWorkflowManifest(t *testing.T, run *model.Run, expected workflowapi.Nodes) {
	t.Helper()
	var workflow workflowapi.Workflow
	require.NoError(t, json.Unmarshal([]byte(run.WorkflowRuntimeManifest), &workflow))
	assert.Empty(t, workflow.Status.OffloadNodeStatusVersion)
	assert.Empty(t, workflow.Status.Nodes)
	require.NotEmpty(t, workflow.Status.CompressedNodes)
	decoded, err := file.DecodeDecompressString(workflow.Status.CompressedNodes)
	require.NoError(t, err)
	var nodes workflowapi.Nodes
	require.NoError(t, json.Unmarshal([]byte(decoded), &nodes))
	assert.Equal(t, expected, nodes)
}

func TestGetRunForDisplayRetrievesOffloadedNodes(t *testing.T) {
	store, manager, created := initWithOneTimeRunV2(t)
	defer store.Close()
	workflow := offloadedRunWorkflow(t, manager, created)
	nodes := workflowapi.Nodes{
		"root": {ID: "root", Name: workflow.Name + ".root", Type: workflowapi.NodeTypeDAG, Phase: workflowapi.NodeRunning},
		"unit": {ID: "unit", Name: workflow.Name + ".root.for-loop-1.iteration-iterations(0:0)", Type: workflowapi.NodeTypeDAG, Phase: workflowapi.NodeSucceeded},
	}
	repo := &fakeOffloadedNodeStatusReader{nodes: nodes}
	manager.offloadedNodeStatusRepo = repo

	displayed, err := manager.GetRunForDisplay(context.Background(), created.UUID)
	require.NoError(t, err)
	assert.Equal(t, model.RuntimeStateRunning, displayed.State)
	assert.Equal(t, "workflow-uid", repo.uid)
	assert.Equal(t, "fnv:123", repo.version)
	assertHydratedWorkflowManifest(t, displayed, nodes)
	assert.Equal(t, "fnv:123", workflow.Status.OffloadNodeStatusVersion)
	assert.Empty(t, workflow.Status.CompressedNodes)
}

func TestGetRunForDisplayRetrievesOffloadedNodesAfterWorkflowGC(t *testing.T) {
	store, manager, created := initWithOneTimeRunV2(t)
	defer store.Close()
	workflow := offloadedRunWorkflow(t, manager, created)
	nodes := workflowapi.Nodes{
		"unit": {ID: "unit", Name: workflow.Name + ".root.for-loop-1.iteration-iterations(0:0)", Type: workflowapi.NodeTypeDAG, Phase: workflowapi.NodeSucceeded},
	}
	manager.offloadedNodeStatusRepo = &fakeOffloadedNodeStatusReader{nodes: nodes}
	persisted, err := manager.GetRun(created.UUID)
	require.NoError(t, err)
	persisted.WorkflowRuntimeManifest = model.LargeText(workflow.ToStringForStore())
	require.NoError(t, manager.runStore.UpdateRun(persisted))
	manager.execClient = client.NewFakeExecClient()

	displayed, err := manager.GetRunForDisplay(context.Background(), created.UUID)
	require.NoError(t, err)
	assertHydratedWorkflowManifest(t, displayed, nodes)
}

func TestGetRunForDisplayKeepsWorkflowPhaseWhenOffloadUnavailable(t *testing.T) {
	store, manager, created := initWithOneTimeRunV2(t)
	defer store.Close()
	offloadedRunWorkflow(t, manager, created)
	manager.offloadedNodeStatusRepo = &fakeOffloadedNodeStatusReader{err: errors.New("database unavailable")}

	displayed, err := manager.GetRunForDisplay(context.Background(), created.UUID)
	require.NoError(t, err)
	assert.Equal(t, model.RuntimeStateRunning, displayed.State)
	var workflow workflowapi.Workflow
	require.NoError(t, json.Unmarshal([]byte(displayed.WorkflowRuntimeManifest), &workflow))
	assert.Equal(t, "fnv:123", workflow.Status.OffloadNodeStatusVersion)
}
