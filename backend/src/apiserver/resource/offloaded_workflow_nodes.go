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
	"fmt"

	argoconfig "github.com/argoproj/argo-workflows/v3/config"
	argopersist "github.com/argoproj/argo-workflows/v3/persist/sqldb"
	workflowapi "github.com/argoproj/argo-workflows/v3/pkg/apis/workflow/v1alpha1"
	"github.com/argoproj/argo-workflows/v3/util/file"
	argosqldb "github.com/argoproj/argo-workflows/v3/util/sqldb"
	"github.com/golang/glog"
	"github.com/kubeflow/pipelines/backend/src/apiserver/common"
	"github.com/kubeflow/pipelines/backend/src/apiserver/model"
	"github.com/kubeflow/pipelines/backend/src/common/util"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/yaml"
)

const argoControllerConfigMap = "argo-workflow-controller-configmap"

type offloadedNodeStatusReader interface {
	Get(uid, version string) (workflowapi.Nodes, error)
}

// Argo stores large node maps outside the Workflow CR. Reuse Argo's configured repository
// and return a compressed copy so the run API remains small and the UI can decode it.
func (r *ResourceManager) hydrateWorkflowForDisplay(ctx context.Context, workflow *util.Workflow) *util.Workflow {
	version := workflow.Status.OffloadNodeStatusVersion
	if version == "" {
		return workflow
	}

	repo, err := r.getOffloadedNodeStatusRepo(ctx)
	if err != nil {
		glog.Errorf("Cannot open Argo offloaded node repository for Workflow %s/%s: %v", workflow.Namespace, workflow.Name, err)
		return workflow
	}
	nodes, err := repo.Get(string(workflow.UID), version)
	if err != nil {
		glog.Errorf("Cannot load Argo offloaded nodes for Workflow %s/%s, version %s: %v", workflow.Namespace, workflow.Name, version, err)
		return workflow
	}
	if len(nodes) == 0 {
		glog.Errorf("Argo offloaded node repository returned no nodes for Workflow %s/%s, version %s", workflow.Namespace, workflow.Name, version)
		return workflow
	}
	encoded, err := json.Marshal(nodes)
	if err != nil {
		glog.Errorf("Cannot encode Argo offloaded nodes for Workflow %s/%s: %v", workflow.Namespace, workflow.Name, err)
		return workflow
	}

	copy := workflow.Workflow.DeepCopy()
	copy.Status.Nodes = nil
	copy.Status.CompressedNodes = file.CompressEncodeString(string(encoded))
	copy.Status.OffloadNodeStatusVersion = ""
	return util.NewWorkflow(copy)
}

func (r *ResourceManager) hydrateStoredWorkflowForDisplay(ctx context.Context, run *model.Run) {
	if run.WorkflowRuntimeManifest == "" {
		return
	}
	var workflow workflowapi.Workflow
	if err := json.Unmarshal([]byte(run.WorkflowRuntimeManifest), &workflow); err != nil || workflow.Status.OffloadNodeStatusVersion == "" {
		return
	}
	hydrated := r.hydrateWorkflowForDisplay(ctx, util.NewWorkflow(&workflow))
	if hydrated.Status.OffloadNodeStatusVersion == "" {
		run.WorkflowRuntimeManifest = model.LargeText(hydrated.ToStringForStore())
	}
}

func (r *ResourceManager) getOffloadedNodeStatusRepo(ctx context.Context) (offloadedNodeStatusReader, error) {
	r.offloadedNodeStatusMu.Lock()
	defer r.offloadedNodeStatusMu.Unlock()
	if r.offloadedNodeStatusRepo != nil {
		return r.offloadedNodeStatusRepo, nil
	}

	namespace := common.GetPodNamespace()
	clientSet := r.k8sCoreClient.GetClientSet()
	configMap, err := clientSet.CoreV1().ConfigMaps(namespace).Get(ctx, argoControllerConfigMap, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("read Argo controller configuration: %w", err)
	}
	if configMap.Data["persistence"] == "" {
		return nil, fmt.Errorf("Argo controller persistence is not configured")
	}
	var persistence argoconfig.PersistConfig
	if err := yaml.Unmarshal([]byte(configMap.Data["persistence"]), &persistence); err != nil {
		return nil, fmt.Errorf("parse Argo persistence configuration: %w", err)
	}
	tableName, err := argopersist.GetTableName(&persistence)
	if err != nil {
		return nil, fmt.Errorf("find Argo offload table: %w", err)
	}
	session, err := argosqldb.CreateDBSession(ctx, clientSet, namespace, persistence.DBConfig)
	if err != nil {
		return nil, fmt.Errorf("connect to Argo persistence: %w", err)
	}
	repo, err := argopersist.NewOffloadNodeStatusRepo(session, persistence.GetClusterName(), tableName)
	if err != nil {
		_ = session.Close()
		return nil, fmt.Errorf("open Argo offloaded node repository: %w", err)
	}
	r.offloadedNodeStatusRepo = repo
	return repo, nil
}
