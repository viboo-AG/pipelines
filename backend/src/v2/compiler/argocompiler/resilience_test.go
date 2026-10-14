// Copyright 2026 The Kubeflow Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package argocompiler

import (
	"testing"

	wfapi "github.com/argoproj/argo-workflows/v3/pkg/apis/workflow/v1alpha1"
	"github.com/kubeflow/pipelines/api/v2alpha1/go/pipelinespec"
	"github.com/kubeflow/pipelines/backend/src/apiserver/config/proxy"
	"github.com/kubeflow/pipelines/backend/src/common/util"
	"github.com/kubeflow/pipelines/kubernetes_platform/go/kubernetesplatform"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"
)

func TestCompileDriverRetriesAndPodMetadata(t *testing.T) {
	proxy.InitializeConfigWithEmptyForTests()
	job := &pipelinespec.PipelineJob{}
	require.NoError(t, protojson.Unmarshal([]byte(`{"pipelineSpec": {
		"pipelineInfo": {"name": "driver-retry-test"},
		"root": {"dag": {"tasks": {"task": {
			"taskInfo": {"name": "task"}, "componentRef": {"name": "comp-task"}
		}}}},
		"components": {"comp-task": {"executorLabel": "exec-task"}},
		"deploymentSpec": {"executors": {"exec-task": {"container": {
			"image": "busybox", "command": ["true"]
		}}}},
		"schemaVersion": "2.1.0", "sdkVersion": "kfp-2.16.0"
	}}`), job))
	wf, err := Compile(job, nil, &Options{})
	require.NoError(t, err)
	assert.Equal(t, "false", wf.Spec.PodMetadata.Labels[util.AnnotationKeyIstioSidecarInject])
	assert.Nil(t, wf.Spec.RetryStrategy, "retries must not apply to all templates")
	assert.Nil(t, wf.Spec.ActiveDeadlineSeconds, "no workflow time limit was requested")
	drivers := map[string]bool{"system-dag-driver": false, "system-container-driver": false}
	for _, template := range wf.Spec.Templates {
		if _, isDriver := drivers[template.Name]; !isDriver {
			assert.Nil(t, template.RetryStrategy, "unexpected retries on %s", template.Name)
			continue
		}
		drivers[template.Name] = true
		strategy := template.RetryStrategy
		require.NotNil(t, strategy, template.Name)
		require.NotNil(t, strategy.Limit)
		assert.Equal(t, 2, strategy.Limit.IntValue())
		assert.Equal(t, wfapi.RetryPolicyAlways, strategy.RetryPolicy)
		require.NotNil(t, strategy.Backoff)
		assert.Equal(t, "30m", strategy.Backoff.Duration)
		require.NotNil(t, strategy.Backoff.Factor)
		assert.Equal(t, 1, strategy.Backoff.Factor.IntValue())
		assert.Empty(t, strategy.Backoff.MaxDuration)
	}
	assert.Equal(t, map[string]bool{"system-dag-driver": true, "system-container-driver": true}, drivers)
}

func TestExecutorPreservesExplicitIstioInjection(t *testing.T) {
	for _, tc := range []struct {
		name        string
		annotations map[string]string
		labels      map[string]string
		label       string
	}{
		{name: "default"},
		{name: "legacy annotation opt-in", annotations: map[string]string{util.AnnotationKeyIstioSidecarInject: "true"}, label: "true"},
		{name: "legacy annotation opt-out", annotations: map[string]string{util.AnnotationKeyIstioSidecarInject: "false"}, label: "false"},
		{name: "label opt-in", labels: map[string]string{util.AnnotationKeyIstioSidecarInject: "true"}, label: "true"},
		{name: "label overrides annotation", annotations: map[string]string{util.AnnotationKeyIstioSidecarInject: "true"}, labels: map[string]string{util.AnnotationKeyIstioSidecarInject: "false"}, label: "false"},
		{name: "unrelated annotation", annotations: map[string]string{"example.com/key": "value"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := &workflowCompiler{templates: make(map[string]*wfapi.Template), wf: &wfapi.Workflow{}}
			config, err := protojson.Marshal(&kubernetesplatform.KubernetesExecutorConfig{PodMetadata: &kubernetesplatform.PodMetadata{Annotations: tc.annotations, Labels: tc.labels}})
			require.NoError(t, err)
			c.wf.Spec.Arguments.Parameters = []wfapi.Parameter{{Name: argumentsKubernetesSpec + "comp-task", Value: wfapi.AnyStringPtr(string(config))}}
			task, err := c.containerExecutorTask("executor", containerExecutorInputs{}, &pipelinespec.PipelineTaskSpec{ComponentRef: &pipelinespec.ComponentRef{Name: "comp-task"}})
			require.NoError(t, err)
			var executor *wfapi.Template
			for _, template := range c.templates {
				if template.Container != nil {
					executor = template
				}
			}
			require.NotNil(t, executor)
			assert.NotContains(t, executor.Metadata.Labels, util.AnnotationKeyIstioSidecarInject, "explicit overrides must use runtime parameters")
			if tc.label != "" {
				assert.Equal(t, inputParameter(paramPodLabelVal), executor.Metadata.Labels[inputParameter(paramPodLabelKey)])
				value := task.Arguments.GetParameterByName(paramPodLabelVal)
				require.NotNil(t, value)
				assert.Equal(t, tc.label, value.Value.String())
				key := task.Arguments.GetParameterByName(paramPodLabelKey)
				require.NotNil(t, key)
				assert.Equal(t, util.AnnotationKeyIstioSidecarInject, key.Value.String())
			} else {
				assert.Nil(t, task.Arguments.GetParameterByName(paramPodLabelVal))
			}
			if len(tc.annotations) > 0 {
				assert.Equal(t, inputParameter(paramPodAnnotationVal), executor.Metadata.Annotations[inputParameter(paramPodAnnotationKey)])
			}
		})
	}
}
