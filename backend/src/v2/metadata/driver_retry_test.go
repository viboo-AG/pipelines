// Copyright 2026 The Kubeflow Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package metadata

import (
	"context"
	"testing"

	pb "github.com/kubeflow/pipelines/third_party/ml-metadata/go/ml_metadata"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// Model MLMD's unique execution names and writes whose responses are lost.
type retryMetadataStore struct {
	pb.MetadataStoreServiceClient
	executions        map[string]*pb.Execution
	loseWriteResponse bool
	loseReadResponse  bool
	writeError        error
}

func (s *retryMetadataStore) PutExecutionType(context.Context, *pb.PutExecutionTypeRequest, ...grpc.CallOption) (*pb.PutExecutionTypeResponse, error) {
	return &pb.PutExecutionTypeResponse{TypeId: proto.Int64(1)}, nil
}

func (s *retryMetadataStore) PutExecution(_ context.Context, req *pb.PutExecutionRequest, _ ...grpc.CallOption) (*pb.PutExecutionResponse, error) {
	if s.writeError != nil {
		return nil, s.writeError
	}
	name := req.Execution.GetName()
	if _, exists := s.executions[name]; exists {
		return nil, status.Error(codes.AlreadyExists, "execution name exists")
	}
	e := proto.Clone(req.Execution).(*pb.Execution)
	e.Id = proto.Int64(int64(len(s.executions) + 1))
	s.executions[name] = e
	if s.loseWriteResponse {
		s.loseWriteResponse = false
		return nil, status.Error(codes.Unavailable, "response lost after commit")
	}
	return &pb.PutExecutionResponse{ExecutionId: e.Id}, nil
}

func (s *retryMetadataStore) GetExecutionsByID(_ context.Context, req *pb.GetExecutionsByIDRequest, _ ...grpc.CallOption) (*pb.GetExecutionsByIDResponse, error) {
	if s.loseReadResponse {
		s.loseReadResponse = false
		return nil, status.Error(codes.Unavailable, "read unavailable after commit")
	}
	for _, e := range s.executions {
		if e.GetId() == req.ExecutionIds[0] {
			return &pb.GetExecutionsByIDResponse{Executions: []*pb.Execution{proto.Clone(e).(*pb.Execution)}}, nil
		}
	}
	return nil, status.Error(codes.NotFound, "execution missing")
}

func (s *retryMetadataStore) GetExecutionByTypeAndName(_ context.Context, req *pb.GetExecutionByTypeAndNameRequest, _ ...grpc.CallOption) (*pb.GetExecutionByTypeAndNameResponse, error) {
	if e, ok := s.executions[req.GetExecutionName()]; ok {
		return &pb.GetExecutionByTypeAndNameResponse{Execution: proto.Clone(e).(*pb.Execution)}, nil
	}
	return nil, status.Error(codes.NotFound, "execution missing")
}

func retryTestClient() (*Client, *Pipeline, *retryMetadataStore) {
	store := &retryMetadataStore{executions: make(map[string]*pb.Execution)}
	return &Client{svc: store}, &Pipeline{pipelineCtx: &pb.Context{Id: proto.Int64(1)}, pipelineRunCtx: &pb.Context{Id: proto.Int64(2)}}, store
}

func TestDriverRetryPreservesExecutionAndOutputs(t *testing.T) {
	for _, config := range []ExecutionConfig{
		{Name: "run/test-run", ExecutionType: DagExecutionTypeName},
		{TaskName: "train", ParentDagID: 10, ExecutionType: ContainerExecutionTypeName},
		{TaskName: "nested", ParentDagID: 10, ExecutionType: DagExecutionTypeName},
	} {
		t.Run(string(config.ExecutionType)+config.TaskName, func(t *testing.T) {
			client, pipeline, store := retryTestClient()
			first, err := client.CreateExecutionForDriver(context.Background(), pipeline, &config)
			require.NoError(t, err)
			for _, e := range store.executions {
				e.LastKnownState = pb.Execution_COMPLETE.Enum()
				e.CustomProperties["output-proof"] = StringValue("preserved")
			}
			again, err := client.CreateExecutionForDriver(context.Background(), pipeline, &config)
			require.NoError(t, err)
			require.Equal(t, first.GetID(), again.GetID())
			require.Len(t, store.executions, 1)
			require.Equal(t, pb.Execution_COMPLETE, again.GetExecution().GetLastKnownState())
			require.Equal(t, "preserved", again.GetExecution().CustomProperties["output-proof"].GetStringValue())
		})
	}
}

func TestDriverRetryRecoversAmbiguousCommit(t *testing.T) {
	for _, loseWrite := range []bool{true, false} {
		client, pipeline, store := retryTestClient()
		store.loseWriteResponse, store.loseReadResponse = loseWrite, !loseWrite
		config := &ExecutionConfig{TaskName: "train", ParentDagID: 10, ExecutionType: ContainerExecutionTypeName}
		_, err := client.CreateExecutionForDriver(context.Background(), pipeline, config)
		require.Equal(t, codes.Unavailable, status.Code(err))
		retried, err := client.CreateExecutionForDriver(context.Background(), pipeline, config)
		require.NoError(t, err)
		require.Equal(t, int64(1), retried.GetID())
		require.Len(t, store.executions, 1)
		require.Empty(t, config.Name, "the caller's configuration must not be mutated")
	}
}

func TestDriverExecutionIdentitySeparatesTasksAndIterations(t *testing.T) {
	client, pipeline, store := retryTestClient()
	zero, one := 0, 1
	for _, config := range []ExecutionConfig{
		{TaskName: "train", ParentDagID: 10},
		{TaskName: "train", ParentDagID: 11},
		{TaskName: "consume", ParentDagID: 10},
		{TaskName: "train", ParentDagID: 10, IterationIndex: &zero},
		{TaskName: "train", ParentDagID: 10, IterationIndex: &one},
	} {
		config.ExecutionType = ContainerExecutionTypeName
		_, err := client.CreateExecutionForDriver(context.Background(), pipeline, &config)
		require.NoError(t, err)
	}
	require.Len(t, store.executions, 5)
}

func TestDriverExecutionDoesNotHideWriteErrors(t *testing.T) {
	client, pipeline, store := retryTestClient()
	store.writeError = status.Error(codes.PermissionDenied, "denied")
	_, err := client.CreateExecutionForDriver(context.Background(), pipeline, &ExecutionConfig{Name: "run/test", ExecutionType: DagExecutionTypeName})
	require.Equal(t, codes.PermissionDenied, status.Code(err))
	require.Empty(t, store.executions)
}
