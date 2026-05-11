//go:build aws

package cmd

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
)

func TestCallMCPTool_Success(t *testing.T) {
	expected := map[string]any{"analysis_status": "SUCCEEDED", "root_cause": "OOM"}
	resultJSON, _ := json.Marshal(expected)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Verify request structure
		var req mcpRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		if req.Method != "tools/call" {
			t.Errorf("got method %q, want tools/call", req.Method)
		}
		if req.Params.Name != "analyze_spark_workload" {
			t.Errorf("got tool %q, want analyze_spark_workload", req.Params.Name)
		}

		resp := map[string]any{
			"result": map[string]any{
				"content": []map[string]any{
					{"text": string(resultJSON)},
				},
			},
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
	}))
	defer srv.Close()

	cfg := aws.Config{
		Region:      "us-east-1",
		Credentials: credentials.NewStaticCredentialsProvider("AKID", "SECRET", "TOKEN"),
	}

	// Override endpoint by calling directly with the test server URL
	result, err := callMCPToolWithEndpoint(srv.URL, cfg, "us-east-1", "analyze_spark_workload", map[string]any{
		"platform_type":   "EMR_EC2",
		"platform_params": map[string]string{"cluster_id": "j-123"},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result["analysis_status"] != "SUCCEEDED" {
		t.Errorf("got status %v, want SUCCEEDED", result["analysis_status"])
	}
}

func TestCallMCPTool_RemoteError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		resp := map[string]any{
			"error": map[string]any{
				"message": "something went wrong",
			},
		}
		json.NewEncoder(w).Encode(resp)
	}))
	defer srv.Close()

	cfg := aws.Config{
		Region:      "us-east-1",
		Credentials: credentials.NewStaticCredentialsProvider("AKID", "SECRET", "TOKEN"),
	}

	_, err := callMCPToolWithEndpoint(srv.URL, cfg, "us-east-1", "analyze_spark_workload", map[string]any{})
	if err == nil {
		t.Fatal("expected error")
	}
	if got := err.Error(); got != "remote error: something went wrong" {
		t.Errorf("got error %q", got)
	}
}

func TestCallMCPTool_HTTPError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		w.Write([]byte(`{"message":"access denied"}`))
	}))
	defer srv.Close()

	cfg := aws.Config{
		Region:      "us-east-1",
		Credentials: credentials.NewStaticCredentialsProvider("AKID", "SECRET", "TOKEN"),
	}

	_, err := callMCPToolWithEndpoint(srv.URL, cfg, "us-east-1", "analyze_spark_workload", map[string]any{})
	if err == nil {
		t.Fatal("expected error")
	}
}
