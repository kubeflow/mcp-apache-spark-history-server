package cmd

import (
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestResolvePlatform_EMR_EC2(t *testing.T) {
	appID = "s-XXXXX"
	clusterID = "j-12345"
	emrServerlessApp = ""

	pType, params, err := resolvePlatform()
	if err != nil {
		t.Fatal(err)
	}
	if pType != "EMR_EC2" {
		t.Errorf("got platform_type=%q, want EMR_EC2", pType)
	}
	want := map[string]string{"cluster_id": "j-12345", "step_id": "s-XXXXX"}
	if diff := cmp.Diff(want, params); diff != "" {
		t.Errorf("params mismatch (-want +got):\n%s", diff)
	}
}

func TestResolvePlatform_EMRServerless(t *testing.T) {
	appID = ""
	clusterID = ""
	emrServerlessApp = "00abc"
	emrServerlessRun = "00xyz"

	pType, params, err := resolvePlatform()
	if err != nil {
		t.Fatal(err)
	}
	if pType != "EMR_SERVERLESS" {
		t.Errorf("got platform_type=%q, want EMR_SERVERLESS", pType)
	}
	want := map[string]string{"application_id": "00abc", "job_run_id": "00xyz"}
	if diff := cmp.Diff(want, params); diff != "" {
		t.Errorf("params mismatch (-want +got):\n%s", diff)
	}
}

func TestResolvePlatform_EMRServerlessMissingRunID(t *testing.T) {
	clusterID = ""
	emrServerlessApp = "00abc"
	emrServerlessRun = ""

	_, _, err := resolvePlatform()
	if err == nil {
		t.Fatal("expected error when --job-run missing")
	}
}
