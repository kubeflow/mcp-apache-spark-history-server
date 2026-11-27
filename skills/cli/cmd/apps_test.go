package cmd

import (
	"testing"

	"github.com/kubeflow/mcp-apache-spark-history-server/skills/cli/client"
	"github.com/kubeflow/mcp-apache-spark-history-server/skills/cli/util"
)

func TestLatestAttemptEpoch(t *testing.T) {
	tests := []struct {
		name string
		app  client.Application
		want int64
	}{
		{
			name: "nil attempts",
			app:  client.Application{},
			want: 0,
		},
		{
			name: "empty attempts",
			app:  client.Application{Attempts: &[]client.ApplicationAttempt{}},
			want: 0,
		},
		{
			name: "first attempt has epoch",
			app: client.Application{
				Attempts: &[]client.ApplicationAttempt{
					{StartTimeEpoch: util.Ptr(int64(2000))},
					{StartTimeEpoch: util.Ptr(int64(1000))},
				},
			},
			want: 2000,
		},
		{
			name: "skips nil epoch to next attempt",
			app: client.Application{
				Attempts: &[]client.ApplicationAttempt{
					{},
					{StartTimeEpoch: util.Ptr(int64(1500))},
				},
			},
			want: 1500,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := latestAttemptEpoch(tt.app); got != tt.want {
				t.Errorf("got %d, want %d", got, tt.want)
			}
		})
	}
}

func TestLatestAttemptDuration(t *testing.T) {
	tests := []struct {
		name string
		app  client.Application
		want int64
	}{
		{
			name: "nil attempts",
			app:  client.Application{},
			want: 0,
		},
		{
			name: "empty attempts",
			app:  client.Application{Attempts: &[]client.ApplicationAttempt{}},
			want: 0,
		},
		{
			name: "first attempt has duration",
			app: client.Application{
				Attempts: &[]client.ApplicationAttempt{
					{Duration: util.Ptr(int64(5000))},
					{Duration: util.Ptr(int64(3000))},
				},
			},
			want: 5000,
		},
		{
			name: "skips nil duration to next attempt",
			app: client.Application{
				Attempts: &[]client.ApplicationAttempt{
					{},
					{Duration: util.Ptr(int64(4200))},
				},
			},
			want: 4200,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := latestAttemptDuration(tt.app); got != tt.want {
				t.Errorf("got %d, want %d", got, tt.want)
			}
		})
	}
}

func TestAppAttemptCount(t *testing.T) {
	tests := []struct {
		name string
		app  client.Application
		want int
	}{
		{
			name: "nil attempts",
			app:  client.Application{},
			want: 0,
		},
		{
			name: "empty attempts",
			app:  client.Application{Attempts: &[]client.ApplicationAttempt{}},
			want: 0,
		},
		{
			name: "multiple attempts",
			app: client.Application{
				Attempts: &[]client.ApplicationAttempt{
					{AttemptId: util.Ptr("2")},
					{AttemptId: util.Ptr("1")},
				},
			},
			want: 2,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := appAttemptCount(tt.app); got != tt.want {
				t.Errorf("got %d, want %d", got, tt.want)
			}
		})
	}
}

func TestSortApps(t *testing.T) {
	makeApps := func() []client.Application {
		return []client.Application{
			{
				Id:   util.Ptr("app-2"),
				Name: util.Ptr("beta"),
				Attempts: &[]client.ApplicationAttempt{
					{StartTimeEpoch: util.Ptr(int64(3000)), Duration: util.Ptr(int64(100))},
				},
			},
			{
				Id:   util.Ptr("app-3"),
				Name: util.Ptr("alpha"),
				Attempts: &[]client.ApplicationAttempt{
					{StartTimeEpoch: util.Ptr(int64(1000)), Duration: util.Ptr(int64(500))},
				},
			},
			{
				Id:   util.Ptr("app-1"),
				Name: util.Ptr("gamma"),
				Attempts: &[]client.ApplicationAttempt{
					{StartTimeEpoch: util.Ptr(int64(2000)), Duration: util.Ptr(int64(250))},
				},
			},
		}
	}

	tests := []struct {
		name      string
		field     string
		desc      bool
		wantOrder []string
	}{
		{name: "by name asc", field: "name", desc: false, wantOrder: []string{"app-3", "app-2", "app-1"}},
		{name: "by name desc", field: "name", desc: true, wantOrder: []string{"app-1", "app-2", "app-3"}},
		{name: "by id asc", field: "id", desc: false, wantOrder: []string{"app-1", "app-2", "app-3"}},
		{name: "by id desc", field: "id", desc: true, wantOrder: []string{"app-3", "app-2", "app-1"}},
		{name: "by date asc", field: "date", desc: false, wantOrder: []string{"app-3", "app-1", "app-2"}},
		{name: "by date desc", field: "date", desc: true, wantOrder: []string{"app-2", "app-1", "app-3"}},
		{name: "by duration asc", field: "duration", desc: false, wantOrder: []string{"app-2", "app-1", "app-3"}},
		{name: "by duration desc", field: "duration", desc: true, wantOrder: []string{"app-3", "app-1", "app-2"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			apps := makeApps()
			sortApps(apps, tt.field, tt.desc)
			for i, want := range tt.wantOrder {
				if got := util.Deref(apps[i].Id); got != want {
					t.Errorf("position %d: got %q, want %q", i, got, want)
				}
			}
		})
	}
}
