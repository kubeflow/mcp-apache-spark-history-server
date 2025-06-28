{{/*
Expand the name of the chart.
*/}}
{{- define "spark-history-mcp.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "spark-history-mcp.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "spark-history-mcp.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "spark-history-mcp.labels" -}}
helm.sh/chart: {{ include "spark-history-mcp.chart" . }}
{{ include "spark-history-mcp.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "spark-history-mcp.selectorLabels" -}}
app.kubernetes.io/name: {{ include "spark-history-mcp.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Create the name of the service account to use
*/}}
{{- define "spark-history-mcp.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "spark-history-mcp.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
Create the name of the config map
*/}}
{{- define "spark-history-mcp.configMapName" -}}
{{- printf "%s-config" (include "spark-history-mcp.fullname" .) }}
{{- end }}

{{/*
Create the name of the secret
*/}}
{{- define "spark-history-mcp.secretName" -}}
{{- if .Values.auth.secret.create }}
{{- printf "%s-auth" (include "spark-history-mcp.fullname" .) }}
{{- else }}
{{- default (printf "%s-auth" (include "spark-history-mcp.fullname" .)) .Values.auth.secret.name }}
{{- end }}
{{- end }}

{{/*
Create image name
*/}}
{{- define "spark-history-mcp.image" -}}
{{- printf "%s:%s" .Values.image.repository (.Values.image.tag | default .Chart.AppVersion) }}
{{- end }}

{{/*
Create environment variables
*/}}
{{- define "spark-history-mcp.env" -}}
- name: MCP_PORT
  value: {{ .Values.config.port | quote }}
- name: MCP_DEBUG
  value: {{ .Values.config.debug | quote }}
{{- if .Values.auth.enabled }}
- name: SPARK_USERNAME
  valueFrom:
    secretKeyRef:
      name: {{ include "spark-history-mcp.secretName" . }}
      key: username
      optional: true
- name: SPARK_PASSWORD
  valueFrom:
    secretKeyRef:
      name: {{ include "spark-history-mcp.secretName" . }}
      key: password
      optional: true
- name: SPARK_TOKEN
  valueFrom:
    secretKeyRef:
      name: {{ include "spark-history-mcp.secretName" . }}
      key: token
      optional: true
{{- end }}
{{- range .Values.env }}
- name: {{ .name }}
  value: {{ .value | quote }}
{{- end }}
{{- end }}
