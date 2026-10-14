{{/*
Expand the name of the chart.
*/}}
{{- define "mcp-apache-spark-history-server.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
Use the release name by default so the chart's long name is not repeated in every
resource name. Keep the standard Helm override behavior when nameOverride or
fullnameOverride is explicitly configured.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
*/}}
{{- define "mcp-apache-spark-history-server.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else if .Values.nameOverride }}
{{- $name := .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- else }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "mcp-apache-spark-history-server.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "mcp-apache-spark-history-server.labels" -}}
helm.sh/chart: {{ include "mcp-apache-spark-history-server.chart" . }}
{{ include "mcp-apache-spark-history-server.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "mcp-apache-spark-history-server.selectorLabels" -}}
app.kubernetes.io/name: {{ include "mcp-apache-spark-history-server.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Create the name of the service account to use
*/}}
{{- define "mcp-apache-spark-history-server.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "mcp-apache-spark-history-server.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
Create the name of the config map
*/}}
{{- define "mcp-apache-spark-history-server.configMapName" -}}
{{- printf "%s-config" (include "mcp-apache-spark-history-server.fullname" .) }}
{{- end }}

{{/*
Create the name of the shared auth secret
*/}}
{{- define "mcp-apache-spark-history-server.secretName" -}}
{{- $default := printf "%s-auth" (include "mcp-apache-spark-history-server.fullname" .) -}}
{{- if .Values.auth.secret.name -}}
{{- .Values.auth.secret.name -}}
{{- else -}}
{{- $default -}}
{{- end -}}
{{- end }}

{{/*
Create the name of the auth secret for one Spark History Server.
Requires a dict with "root" (the root context) and "server" (the server name).
Uses the configured secret name, or derives "<fullname>-auth-<server>".
*/}}
{{- define "mcp-apache-spark-history-server.serverSecretName" -}}
{{- $root := .root -}}
{{- $server := .server -}}
{{- $serverAuth := index (default (dict) $root.Values.auth.servers) $server | default (dict) -}}
{{- $serverSecret := $serverAuth.secret | default (dict) -}}
{{- if $serverSecret.name -}}
{{- $serverSecret.name -}}
{{- else -}}
{{- printf "%s-auth-%s" (include "mcp-apache-spark-history-server.fullname" $root) ($server | lower | replace "_" "-") | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end }}

{{/*
Whether the chart renders at least one auth Secret (shared or per server)
*/}}
{{- define "mcp-apache-spark-history-server.hasManagedAuthSecrets" -}}
{{- $managed := false -}}
{{- $sharedSecret := .Values.auth.secret | default (dict) -}}
{{- if $sharedSecret.create -}}
{{- $managed = true -}}
{{- end -}}
{{- range $serverName, $serverAuth := (default (dict) .Values.auth.servers) -}}
{{- $auth := $serverAuth | default (dict) -}}
{{- $secret := $auth.secret | default (dict) -}}
{{- if $secret.create -}}
{{- $managed = true -}}
{{- end -}}
{{- end -}}
{{- $managed -}}
{{- end }}

{{/*
Validate the per-server authentication values
*/}}
{{- define "mcp-apache-spark-history-server.validateAuth" -}}
{{- range $serverName, $server := $.Values.config.servers -}}
{{- if ne $serverName ($serverName | lower) -}}
{{- fail (printf "config.servers.%s must be lowercase when auth is enabled: the chart injects credentials as SHS_SERVERS__<NAME>__AUTH__* and the server lowercases env-derived keys" $serverName) -}}
{{- end -}}
{{- end -}}
{{- range $serverName, $serverAuth := (default (dict) .Values.auth.servers) -}}
{{- $auth := $serverAuth | default (dict) -}}
{{- $secret := $auth.secret | default (dict) -}}
{{- if not (hasKey $.Values.config.servers $serverName) -}}
{{- fail (printf "auth.servers.%s does not match any config.servers entry (server names are case-sensitive)" $serverName) -}}
{{- end -}}
{{- if and (not $secret.create) (not $secret.name) (or $secret.username $secret.password $secret.token) -}}
{{- fail (printf "auth.servers.%s.secret defines credentials but neither create nor name is set" $serverName) -}}
{{- end -}}
{{- end -}}
{{- end }}

{{/*
Create image name
*/}}
{{- define "mcp-apache-spark-history-server.image" -}}
{{- printf "%s:%s" .Values.image.repository (.Values.image.tag | default .Chart.AppVersion) }}
{{- end }}

{{/*
Create environment variables
*/}}
{{- define "mcp-apache-spark-history-server.env" -}}
- name: SHS_MCP__PORT
  value: {{ .Values.config.port | quote }}
- name: SHS_MCP__DEBUG
  value: {{ .Values.config.debug | quote }}
{{- if .Values.auth.enabled }}
{{- include "mcp-apache-spark-history-server.validateAuth" . }}
{{- $sharedSecret := (default (dict) .Values.auth.secret) -}}
{{- $externalSecret := (default (dict) .Values.auth.externalsecret) -}}
{{- $csiSecretStore := (default (dict) .Values.auth.csisecretstore) -}}
{{- $hasSharedSecret := or $sharedSecret.create $sharedSecret.name $externalSecret.create $csiSecretStore.enabled -}}
{{- range $serverName, $server := .Values.config.servers }}
{{- $serverAuth := (index (default (dict) $.Values.auth.servers) $serverName) | default (dict) -}}
{{- $serverSecret := $serverAuth.secret | default (dict) -}}
{{- $secretName := "" -}}
{{- if or $serverSecret.create $serverSecret.name -}}
{{- $secretName = include "mcp-apache-spark-history-server.serverSecretName" (dict "root" $ "server" $serverName) -}}
{{- /* Servers with inline auth keep their own credentials. */ -}}
{{- else if and $hasSharedSecret (not $server.auth) -}}
{{- $secretName = include "mcp-apache-spark-history-server.secretName" $ -}}
{{- end -}}
{{- if $secretName }}
- name: SHS_SERVERS__{{ $serverName | upper }}__AUTH__USERNAME
  valueFrom:
    secretKeyRef:
      name: {{ $secretName }}
      key: username
      optional: true
- name: SHS_SERVERS__{{ $serverName | upper }}__AUTH__PASSWORD
  valueFrom:
    secretKeyRef:
      name: {{ $secretName }}
      key: password
      optional: true
- name: SHS_SERVERS__{{ $serverName | upper }}__AUTH__TOKEN
  valueFrom:
    secretKeyRef:
      name: {{ $secretName }}
      key: token
      optional: true
{{- end }}
{{- end }}
{{- end }}
{{- range .Values.env }}
- name: {{ .name }}
  value: {{ .value | quote }}
{{- end }}
{{- end }}

{{/*
Get the name of the SecretProviderClass (must be created externally)
*/}}
{{- define "mcp-apache-spark-history-server.secretProviderClassName" -}}
{{- .Values.auth.csisecretstore.secretProviderClassName -}}
{{- end }}
