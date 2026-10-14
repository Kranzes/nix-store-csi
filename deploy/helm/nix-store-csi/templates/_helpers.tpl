{{- define "nix-store-csi.selectorLabels" -}}
app.kubernetes.io/name: {{ .Chart.Name }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{- define "nix-store-csi.labels" -}}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{ include "nix-store-csi.selectorLabels" . }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{- /* Changes with the prefetch list, so a rollout can wait for every node plugin to hold it. */}}
{{- define "nix-store-csi.prefetchRevision" -}}
{{ toJson .Values.prefetch | sha256sum | trunc 12 }}
{{- end }}
