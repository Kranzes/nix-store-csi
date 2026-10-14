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

{{- /* Changes with a prefetch list, so a rollout can wait for every node plugin to hold it. */}}
{{- define "nix-store-csi.prefetchRevision" -}}
{{ toJson . | sha256sum | trunc 12 }}
{{- end }}

{{- /*
Node selector terms for the nodes that match none of a list of node selectors.
A node is outside a selector if one of its labels differs, and NotIn also
matches a node without the label.
*/}}
{{- define "nix-store-csi.outside" -}}
{{- $terms := list (list) }}
{{- range $selector := . }}
{{- $next := list }}
{{- range $term := $terms }}
{{- range $key, $value := $selector }}
{{- $next = append $next (append $term (dict "key" $key "operator" "NotIn" "values" (list $value))) }}
{{- end }}
{{- end }}
{{- $terms = $next }}
{{- end }}
{{- range $terms }}
- matchExpressions:
    {{- toYaml . | nindent 4 }}
{{- end }}
{{- end }}
