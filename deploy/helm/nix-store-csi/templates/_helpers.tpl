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

{{- /* The locality pods' labels differ from the node plugins', so selectors for one don't pick the other. */}}
{{- define "nix-store-csi.localitySelectorLabels" -}}
app.kubernetes.io/name: {{ .Chart.Name }}-locality
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{- define "nix-store-csi.image" -}}
{{- $tag := .Values.image.tag | default .Chart.AppVersion -}}
image: {{ .Values.image.repository }}:{{ $tag }}
imagePullPolicy: {{ .Values.image.pullPolicy | default (ternary "IfNotPresent" "Always" (regexMatch "^v?[0-9]+\\.[0-9]+\\.[0-9]+" $tag)) }}
{{- end }}

{{- /* The node plugins and the locality extender read the same caches. */}}
{{- define "nix-store-csi.cacheEnv" -}}
- name: NIX_STORE_CSI_SUBSTITUTERS
  value: {{ join " " .Values.substituters | quote }}
- name: NIX_STORE_CSI_TRUSTED_PUBLIC_KEYS
  value: {{ join " " .Values.trustedPublicKeys | quote }}
{{- if .Values.netrcSecret }}
- name: NIX_STORE_CSI_NETRC_FILE
  value: /var/run/secrets/nix-store-csi/netrc
{{- end }}
{{- end }}

{{- define "nix-store-csi.credentialsMount" -}}
{{- if or .Values.serviceAccountToken.audience .Values.netrcSecret -}}
# A subPath mount wouldn't see kubelet rotate the token or update
# the Secret, so this mounts the whole directory.
- name: credentials
  mountPath: /var/run/secrets/nix-store-csi
  readOnly: true
{{- end }}
{{- end }}

{{- define "nix-store-csi.credentials" -}}
{{- if or .Values.serviceAccountToken.audience .Values.netrcSecret -}}
- name: credentials
  projected:
    sources:
      {{- with .Values.serviceAccountToken.audience }}
      - serviceAccountToken:
          audience: {{ . | quote }}
          path: token
      {{- end }}
      {{- with .Values.netrcSecret }}
      - secret:
          name: {{ . }}
          items:
            - key: netrc
              path: netrc
      {{- end }}
{{- end }}
{{- end }}

{{- /* What the pods of the node plugins and of the locality extender share. */}}
{{- define "nix-store-csi.podSpec" -}}
serviceAccountName: {{ .Release.Name }}
automountServiceAccountToken: false
{{- with .Values.imagePullSecrets }}
imagePullSecrets:
  {{- range . }}
  - name: {{ . }}
  {{- end }}
{{- end }}
hostNetwork: {{ .Values.hostNetwork }}
{{- if .Values.hostNetwork }}
# Lets pods on the host network resolve cluster names, like a cache's
# Service.
dnsPolicy: ClusterFirstWithHostNet
{{- end }}
{{- end }}

{{- define "nix-store-csi.cacheEnvFrom" -}}
{{- with .Values.s3CredentialsSecret -}}
envFrom:
  - secretRef:
      name: {{ . }}
{{- end }}
{{- end }}
