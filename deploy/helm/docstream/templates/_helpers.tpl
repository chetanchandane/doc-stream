{{/* Common naming and label helpers. */}}

{{- define "docstream.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "docstream.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name (include "docstream.name" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}

{{- define "docstream.labels" -}}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" }}
app.kubernetes.io/name: {{ include "docstream.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/part-of: docstream
{{- end -}}

{{/* Per-component selector labels. Call with (dict "ctx" . "component" "gateway") */}}
{{- define "docstream.selectorLabels" -}}
app.kubernetes.io/name: {{ include "docstream.name" .ctx }}
app.kubernetes.io/instance: {{ .ctx.Release.Name }}
app.kubernetes.io/component: {{ .component }}
{{- end -}}

{{/*
Pod-template labels: the selector labels PLUS part-of.

Kept separate from selectorLabels on purpose. A Deployment's spec.selector is
IMMUTABLE, so adding a label there would break `helm upgrade` on an existing
release. Pod templates may carry extra labels beyond the selector, which is
where part-of belongs — it's what `kubectl logs -l app.kubernetes.io/part-of`
and the CI restart-count assertion select on.

Call with (dict "ctx" . "component" "gateway").
*/}}
{{- define "docstream.podLabels" -}}
{{ include "docstream.selectorLabels" . }}
app.kubernetes.io/part-of: docstream
{{- end -}}

{{- define "docstream.secretName" -}}
{{- if .Values.secrets.existingSecret -}}
{{- .Values.secrets.existingSecret -}}
{{- else -}}
{{- printf "%s-secrets" (include "docstream.fullname" .) -}}
{{- end -}}
{{- end -}}

{{- define "docstream.configMapName" -}}
{{- printf "%s-config" (include "docstream.fullname" .) -}}
{{- end -}}

{{/*
Infra endpoints. When the bundled dev infra is enabled we point at in-cluster
Services; otherwise the operator supplies external endpoints.
*/}}
{{- define "docstream.kafkaBootstrap" -}}
{{- if .Values.infra.enabled -}}
{{- printf "%s-kafka:9092" (include "docstream.fullname" .) -}}
{{- else -}}
{{- required "external.kafkaBootstrap is required when infra.enabled=false" .Values.external.kafkaBootstrap -}}
{{- end -}}
{{- end -}}

{{- define "docstream.qdrantUrl" -}}
{{- if .Values.infra.enabled -}}
{{- printf "http://%s-qdrant:6333" (include "docstream.fullname" .) -}}
{{- else -}}
{{- required "external.qdrantUrl is required when infra.enabled=false" .Values.external.qdrantUrl -}}
{{- end -}}
{{- end -}}

{{- define "docstream.storageEndpoint" -}}
{{- if .Values.infra.enabled -}}
{{- printf "http://%s-minio:9000" (include "docstream.fullname" .) -}}
{{- else -}}
{{- .Values.external.storageEndpoint -}}
{{- end -}}
{{- end -}}

{{/* Postgres host only; the full DSN is assembled in the env block so the
     password can come from the Secret rather than being baked into a string. */}}
{{- define "docstream.postgresHost" -}}
{{- printf "%s-postgres" (include "docstream.fullname" .) -}}
{{- end -}}

{{/*
Dependency-wait init containers.

Kubernetes has no ordering primitive between resources, so anything that needs
Postgres or Kafka to exist first polls for it. Cheap, restart-safe, and the
standard alternative to compose's depends_on.

Only emitted when infra.enabled — with external managed services the endpoints
are assumed to be up already, and parsing an arbitrary DSN here would be fragile.
*/}}
Each helper emits the whole ``initContainers:`` key, so when infra is disabled
the block vanishes cleanly instead of leaving a dangling empty field.
*/}}
{{- define "docstream.waitForPostgres" -}}
{{- if .Values.infra.enabled }}
initContainers:
  - name: wait-for-postgres
    image: busybox:1.36
    command: ["sh", "-c", "until nc -z {{ include "docstream.postgresHost" . }} 5432; do echo waiting for postgres; sleep 2; done"]
{{- end }}
{{- end -}}

{{- define "docstream.waitForKafka" -}}
{{- if .Values.infra.enabled }}
initContainers:
  - name: wait-for-kafka
    image: busybox:1.36
    command: ["sh", "-c", "until nc -z {{ include "docstream.fullname" . }}-kafka 9092; do echo waiting for kafka; sleep 2; done"]
{{- end }}
{{- end -}}

{{/*
All four dependencies, for the application pods.

Postgres and Kafka are the obvious ones, but MinIO and Qdrant matter just as
much: every service calls get_storage() -> ensure_bucket() against MinIO on
startup, and the enrichment worker and query API also reach Qdrant. Gating on
only Postgres and Kafka let pods start too early and crash-loop until the other
two happened to be up — which read like a resource problem but was really a
missing dependency gate.
*/}}
{{- define "docstream.waitForInfra" -}}
{{- if .Values.infra.enabled }}
initContainers:
  - name: wait-for-postgres
    image: busybox:1.36
    command: ["sh", "-c", "until nc -z {{ include "docstream.postgresHost" . }} 5432; do echo waiting for postgres; sleep 2; done"]
  - name: wait-for-kafka
    image: busybox:1.36
    command: ["sh", "-c", "until nc -z {{ include "docstream.fullname" . }}-kafka 9092; do echo waiting for kafka; sleep 2; done"]
  - name: wait-for-minio
    image: busybox:1.36
    command: ["sh", "-c", "until nc -z {{ include "docstream.fullname" . }}-minio 9000; do echo waiting for minio; sleep 2; done"]
  - name: wait-for-qdrant
    image: busybox:1.36
    command: ["sh", "-c", "until nc -z {{ include "docstream.fullname" . }}-qdrant 6333; do echo waiting for qdrant; sleep 2; done"]
{{- end }}
{{- end -}}

{{/*
The env block every service shares. Non-secret values come from the ConfigMap,
credentials from the Secret — so rotating a key never requires a chart change.
*/}}
{{- define "docstream.env" -}}
- name: DOCSTREAM_ENV
  valueFrom:
    configMapKeyRef: { name: {{ include "docstream.configMapName" . }}, key: env }
- name: DOCSTREAM_LOG_LEVEL
  valueFrom:
    configMapKeyRef: { name: {{ include "docstream.configMapName" . }}, key: logLevel }
- name: DOCSTREAM_KAFKA__BOOTSTRAP_SERVERS
  valueFrom:
    configMapKeyRef: { name: {{ include "docstream.configMapName" . }}, key: kafkaBootstrap }
- name: DOCSTREAM_QDRANT__URL
  valueFrom:
    configMapKeyRef: { name: {{ include "docstream.configMapName" . }}, key: qdrantUrl }
- name: DOCSTREAM_QDRANT__COLLECTION
  valueFrom:
    configMapKeyRef: { name: {{ include "docstream.configMapName" . }}, key: qdrantCollection }
- name: DOCSTREAM_QDRANT__VECTOR_SIZE
  valueFrom:
    configMapKeyRef: { name: {{ include "docstream.configMapName" . }}, key: qdrantVectorSize }
- name: DOCSTREAM_EMBEDDING__MODEL
  valueFrom:
    configMapKeyRef: { name: {{ include "docstream.configMapName" . }}, key: embeddingModel }
- name: DOCSTREAM_EMBEDDING__DIM
  valueFrom:
    configMapKeyRef: { name: {{ include "docstream.configMapName" . }}, key: embeddingDim }
- name: DOCSTREAM_EMBEDDING__CHUNK_SIZE
  valueFrom:
    configMapKeyRef: { name: {{ include "docstream.configMapName" . }}, key: embeddingChunkSize }
- name: DOCSTREAM_EMBEDDING__CHUNK_OVERLAP
  valueFrom:
    configMapKeyRef: { name: {{ include "docstream.configMapName" . }}, key: embeddingChunkOverlap }
- name: DOCSTREAM_LLM__MODEL
  valueFrom:
    configMapKeyRef: { name: {{ include "docstream.configMapName" . }}, key: llmModel }
- name: DOCSTREAM_LLM__MAX_TOKENS
  valueFrom:
    configMapKeyRef: { name: {{ include "docstream.configMapName" . }}, key: llmMaxTokens }
- name: DOCSTREAM_CONSUMER__MAX_ATTEMPTS
  valueFrom:
    configMapKeyRef: { name: {{ include "docstream.configMapName" . }}, key: consumerMaxAttempts }
- name: DOCSTREAM_CONSUMER__BACKOFF_SECONDS
  valueFrom:
    configMapKeyRef: { name: {{ include "docstream.configMapName" . }}, key: consumerBackoffSeconds }
- name: DOCSTREAM_QUERY__MIN_SCORE
  valueFrom:
    configMapKeyRef: { name: {{ include "docstream.configMapName" . }}, key: queryMinScore }
- name: DOCSTREAM_QUERY__RELATIVE_CUTOFF
  valueFrom:
    configMapKeyRef: { name: {{ include "docstream.configMapName" . }}, key: queryRelativeCutoff }
- name: DOCSTREAM_STORAGE__BACKEND
  value: "s3"
- name: DOCSTREAM_STORAGE__BUCKET
  valueFrom:
    configMapKeyRef: { name: {{ include "docstream.configMapName" . }}, key: storageBucket }
- name: DOCSTREAM_STORAGE__ENDPOINT_URL
  valueFrom:
    configMapKeyRef: { name: {{ include "docstream.configMapName" . }}, key: storageEndpoint }
# --- credentials ---
- name: POSTGRES_PASSWORD
  valueFrom:
    secretKeyRef: { name: {{ include "docstream.secretName" . }}, key: postgresPassword }
- name: DOCSTREAM_POSTGRES__DSN
{{- if .Values.infra.enabled }}
  value: "postgresql+asyncpg://{{ .Values.infra.postgres.username }}:$(POSTGRES_PASSWORD)@{{ include "docstream.postgresHost" . }}:5432/{{ .Values.infra.postgres.database }}"
{{- else }}
  value: {{ required "external.postgresDsn is required when infra.enabled=false" .Values.external.postgresDsn | quote }}
{{- end }}
- name: DOCSTREAM_STORAGE__ACCESS_KEY
  valueFrom:
    secretKeyRef: { name: {{ include "docstream.secretName" . }}, key: storageAccessKey }
- name: DOCSTREAM_STORAGE__SECRET_KEY
  valueFrom:
    secretKeyRef: { name: {{ include "docstream.secretName" . }}, key: storageSecretKey }
- name: DOCSTREAM_EMBEDDING__API_KEY
  valueFrom:
    secretKeyRef: { name: {{ include "docstream.secretName" . }}, key: embeddingApiKey }
- name: DOCSTREAM_LLM__API_KEY
  valueFrom:
    secretKeyRef: { name: {{ include "docstream.secretName" . }}, key: llmApiKey }
{{- end -}}
