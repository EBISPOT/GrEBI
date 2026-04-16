{{- define "grebi.postgresSecretName" -}}
{{- default (printf "%s-postgres" .Release.Name) .Values.postgres.secret.name -}}
{{- end -}}
