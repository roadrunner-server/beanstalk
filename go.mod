module github.com/roadrunner-server/beanstalk/v5

go 1.22

toolchain go1.22.5

require (
	github.com/beanstalkd/go-beanstalk v0.2.0
	github.com/cenkalti/backoff/v4 v4.3.0
	github.com/goccy/go-json v0.10.3
	github.com/google/uuid v1.6.0
	github.com/roadrunner-server/api/v4 v4.16.0
	github.com/roadrunner-server/endure/v2 v2.4.6
	github.com/roadrunner-server/errors v1.4.1
	go.opentelemetry.io/contrib/propagators/jaeger v1.28.0
	go.opentelemetry.io/otel v1.28.0
	go.opentelemetry.io/otel/sdk v1.28.0
	go.opentelemetry.io/otel/trace v1.28.0
	go.uber.org/zap v1.27.0
)

exclude (
	github.com/spf13/viper v1.18.0
	github.com/spf13/viper v1.18.1
)

require (
	github.com/davecgh/go-spew v1.1.2-0.20180830191138-d8f796af33cc // indirect
	github.com/go-logr/logr v1.4.2 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/pmezard/go-difflib v1.0.1-0.20181226105442-5d4384ee4fb2 // indirect
	go.opentelemetry.io/otel/metric v1.28.0 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	golang.org/x/sys v0.22.0 // indirect
)
