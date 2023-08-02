module github.com/roadrunner-server/beanstalk/v4

go 1.20

require (
	github.com/beanstalkd/go-beanstalk v0.2.0
	github.com/cenkalti/backoff/v4 v4.2.1
	github.com/goccy/go-json v0.10.2
	github.com/google/uuid v1.3.0
	github.com/roadrunner-server/api/v4 v4.5.0
	github.com/roadrunner-server/endure/v2 v2.3.0
	github.com/roadrunner-server/errors v1.2.0
	github.com/roadrunner-server/sdk/v4 v4.3.1
	go.opentelemetry.io/contrib/propagators/jaeger v1.17.0
	go.opentelemetry.io/otel v1.16.0
	go.opentelemetry.io/otel/sdk v1.16.0
	go.opentelemetry.io/otel/trace v1.16.0
	go.uber.org/zap v1.25.0
)

require (
	github.com/go-logr/logr v1.2.4 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/roadrunner-server/tcplisten v1.3.0 // indirect
	go.opentelemetry.io/otel/metric v1.16.0 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	golang.org/x/sys v0.10.0 // indirect
)
