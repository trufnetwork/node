package observer

// ssm tag is used by CreateStartObserverScript to get the parameters from SSM at startup
// they can be empty here if the params are set in the .env file
type ObserverParameters struct {
	PrometheusEndpoint *string `env:"GRAFANA_CLOUD_PROMETHEUS_ENDPOINT" ssm:"GRAFANA_CLOUD_PROMETHEUS_ENDPOINT"`
	PrometheusUser     *string `env:"GRAFANA_CLOUD_PROMETHEUS_USER" ssm:"GRAFANA_CLOUD_PROMETHEUS_USER"`
	PrometheusPassword *string `env:"GRAFANA_CLOUD_PROMETHEUS_PASSWORD" ssm:"GRAFANA_CLOUD_PROMETHEUS_PASSWORD,secure"`
	LogsDomain         *string `env:"GRAFANA_CLOUD_LOGS_DOMAIN" ssm:"GRAFANA_CLOUD_LOGS_DOMAIN"`
	LogsUser           *string `env:"GRAFANA_CLOUD_LOGS_USER" ssm:"GRAFANA_CLOUD_LOGS_USER"`
	LogsPassword       *string `env:"GRAFANA_CLOUD_LOGS_PASSWORD" ssm:"GRAFANA_CLOUD_LOGS_PASSWORD,secure"`
	InstanceName       *string `env:"INSTANCE_NAME"` // not set by ssm
	ServiceName        *string `env:"SERVICE_NAME"`
	Env                *string `env:"ENV"`
}
