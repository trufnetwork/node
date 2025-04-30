package utils

import (
	"fmt"

	"github.com/trufnetwork/node/infra/scripts/renderer"
)

// InstallDockerScript renders the script to install Docker and Docker Compose
// using the TplInstallDocker template.
// It returns an error if the template rendering fails.
func InstallDockerScript() (string, error) {
	script, err := renderer.Render(renderer.TplInstallDocker, nil)
	if err != nil {
		return "", fmt.Errorf("render %s: %w", renderer.TplInstallDocker, err)
	}
	return script, nil
}

// CreateSystemdServiceScript creates a systemd service file content.
// NOTE: This helper was NOT refactored to use templates in this pass.
// It still uses string concatenation.
func CreateSystemdServiceScript(
	serviceName, description, startCommand, stopCommand string,
	envVars map[string]string,
) string {
	envString := GetEnvStringsForService(envVars)

	return `
cat <<EOF > /etc/systemd/system/` + serviceName + `.service
[Unit]
Description=` + description + `
Restart=on-failure

[Service]
Type=oneshot
RemainAfterExit=yes
ExecStart=` + startCommand + `
ExecStop=` + stopCommand + `
` + envString + `

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable ` + serviceName + `.service
systemctl start ` + serviceName + `.service
`
}

// UnzipFileScript returns a simple shell command to unzip a file.
// NOTE: Not template based.
func UnzipFileScript(zipPath, destPath string) string {
	return "unzip " + zipPath + " -d " + destPath
}

// ConfigureDockerInput defines the input for configuring the Docker daemon.
// Corresponds to the data needed by TplConfigureDocker template.
type ConfigureDockerInput struct {
	DataRoot    *string `json:"data-root"`
	MetricsAddr *string `json:"metrics-addr"`
}

// ConfigureDocker renders the script to configure the Docker daemon
// using the TplConfigureDocker template and the provided input.
// It returns an error if the template rendering fails.
func ConfigureDocker(input ConfigureDockerInput) (string, error) {
	script, err := renderer.Render(renderer.TplConfigureDocker, input)
	if err != nil {
		return "", fmt.Errorf("render %s: %w", renderer.TplConfigureDocker, err)
	}
	return script, nil
}
