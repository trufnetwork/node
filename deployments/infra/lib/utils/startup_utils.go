// File: lib/utils/startup_utils.go

package utils

// InstallDockerScript returns the script to install and setup Docker
func InstallDockerScript() string {
	return `
# Update the system
yum update -y

# Install Docker
amazon-linux-extras install docker

# Start Docker and enable it to start at boot
systemctl start docker
systemctl enable docker

# Add the ec2-user to the docker group (ec2-user is the default user in Amazon Linux 2)
usermod -aG docker ec2-user

# reload the group
newgrp docker

mkdir -p /usr/local/lib/docker/cli-plugins/
curl -SL https://github.com/docker/compose/releases/latest/download/docker-compose-linux-x86_64 -o /usr/local/lib/docker/cli-plugins/docker-compose
chmod a+x /usr/local/lib/docker/cli-plugins/docker-compose
`
}

// CreateSystemdServiceScript creates a systemd service file
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

// UnzipFileScript returns a script to unzip a file
func UnzipFileScript(zipPath, destPath string) string {
	return "unzip " + zipPath + " -d " + destPath
}

// ConfigureDockerDataRoot makes the docker data directory live on the given directory
// it should be called before pulling any image
// - stop docker service
// - add /etc/docker/daemon.json with data-root set to the given directory
// - start docker service

func ConfigureDockerDataRoot(directory string) string {
	return `
systemctl stop docker

cat <<EOF > /etc/docker/daemon.json
{
  "data-root": "` + directory + `"
}
EOF

systemctl start docker
`
}
