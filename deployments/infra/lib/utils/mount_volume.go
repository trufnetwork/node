package utils

import (
	"github.com/aws/jsii-runtime-go"
)

func MountVolumeToPathAndPersist(volumeName string, path string) []*string {
	if volumeName == "" || path == "" {
		// we panic. cdk is build time only.
		panic("volumeName and path cannot be empty")
	}

	commands := jsii.Strings(
		fmt.Sprintf("sudo mkfs -t xfs /dev/%s", volumeName),
		fmt.Sprintf("sudo mkdir -p %s", path),
		fmt.Sprintf("sudo mount /dev/%s %s", volumeName, path),
		fmt.Sprintf("sudo chown ec2-user:ec2-user %s", path),
		fmt.Sprintf("echo '/dev/%s %s xfs defaults 0 0' | sudo tee -a /etc/fstab", volumeName, path),
	)

	return *commands
}

func MoveToPath(file string, path string) *string {
	if file == "" || path == "" {
		// we panic. cdk is build time only.
		panic("file and path cannot be empty")
	}

	command := jsii.String(fmt.Sprintf("sudo mv %s %s", file, path))
	return command
}
