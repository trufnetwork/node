package kwil_gateway

import (
	"github.com/aws/aws-cdk-go/awscdk/v2"
	"github.com/aws/aws-cdk-go/awscdk/v2/awsec2"
	"github.com/aws/aws-cdk-go/awscdk/v2/awsiam"
	"github.com/aws/jsii-runtime-go"
)

// Note: we currently DON'T use Nitro Enclave in our infrastructure. However, it's very likely that the current approach
// of using other means for SSL certificates may change.

// Nitro enclave requires 4cpu and 8GB of memory
// they wouldn't be the best choice for a small instance
// however we will preserve the ability to use them on production

func SetupNitroEnclaveService(instance awsec2.Instance) {
	nitroEnclaveInstallScript := `#!/bin/bash
# Install the AWS Nitro Enclaves CLI, to be able to use the ACM agent
# for certificate management with nginx
sudo amazon-linux-extras enable aws-nitro-enclaves-cli
sudo yum install aws-nitro-enclaves-acm -y


# Start the ACM agent
systemctl enable nitro-enclaves-acm.service
systemctl start nitro-enclaves-acm.service`
	instance.AddUserData(jsii.String(nitroEnclaveInstallScript))
}

// AssociateEnclaveCertificateToInstanceIamRole Associate an AWS Nitro Enclaves certificate with an AWS Identity and Access Management (IAM) role.
// See https://docs.aws.amazon.com/enclaves/latest/user/nitro-enclave-refapp.html#install-acm
func AssociateEnclaveCertificateToInstanceIamRole(stack awscdk.Stack, certificateArn string, role awsiam.IRole) awsec2.CfnEnclaveCertificateIamRoleAssociation {

	association := awsec2.NewCfnEnclaveCertificateIamRoleAssociation(stack, jsii.String("EnclaveCertificateIamRoleAssociation"), &awsec2.CfnEnclaveCertificateIamRoleAssociationProps{
		CertificateArn: jsii.String(certificateArn),
		RoleArn:        jsii.String(*role.RoleArn()),
	})

	bucketName := association.AttrCertificateS3BucketName()
	encryptionKmsKeyId := association.AttrEncryptionKmsKeyId()

	policy := awsiam.NewPolicy(stack, jsii.String("EnclaveCertificateIamRolePolicy"), &awsiam.PolicyProps{
		Statements: &[]awsiam.PolicyStatement{
			awsiam.NewPolicyStatement(&awsiam.PolicyStatementProps{
				Effect: awsiam.Effect_ALLOW,
			}),
			awsiam.NewPolicyStatement(&awsiam.PolicyStatementProps{
				Effect: awsiam.Effect_ALLOW,
				Actions: &[]*string{
					jsii.String("s3:GetObject"),
				},
				Resources: &[]*string{
					jsii.String("arn:aws:s3:::" + *bucketName + "/*"),
				},
			}),
			awsiam.NewPolicyStatement(&awsiam.PolicyStatementProps{
				Sid:    jsii.String("VisualEditor0"),
				Effect: awsiam.Effect_ALLOW,
				Actions: &[]*string{
					jsii.String("kms:Decrypt"),
				},
				Resources: &[]*string{
					jsii.String("arn:aws:kms:" + *stack.Region() + ":*:" + *encryptionKmsKeyId),
				},
			}),
			awsiam.NewPolicyStatement(&awsiam.PolicyStatementProps{
				Effect: awsiam.Effect_ALLOW,
				Actions: &[]*string{
					jsii.String("iam:GetRole"),
				},
				Resources: &[]*string{
					role.RoleArn(),
				},
			}),
		},
	})

	policy.AttachToRole(role)

	return association
}
