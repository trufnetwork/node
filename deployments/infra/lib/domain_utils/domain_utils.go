package domain_utils

import (
	"github.com/aws/aws-cdk-go/awscdk/v2"
	"github.com/aws/aws-cdk-go/awscdk/v2/awscertificatemanager"
	"github.com/aws/aws-cdk-go/awscdk/v2/awsec2"
	"github.com/aws/aws-cdk-go/awscdk/v2/awsiam"
	"github.com/aws/aws-cdk-go/awscdk/v2/awsroute53"
	"github.com/aws/jsii-runtime-go"
)

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

func CreateDomainRecords(stack awscdk.Stack, domain *string, hostedZone *awsroute53.IHostedZone, publicIp *string) awsroute53.ARecord {
	// Create Route53 record.
	return awsroute53.NewARecord(stack, jsii.String("ARecord"), &awsroute53.ARecordProps{
		Zone:       *hostedZone,
		RecordName: jsii.String(*domain),
		Target:     awsroute53.RecordTarget_FromIpAddresses(publicIp),
		Ttl:        awscdk.Duration_Minutes(jsii.Number(5)),
	})
}

func GetACMCertificate(stack awscdk.Stack, domain *string, hostedZone *awsroute53.IHostedZone) awscertificatemanager.Certificate {
	id := awscdk.Fn_Join(jsii.String("-"), &[]*string{domain, jsii.String("ACM-Certificate")})
	// Create ACM certificate.
	return awscertificatemanager.NewCertificate(stack, id, &awscertificatemanager.CertificateProps{
		DomainName: domain,
		Validation: awscertificatemanager.CertificateValidation_FromDns(*hostedZone),
	})
}

const MainDomain = "tsn.test.truflation.com"

func GetTSNHostedZone(stack awscdk.Stack) awsroute53.IHostedZone {
	return awsroute53.HostedZone_FromLookup(stack, jsii.String("HostedZone"), &awsroute53.HostedZoneProviderProps{
		DomainName: jsii.String(MainDomain),
	})
}
