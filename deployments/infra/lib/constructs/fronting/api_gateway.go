package fronting

import (
	"github.com/aws/aws-cdk-go/awscdk/v2/awsapigatewayv2"
	"github.com/aws/aws-cdk-go/awscdk/v2/awsapigatewayv2integrations"
	"github.com/aws/aws-cdk-go/awscdk/v2/awscertificatemanager"
	"github.com/aws/aws-cdk-go/awscdk/v2/awsec2"
	"github.com/aws/aws-cdk-go/awscdk/v2/awsroute53"
	"github.com/aws/aws-cdk-go/awscdk/v2/awsroute53targets"
	"github.com/aws/constructs-go/constructs/v10"
	"github.com/aws/jsii-runtime-go"
)

type apiGateway struct{}

// NewApiGatewayFronting returns a Fronting implemented via HTTP API.
func NewApiGatewayFronting() Fronting {
	return &apiGateway{}
}

func (a *apiGateway) AttachRoutes(scope constructs.Construct, id string, props *FrontingProps) FrontingResult {
	// 1. Create HTTP API
	httpApi := awsapigatewayv2.NewHttpApi(scope, jsii.String(id+"HttpApi"), nil)

	// 2. Integrations for KGW and Indexer
	kgwInt := awsapigatewayv2integrations.NewHttpUrlIntegration(
		jsii.String(id+"KGWInt"),
		jsii.String("http://"+*props.KGWEndpoint),
		nil,
	)
	idxInt := awsapigatewayv2integrations.NewHttpUrlIntegration(
		jsii.String(id+"IdxInt"),
		jsii.String("http://"+*props.IndexerEndpoint),
		nil,
	)

	// 3. Routes
	httpApi.AddRoutes(&awsapigatewayv2.AddRoutesOptions{
		Path:        jsii.String("/indexer/{proxy+}"),
		Methods:     &[]awsapigatewayv2.HttpMethod{awsapigatewayv2.HttpMethod_ANY},
		Integration: idxInt,
	})
	httpApi.AddRoutes(&awsapigatewayv2.AddRoutesOptions{
		Path:        jsii.String("/{proxy+}"),
		Methods:     &[]awsapigatewayv2.HttpMethod{awsapigatewayv2.HttpMethod_ANY},
		Integration: kgwInt,
	})

	// 4. Determine certificate: use imported if provided, else issue regionally
	// Full FQDN: recordName.zoneName
	fqdn := *props.RecordName + "." + *props.HostedZone.ZoneName()
	var cert awscertificatemanager.ICertificate
	if props.ImportedCertificate != nil {
		cert = props.ImportedCertificate
	} else {
		cm := NewCertManager()
		cert = cm.GetRegional(scope, id+"Cert", props.HostedZone, fqdn, props.AdditionalSANs)
	}

	// 5. Custom Domain & Mapping
	domain := awsapigatewayv2.NewDomainName(scope, jsii.String(id+"Domain"), &awsapigatewayv2.DomainNameProps{
		DomainName:  jsii.String(fqdn),
		Certificate: cert,
	})
	awsapigatewayv2.NewApiMapping(scope, jsii.String(id+"Mapping"), &awsapigatewayv2.ApiMappingProps{
		Api:        httpApi,
		DomainName: domain,
		Stage:      httpApi.DefaultStage(),
	})

	// 6. Route53 A Record
	awsroute53.NewARecord(scope, jsii.String(id+"Alias"), &awsroute53.ARecordProps{
		Zone:       props.HostedZone,
		RecordName: jsii.String(*props.RecordName),
		Target: awsroute53.RecordTarget_FromAlias(awsroute53targets.NewApiGatewayv2DomainProperties(
			domain.RegionalDomainName(), domain.RegionalHostedZoneId())),
	})

	// Return both the public FQDN and the certificate used
	return FrontingResult{
		FQDN:        jsii.String(fqdn),
		Certificate: cert,
	}
}

// IngressRules returns the security-group ingress rules needed by the API Gateway fronting.
func (a *apiGateway) IngressRules() []IngressSpec {
	return []IngressSpec{
		{
			Protocol:    awsec2.Protocol_TCP,
			FromPort:    80,
			ToPort:      80,
			Source:      "0.0.0.0/0",
			Description: "HTTP from API Gateway",
		},
		{
			Protocol:    awsec2.Protocol_TCP,
			FromPort:    1337,
			ToPort:      1337,
			Source:      "0.0.0.0/0",
			Description: "HTTP from API Gateway to Indexer",
		},
	}
}
