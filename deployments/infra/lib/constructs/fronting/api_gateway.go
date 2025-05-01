package fronting

import (
	"fmt"

	"github.com/aws/aws-cdk-go/awscdk/v2/awsapigatewayv2"
	"github.com/aws/aws-cdk-go/awscdk/v2/awsapigatewayv2integrations"
	"github.com/aws/aws-cdk-go/awscdk/v2/awscertificatemanager"
	"github.com/aws/aws-cdk-go/awscdk/v2/awsec2"
	"github.com/aws/aws-cdk-go/awscdk/v2/awsroute53"
	"github.com/aws/aws-cdk-go/awscdk/v2/awsroute53targets"
	"github.com/aws/constructs-go/constructs/v10"
	"github.com/aws/jsii-runtime-go"
)

// ──────────────────────────────────────────────────────────────────────────────
// Why choose an **HTTP API Gateway** front-end?
//
//   - **Burst-friendly, pay-per-request pricing**                       – No hourly
//     or per-LCU charge. A dev or test stack that sleeps 95 % of the time costs
//     only a few US-cents per month, while still handling sudden traffic spikes.
//
//   - **Built-in features** you'd otherwise glue together:
//     – JWT / Cognito / Lambda authorizers out-of-the-box.
//     – Per-method throttling & quotas (Safeguards public RPC endpoints).
//     – Request validation, mapping, CORS, stage variables.
//
//   - **Automatic TLS** – uploads the ACM cert, handles renewals, SNI, ALPN.
//     No need for an HTTPS listener and security-group port 443 like on ALB.
//
//   - **Native WebSockets?**  Not yet in HTTP API (that's the REST/WebSocket API),
//     but for standard JSON/REST KGW & Indexer calls HTTP API is perfect.
//
//   - **Latency** – lives in-region, so 1–5 ms to VPC targets is typical.
//     Faster than routing through CloudFront if users are mostly in the same
//     region.
//
//   - **Simplest network path** – API GW → private link-local address of the VPC
//     endpoint you specify (our EC2 nodes' public IP in this case).
//
// When **not** to use it:
//
//   - **Hot production workloads with sustained 1000+ RPS** – at that point the
//     per-request price overtakes ALB's hourly + LCU model.
//
//   - **Long-lived or very large payloads** – body limit is 10 MB and idle
//     timeout 29 seconds. ALB goes to 100 MB / 4000 s.
//
// • **Global edge caching / static acceleration** – that's CloudFront's domain.
//
//   - **Layer-7 tricks (weighted routes, blue-green)** – ALB listener rules give
//     you more granular traffic shifting.
//
// TL;DR – keep HTTP API as the **default** front-end for KGW / Indexer when
// traffic is intermittent, you want turnkey auth/throttling, and TCO matters.
// Graduate to ALB for always-hot, high-RPS clusters or to CloudFront when
// latency for worldwide users is king.
// ──────────────────────────────────────────────────────────────────────────────
type apiGateway struct{}

func (a *apiGateway) AttachRoutes(scope constructs.Construct, id string, props *FrontingProps) FrontingResult {
	// Create HTTP API for this backend
	httpApi := awsapigatewayv2.NewHttpApi(scope, jsii.String(id+"HttpApi"), &awsapigatewayv2.HttpApiProps{
		ApiName: jsii.String(id + "HttpApi"),
	})

	// Validate and use the provided endpoint
	if props.Endpoint == nil || *props.Endpoint == "" {
		panic(fmt.Sprintf("Endpoint is required for apiGateway construct %s", id))
	}
	endpointUrl := "http://" + *props.Endpoint
	integration := awsapigatewayv2integrations.NewHttpUrlIntegration(
		jsii.String(id+"Integration"),
		jsii.String(endpointUrl),
		&awsapigatewayv2integrations.HttpUrlIntegrationProps{
			Method: awsapigatewayv2.HttpMethod_ANY,
			ParameterMapping: awsapigatewayv2.NewParameterMapping().
				AppendHeader(jsii.String("path"), awsapigatewayv2.MappingValue_ContextVariable(jsii.String("request.path"))),
		},
	)

	httpApi.AddRoutes(&awsapigatewayv2.AddRoutesOptions{
		Path:        jsii.String("/{proxy+}"),
		Methods:     &[]awsapigatewayv2.HttpMethod{awsapigatewayv2.HttpMethod_ANY},
		Integration: integration,
	})

	zoneName := props.HostedZone.ZoneName()
	if props.RecordName == nil || *props.RecordName == "" {
		panic(fmt.Sprintf("RecordName is required for apiGateway construct %s", id))
	}
	fqdn := *props.RecordName + "." + *zoneName

	var cert awscertificatemanager.ICertificate
	certMgr := NewCertManager()

	if props.ImportedCertificate != nil {
		cert = props.ImportedCertificate
	} else {
		// Issue new certificate for this domain
		certId := id + "Cert"
		cert = certMgr.GetRegional(scope, certId, props.HostedZone, fqdn, props.AdditionalSANs)
	}

	domainNameId := id + "DomainName"
	domainName := awsapigatewayv2.NewDomainName(scope, jsii.String(domainNameId), &awsapigatewayv2.DomainNameProps{
		DomainName:  jsii.String(fqdn),
		Certificate: cert,
	})

	apiMappingId := id + "ApiMapping"
	awsapigatewayv2.NewApiMapping(scope, jsii.String(apiMappingId), &awsapigatewayv2.ApiMappingProps{
		Api:        httpApi,
		DomainName: domainName,
	})

	aRecordId := id + "ARecord"
	awsroute53.NewARecord(scope, jsii.String(aRecordId), &awsroute53.ARecordProps{
		Zone:       props.HostedZone,
		RecordName: props.RecordName,
		Target: awsroute53.RecordTarget_FromAlias(
			awsroute53targets.NewApiGatewayv2DomainProperties(
				domainName.RegionalDomainName(),
				domainName.RegionalHostedZoneId(),
			),
		),
	})

	return FrontingResult{
		FQDN:        jsii.String(fqdn),
		Certificate: cert,
	}
}

// IngressRules returns the security-group ingress rules needed by the API Gateway fronting.
func (a *apiGateway) IngressRules() []IngressSpec {
	rules := []IngressSpec{
		{
			Protocol:    awsec2.Protocol_TCP,
			FromPort:    80,
			ToPort:      80,
			Source:      "0.0.0.0/0",
			Description: "Allow HTTP from API Gateway",
		},
	}

	return rules
}
