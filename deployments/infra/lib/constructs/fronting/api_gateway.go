package fronting

import (
	"fmt"
	"strings"

	"github.com/aws/aws-cdk-go/awscdk/v2/awsapigatewayv2"
	"github.com/aws/aws-cdk-go/awscdk/v2/awsapigatewayv2integrations"
	"github.com/aws/aws-cdk-go/awscdk/v2/awscertificatemanager"
	"github.com/aws/aws-cdk-go/awscdk/v2/awsec2"
	"github.com/aws/aws-cdk-go/awscdk/v2/awsroute53"
	"github.com/aws/aws-cdk-go/awscdk/v2/awsroute53targets"
	"github.com/aws/constructs-go/constructs/v10"
	"github.com/aws/jsii-runtime-go"
	"github.com/trufnetwork/node/infra/lib/cdklogger"
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
	httpApiConstructID := id + "HttpApi"
	httpApi := awsapigatewayv2.NewHttpApi(scope, jsii.String(httpApiConstructID), &awsapigatewayv2.HttpApiProps{
		ApiName: jsii.String(httpApiConstructID),
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
				AppendHeader(jsii.String("path"), awsapigatewayv2.MappingValue_ContextVariable(jsii.String("request.path"))).
				OverwritePath(awsapigatewayv2.MappingValue_RequestPath()),
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

	var cert awscertificatemanager.ICertificate
	certConstructID := id + "Cert"

	// The fqdn for the API GW DomainName, used in logging
	apiGwFqdnForLog := *props.RecordName + "." + *zoneName

	if props.ImportedCertificate != nil {
		cert = props.ImportedCertificate
		msg := fmt.Sprintf("Importing certificate %s for API Gateway domain %s.", *cert.CertificateArn(), apiGwFqdnForLog)
		cdklogger.LogInfo(scope, id, msg)
	} else {
		// Validate required props for certificate issuance
		if props.ValidationMethod == nil {
			panic(fmt.Sprintf("ValidationMethod is required in FrontingProps for %s when ImportedCertificate is nil", id))
		}
		if props.PrimaryDomainName == nil || *props.PrimaryDomainName == "" {
			panic(fmt.Sprintf("PrimaryDomainName is required in FrontingProps for %s when ImportedCertificate is nil", id))
		}

		// Log certificate issuance details
		sanStrings := []string{}
		if props.SubjectAlternativeNames != nil {
			for _, sanPtr := range props.SubjectAlternativeNames {
				if sanPtr != nil {
					sanStrings = append(sanStrings, *sanPtr)
				}
			}
		}
		var validationDetail string
		if dnsValidationProps, ok := props.ValidationMethod.(interface{ GetHostedZone() awsroute53.IHostedZone }); ok && dnsValidationProps.GetHostedZone() != nil {
			validationDetail = fmt.Sprintf("DNS validation in zone %s", *dnsValidationProps.GetHostedZone().ZoneName())
		} else {
			validationDetail = "(details not automatically extractable for log)"
		}
		msg := fmt.Sprintf("Issuing new certificate for %s with SANs: [%s] using %s.", *props.PrimaryDomainName, strings.Join(sanStrings, ", "), validationDetail)
		cdklogger.LogInfo(scope, id, msg)

		// Issue a new certificate with specified validation method
		certProps := &awscertificatemanager.CertificateProps{
			DomainName: props.PrimaryDomainName,
			Validation: props.ValidationMethod,
		}
		if props.SubjectAlternativeNames != nil {
			certProps.SubjectAlternativeNames = &props.SubjectAlternativeNames
		}
		cert = awscertificatemanager.NewCertificate(scope, jsii.String(certConstructID), certProps)
	}

	// The fqdn for the API GW DomainName should still be derived from props.RecordName + props.HostedZone.ZoneName()
	apiGwFqdn := *props.RecordName + "." + *zoneName
	domainNameId := id + "DomainName"
	domainName := awsapigatewayv2.NewDomainName(scope, jsii.String(domainNameId), &awsapigatewayv2.DomainNameProps{
		DomainName:  jsii.String(apiGwFqdn),
		Certificate: cert,
	})

	apiMappingId := id + "ApiMapping"
	awsapigatewayv2.NewApiMapping(scope, jsii.String(apiMappingId), &awsapigatewayv2.ApiMappingProps{
		Api:        httpApi,
		DomainName: domainName,
	})

	// Log HTTP API creation and mapping
	apiIDStr := "[Not Available]"
	if httpApi.ApiId() != nil {
		apiIDStr = *httpApi.ApiId()
	}
	msg := fmt.Sprintf("Created HTTP API %s. Mapped to custom domain: %s via DomainName %s and ApiMapping %s.", apiIDStr, apiGwFqdn, *domainName.Name(), apiMappingId)
	cdklogger.LogInfo(scope, id, msg)

	// Create the alias target properties from the domainName construct
	aliasTargetProps := awsroute53targets.NewApiGatewayv2DomainProperties(
		domainName.RegionalDomainName(),
		domainName.RegionalHostedZoneId(),
	)

	aRecordId := id + "ARecord"
	awsroute53.NewARecord(scope, jsii.String(aRecordId), &awsroute53.ARecordProps{
		Zone:       props.HostedZone,
		RecordName: props.RecordName,
		Target:     awsroute53.RecordTarget_FromAlias(aliasTargetProps),
	})

	// Log A record creation
	msgARecord := fmt.Sprintf("[APISetup 3/3] Created Route53 A Record '%s' in zone '%s' targeting API Gateway regional domain '%s'.", *props.RecordName, *props.HostedZone.ZoneName(), *domainName.RegionalDomainName())
	cdklogger.LogInfo(scope, id, msgARecord)

	return FrontingResult{
		FQDN:        jsii.String(apiGwFqdn),
		Certificate: cert,
		AliasTarget: aliasTargetProps,
		Api:         httpApi,
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
