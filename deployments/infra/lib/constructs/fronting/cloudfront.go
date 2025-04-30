package fronting

import (
	"github.com/aws/aws-cdk-go/awscdk/v2/awsec2"
	"github.com/aws/constructs-go/constructs/v10"
)

// ──────────────────────────────────────────────────────────────────────────────
// When is **CloudFront** the right front-end?
//
//   - **Global audience / low RTT world-wide**
//     – Requests terminate at ~450 edge POPs; TLS handshake + first byte can be
//     50-150 ms faster for users far from your AWS Region.
//
//   - **Edge caching** for READ-heavy workloads with shared params/paths
//     – Static REST responses, large JSON snapshots, or Grafana tiles can be
//     cached for minutes to hours, shrinking EC2 egress and KGW / Indexer load.
//
//   - **Shield + WAF at the edge**
//     – Built-in DDoS protection (AWS Shield Standard) and cheaper per-request
//     WAF pricing compared with ALB.
//
// • **HTTP/3 & IPv6** delivered automatically.
//
//   - **Multi-origin / fail-over** rules
//     – You can set KGW as primary, Indexer as secondary, or different path
//     patterns → different origins without changing back-ends.
//
// Caveats:
//
//   - **Cost floor**
//     – You pay data-transfer-out from edge (~$0.085–0.14/GB) plus request fees.
//     For low-traffic, region-local APIs, HTTP API is much cheaper.
//
//   - **Added latency for in-region users**
//     – A user in us-east-2 hitting an us-east-1 edge may see +20 ms compared
//     with a direct Regional API call.

//   - **Deployment time** is also an issue
//     – It takes 10–15 minutes to deploy a CloudFront distribution.
//
// • **No WebSockets yet** – only HTTP(S).
//
//   - **Deploy complexity**
//     – Needs an ACM certificate **in us-east-1**, special S3 logging buckets,
//     and DNS aliases.
//
// TL;DR – choose CloudFront when you have **read-heavy public endpoints** with shared params/paths or a
// truly **global user-base** that benefits from edge POPs and caching. Keep
// HTTP API for dev / low-RPS stacks and ALB for high-throughput, VPC-local
// workloads.
// ──────────────────────────────────────────────────────────────────────────────
type cloudFront struct{}

// NewCloudFrontFronting returns a Fronting stub for CloudFront.
func NewCloudFrontFronting() Fronting {
	return &cloudFront{}
}

// AttachRoutes is not yet implemented for CloudFront and panics to indicate unimplemented.
func (c *cloudFront) AttachRoutes(scope constructs.Construct, id string, props *FrontingProps) FrontingResult {
	// TODO: create awscloudfront.Distribution, ApiMapping, and awsroute53.ARecord
	panic("CloudFront fronting not implemented yet")
}

// IngressRules declares the security-group ingress rules for CloudFront origin access.
func (c *cloudFront) IngressRules() []IngressSpec {
	// AWS-managed prefix list ID for CloudFront origins
	const cfPrefixList = "pl-68a54001"
	return []IngressSpec{
		{
			Protocol:    awsec2.Protocol_TCP,
			FromPort:    443,
			ToPort:      443,
			Source:      cfPrefixList,
			Description: "TLS from CloudFront",
		},
		{
			Protocol:    awsec2.Protocol_TCP,
			FromPort:    1337,
			ToPort:      1337,
			Source:      cfPrefixList,
			Description: "Indexer TLS traffic from CloudFront",
		},
	}
}
