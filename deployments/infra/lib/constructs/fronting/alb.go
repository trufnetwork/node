package fronting

import (
	"github.com/aws/aws-cdk-go/awscdk/v2/awsec2"
	"github.com/aws/constructs-go/constructs/v10"
)

// ──────────────────────────────────────────────────────────────────────────────
// Why would you ever pick an ALB for front-ending KGW / Indexer?
//
//   - High, steady throughput (≫ 1 M req/mo)                     – ALB is priced
//     per-hour + per-LCU; once traffic is above a few hundred RPS it is cheaper
//     than API Gateway's purely per-request model.
//
//   - Layer-7 routing features you can't (easily) get elsewhere:
//     – Native WebSockets / HTTP/2 (including gRPC pass-through)
//     – Weighted, header- or path-based rules to shift traffic between blue/green
//     KGW deployments, run canaries, or serve a public RPC               easily.
//     – Sticky sessions if you later bolt auth onto KGW.
//
// • WAF on a budget – AWS WAF on an ALB is pennies compared to WAF v2 on API GW.
//
//   - VPC-local back-ends – ALB can send traffic straight to private IP targets
//     (ECS, EKS, EC2) without the public exposure that HTTP API needs.
//
//   - Large payloads / long-lived connections – no 10 MB body limit; idle timeout
//     is 4000 s vs. 29 s on API GW.
//
// When **not** to use it:
//
//   - Low-traffic dev stacks – you still pay ~US$ 0.022/hr (≈ $16/mo) + LCUs even
//     when idle; HTTP API is "scale-to-zero" and costs cents for sporadic calls.
//
//   - Need for global edge caching – CloudFront (or simply two regional APIs) is
//     better if lat-latency world-wide is important.
//
//   - Turn-key IAM/OIDC auth, throttling, request validation – API Gateway gives
//     those out-of-the-box, ALB needs extra services or custom code.
//
// TL;DR – pick ALB when you own a **busy**, VPC-resident service that benefits
// from advanced Layer-7 routing or WebSockets, and the steady hourly cost is
// acceptable. Keep HTTP API for "burst-y", low-volume stacks or when you value
// its built-in auth / throttling / pay-per-use model.
// ──────────────────────────────────────────────────────────────────────────────
type albFronting struct{}

// NewAlbFronting returns a Fronting stub for ALB.
func NewAlbFronting() Fronting {
	return &albFronting{}
}

// AttachRoutes is not yet implemented for ALB fronting.
// It panics to indicate ALB fronting is unimplemented, conforming to the Fronting interface.
func (a *albFronting) AttachRoutes(scope constructs.Construct, id string, props *FrontingProps) FrontingResult {
	// TODO: create awselasticloadbalancingv2.ApplicationLoadBalancer,
	//       add HTTP/HTTPS listeners, configure target groups for KGW and Indexer,
	//       map custom domain with props.RecordName + ZoneName(), and
	//       return the DNS name of the ALB.
	panic("ALB fronting not implemented yet")
}

// IngressRules declares the security-group ingress rules for ALB origin access.
func (a *albFronting) IngressRules() []IngressSpec {
	// ALB exposes standard HTTP and HTTPS ports publicly.
	return []IngressSpec{
		{
			Protocol:    awsec2.Protocol_TCP,
			FromPort:    80,
			ToPort:      80,
			Source:      "0.0.0.0/0",
			Description: "HTTP from clients via ALB",
		},
		{
			Protocol:    awsec2.Protocol_TCP,
			FromPort:    443,
			ToPort:      443,
			Source:      "0.0.0.0/0",
			Description: "HTTPS from clients via ALB",
		},
	}
}
