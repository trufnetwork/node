# TRUF.NETWORK Deployment Options

Choose the deployment method that best fits your needs, experience level, and infrastructure requirements.

## Quick Comparison

| Feature | AMI Deployment | Manual Setup |
|---------|----------------|--------------|
| **Setup Time** | 5-10 minutes | 45-60 minutes |
| **Prerequisites** | AWS Account | Multiple tools (5+) |
| **Skill Level** | Beginner | Advanced |
| **Customization** | Basic options | Full control |
| **Production Ready** | ✅ Yes | ✅ Yes |
| **Cost** | EC2 instance cost | Server/VPS cost |
| **MCP Integration** | ✅ Pre-configured | Manual setup |
| **Auto-updates** | Manual | Manual |
| **Backup/Recovery** | AWS snapshots | Custom scripts |
| **Monitoring** | CloudWatch ready | Custom setup |
| **Security** | AWS security groups | Firewall config |

## Detailed Breakdown

### AMI Deployment (Recommended for Beginners)

**Best for**: New users, quick deployment, production environments

**Advantages**:
- Fastest setup time (5-10 minutes)
- Pre-configured with all dependencies
- Guided interactive setup
- MCP server ready for AI integration
- AWS infrastructure benefits (scaling, monitoring, backups)
- Minimal technical knowledge required
- Pre-tested configuration

**Use Cases**:
- Development and testing
- Production deployments on AWS
- Users new to blockchain nodes
- Teams wanting quick deployment

**Guide**: [AMI Deployment Guide](./ami-deployment-guide.md)

### Manual Setup (Full Control)

**Best for**: Advanced users, custom infrastructure, maximum flexibility

**Advantages**:
- Complete control over configuration
- Works on any Linux/macOS system
- Highly customizable
- No cloud provider dependency
- Deep understanding of system components
- Custom optimization possible

**Use Cases**:
- Custom server environments
- Specific compliance requirements
- Advanced users wanting full control
- Integration with existing infrastructure

**Guide**: [Node Operator Guide](./node-operator-guide.md)

## Decision Matrix

### Choose AMI if you:
- ✅ Want the fastest deployment
- ✅ Are new to running blockchain nodes
- ✅ Need production-ready setup quickly
- ✅ Want MCP server for AI integration
- ✅ Prefer guided configuration
- ✅ Are comfortable with AWS costs
- ✅ Want AWS infrastructure benefits

### Choose Manual Setup if you:
- ✅ Need maximum customization
- ✅ Have specific server requirements
- ✅ Want to understand all components
- ✅ Are running on non-AWS infrastructure
- ✅ Have advanced Linux/system administration skills
- ✅ Need compliance with specific requirements
- ✅ Want to minimize external dependencies

## Migration Between Options

### From Manual to AMI
1. Backup your `nodekey.json` (preserves node identity)
2. Export any custom configuration
3. Launch AMI instance following [AMI Guide](./ami-deployment-guide.md)
4. Import your node identity and custom settings

### From AMI to Manual
1. SSH into your AMI instance
2. Copy configuration files and node identity
3. Set up manual environment following [Manual Guide](./node-operator-guide.md)
4. Import your configuration

## Resource Requirements

### Minimum System Requirements
- **CPU**: 2 cores
- **RAM**: 4 GB
- **Storage**: 30 GB (50+ GB recommended)
- **Network**: Stable internet connection

## Support and Community

### Getting Help
- **GitHub**: [Issues and Discussions](https://github.com/trufnetwork/node/issues)
- **Documentation**: Comprehensive guides for each method

## Conclusion

**Quick Start?** → Choose **AMI Deployment**
**Maximum Control?** → Choose **Manual Setup**

Both methods will get you a fully functional TRUF.NETWORK node. The choice depends on your specific needs, technical expertise, and infrastructure preferences.

---

**Ready to deploy?** Pick your preferred method and follow the corresponding guide:
- [AMI Deployment Guide](./ami-deployment-guide.md)
- [Manual Setup Guide](./node-operator-guide.md)