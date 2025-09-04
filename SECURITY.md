 # Security Policy
 
 We take the security of spark-fuse seriously. If you believe you have found a vulnerability, please follow the guidance below.
 
 ## Reporting a Vulnerability
 - Preferred: Open a private report via GitHub Security Advisories for this repository.
   - Docs: https://docs.github.com/en/code-security/security-advisories
 - Alternate: Contact the Lead Maintainer (Kevin Sames, @kevinsames) via GitHub with a private message or by referencing a private channel; do not open a public issue for sensitive reports.
 
 Please include:
 - A clear description of the issue and potential impact
 - Steps to reproduce or a minimal proof-of-concept
 - Affected versions (if known) and environment details
 
 We will acknowledge receipt within 3 business days and keep you informed of the status as we investigate and remediate. We aim to issue a fix and release within 7–30 days depending on severity and complexity.
 
 ## Coordinated Disclosure
 We request that you keep details private until a fix is released. We will coordinate a public disclosure date with you and credit you (if desired) in the release notes.
 
 ## Supported Versions
 We follow Semantic Versioning. Generally, we provide fixes for the latest minor release and the most recent previous minor release where feasible. Older versions may receive security patches at the maintainers’ discretion.
 
 ## Dependency Vulnerabilities
 If the issue originates from an upstream dependency (e.g., PySpark, delta-spark, Azure SDKs), we will track the upstream fix and publish a patched release that upgrades the dependency when available.
 
 ## Non-Security Issues
 For non-security bugs or feature requests, please use GitHub Issues or Discussions instead of this process.
 
