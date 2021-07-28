# ethelease

#### Author: William Rex Chen

A Python tool to help Data Engineers and the like to 
containerize their data pipelines and toss them onto a 
Kubernetes cluster.  Think of it like a mix of Helm and 
Airflow but without the Go styled templating and 
without the excessively heavy infrastructure requirements 
of the workflow management tool.

The tool's a bit opinionated as they say in order to
force Data Engineers to develop pipelines in a CI/CD
manner.  However, following the `ethelease` flow
will assuredly reduce time trying to cobble a data platform
from scratch.

Right now, GCP is the major cloud provider supported.  AWS
will be added and prioritized.