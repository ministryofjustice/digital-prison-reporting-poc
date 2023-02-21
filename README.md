# Ministry of Justice Digital Prison Reporting Glue Scripts

[![repo standards badge](https://img.shields.io/badge/dynamic/json?color=blue&style=for-the-badge&logo=github&label=MoJ%20Compliant&query=%24.result&url=https%3A%2F%2Foperations-engineering-reports.cloud-platform.service.justice.gov.uk%2Fapi%2Fv1%2Fcompliant_public_repositories%2Fhmpps-digital-prison-reporting-glue-poc)](https://operations-engineering-reports.cloud-platform.service.justice.gov.uk/public-github-repositories.html#hmpps-digital-prison-reporting-glue-poc "Link to report")

#### CODEOWNER

- Team : [hmpps-digital-prison-reporting](https://github.com/orgs/ministryofjustice/teams/hmpps-digital-prison-reporting)
- Email : digitalprisonreporting@digital.justice.gov.uk
- Slack : [#hmpps-dpr-poc](https://mojdt.slack.com/archives/C03TBLUL45B)

**_This repository is for a Proof of Concept only_**

**_Under no circumstance should this repo be considered for production deployment**

## Details


#### Confluence Page:

[DPR Proof of Concept](https://dsdmoj.atlassian.net/wiki/spaces/DPR/pages/4118478887/Proof+of+concept+scope)

#### Overview

[DPR Solution Architecture](https://dsdmoj.atlassian.net/wiki/spaces/DPR/pages/4101931018/Solution+Architecture)

The main components are:
- **Cloud Platform** : a platform to ingest data from a variety of sources and store it for reporting purposes;
- **Domain Platform** : a platform to provide reporting SMEs with a flexible way to develop a data mesh of domains on top of the Cloud Platform


#### Further Investigation

#### Code Changes

- Please keep all Code Commentary and Documentation up to date

#### Branch Naming

- Please use wherever possible the JIRA ticket as branch name.

#### Commits

- Please reference or link any relevant JIRA tickets in commit message.

#### Pull Request

- Please reference or link any relevant JIRA tickets in pull request notes.

#### Local Development or Execution

This Proof of concept uses maven to build and run tests.

- Clone this repo
- install using maven

```
mvn clean install
```

Tests can be skipped using 
```
mvn clean install -DskipTests=true
```

## Unit Testing 
Unit testing uses JUnit and Mockito where appropriate.

```
mvn clean test
```

## Integration Testing

```
TBD 
```
## Acceptance Testing

```
TBD
```

### Notes

- Modify the Dependabot file to suit the [dependency manager](https://docs.github.com/en/code-security/dependabot/dependabot-version-updates/configuration-options-for-the-dependabot.yml-file#package-ecosystem) you plan to use and for [automated pull requests for package updates](https://docs.github.com/en/code-security/supply-chain-security/keeping-your-dependencies-updated-automatically/enabling-and-disabling-dependabot-version-updates#enabling-dependabot-version-updates). Dependabot is enabled in the settings by default.

- Ensure as many of the [GitHub Standards](https://github.com/ministryofjustice/github-repository-standards) rules are maintained as possibly can.
