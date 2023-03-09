from jira import JIRA
from collections import defaultdict
import config
jiraOptions = {'server': "https://iztech.atlassian.net"}

jira = JIRA(options=jiraOptions, basic_auth=(
    config.jira_mail, config.jira_token))


def get_report():
    project = jira.projects()
    print(project)
    listIssues = jira.search_issues(
        jql_str='project = "IA" AND sprint="IA Sprint 3" AND updated > startOfDay("0h") and updated <  startOfDay("24h")')
    listIssueByName = defaultdict(list)
    for singleIssue in listIssues:
        if (singleIssue.fields.assignee):
            listIssueByName[singleIssue.fields.assignee.displayName].append(
                singleIssue.fields.summary)
    return listIssueByName
