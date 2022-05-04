## Data Triage 

<!--
Please complete all items. Ask questions in the #data slack channel
--->

## Housekeeping 
* [ ] Title the issue as "<ISO date> Data Triage" e.g. **2020-07-09 Data Triage**
* [ ] Assign this issue to both the Data Analyst and Data Engineer assigned to Triage 
* [ ] [Add a weight to the issue](https://about.gitlab.com/handbook/business-ops/data-team/how-we-work/#issue-pointing)
* [ ] Link any issues opened as a result of Data Triage to this `parent` issue. 

## Data Analyst tasks
All tasks below should be checked off on your Triage day. 
This is the most important issue on your Triage day. 
Please prioritize this issue since we dedicate a day from your milestone to this activity. 

### Reply to slack channels 
* [ ] Review each slack message request in the **#data** and **#data-lounge** channel 
    - [ ] Reply to slack threads by pointing GitLab team member to the appropriate handbook page, visualization, or to other GitLab team member who may know more about the topic. 
    - [ ] Direct GitLab team member to the channel description, which has the link to the Data team project, if the request requires more than 5 minutes of investigative effort from a Data team member.
* [ ] Review each slack message in the **#data-triage** channel, which will inform the triager of what issues have been opened in the data team project that day.  Because this channel can sometimes be difficult to keep track of, you **should** also look at [issues with the ~"Needs Triage" label](https://gitlab.com/gitlab-data/analytics/-/issues?label_name%5B%5D=Needs+Triage&scope=all&state=opened), as this label is added every hour to issues that may have been missed.
    - [ ] For each issue opened by a non-Data Team member, label the issue by: 
        - [ ] Adding the `Workflow::start (triage)` and `Triage` label
        - [ ] Adding additional [labels](https://about.gitlab.com/handbook/business-ops/data-team/how-we-work/#issue-labeling)
        - [ ] Assigning the issue based on:
            - [ ] the [CODEOWNERS file](https://gitlab.com/gitlab-data/analytics/blob/master/CODEOWNERS) for specific dbt model failures 
            - [ ] the [functional DRIs](https://about.gitlab.com/handbook/business-ops/data-team/organization/#team-organization)
            - [ ] OR to the  Manager, Data if you aren't sure. 
        - [ ] Asking initial questions (data source, business logic clarification, etc) to groom the issue. 

### Friends and family days
* [ ] As we currently have a no-merge Friday rule if there is an upcoming family and friends day during your triage week which affects this please ensure this message (or similar) is shared #data channel by Tuesday at the latest: 
* ```:awesome-dog-pug: :siren-siren:  Hi everyone, just a small FYI / reminder that due to the family and friends day this week the last day to merge MR’s in to the analytics repo this week will be Wednesday. :awesome-dog-pug: :siren-siren:```

### Maintain KPI related information         
* [ ] Maintain the KPI Index page by 
    - [ ] Creating an issue with any outstanding concerns from your respective division (broken links, missing KPI definitions, charts vs links, etc)
    - [ ] Assigning the issue to the [functional DRIs](https://about.gitlab.com/handbook/business-ops/data-team/organization/#team-organization)
* [ ] Review the commit history of the following two files and update the [sheetload_kpi_status data](https://docs.google.com/spreadsheets/d/1CZLnXiAG7D_T_6vm50X0hDPnMPKrKmtajrcga5vyDTQ/edit?usp=sharing) with any new KPIs or update existing KPI statistics (`commit_start` column is the commit URL for the `start_date` and `commit_handbook_v1` column is the commit URL for the `completion_date`)
    - [ ] [KPI Index handbook page](https://gitlab.com/gitlab-com/www-gitlab-com/-/blob/master/sites/handbook/source/handbook/business-technology/data-team/kpi-development/index.html.md.erb)
    - [ ] [Engineering KPI list](https://gitlab.com/gitlab-com/www-gitlab-com/-/blob/master/data/performance_indicators/engineering_function.yml)

### Prepare for Next Milestone 
* [ ] Groom Issues for Next Milestone: for issues that have missing or poor requirements, add comments in the issue asking questions to the Business DRI. 
* [ ] Update the planning issues for this milestone and the next milestone 
* [ ] Close/clean-up any irrelevant issues in your backlog. 


## Data Engineer tasks

* [ ] Notify Data Customers of [data refresh SLO](https://about.gitlab.com/handbook/business-ops/data-team/platform/#extract-and-load) breach by posting a message to the _#data_ Slack channel using the appropriate Data Notification Template
* [ ] [Create an issue](https://gitlab.com/gitlab-data/analytics/issues/new?issuable_template=DE%20Triage%20Errors) for each new failure in **#analytics-pipelines**
    * [ ] Link to all resulting issues and MRs in slack 
* [ ] [Investigate](https://gitlab.com/gitlab-org/gitlab/-/merge_requests?scope=all&state=all&label_name[]=Data%20Warehouse%3A%3AImpact%20Check&draft=no&approved_by_usernames[]=Any) all relevant merge requests to the gitlab.com database schema, create an issue for each MR in the analytics project that impacts the GitLab.com extraction. Link each issue created to this issue. A detailed explanation of how to act if there is some impact is described on the page [#gitlabcom-db-structure-changes](https://about.gitlab.com/handbook/business-technology/data-team/how-we-work/triage/#gitlabcom-db-structure-changes)

In addition to these tasks, the Data Engineer on triage should be focused on resolving these issues, including the backlog found on the [DE - Triage Errors board](https://gitlab.com/groups/gitlab-data/-/boards/1917859)

#### Handling dbt-test errors:

* [ ] Clean up the dbt-test failure logs and put them into a table in the comments of this issue, use the below format as an example: 

<details>
<summary>instructions</summary>

``` 
dbt-test errors: <Link to airflow log> 

Completed with x errors and x warnings:

##### Existing errors 
| Issue | Error | 
| ----- | ----- | 

##### New errors 
| Issue | Error | 
| ----- | ----- |

##### Warnings
| Warning | 
| ------- |
```

* Quick procedure to cleanup the log:
  1. Open any text editor with a regex find and replace; run through the below strings doing a find and replace for all: 
        * `^(?!.*(Failure in test|Database error|Warning)).*$`
        * `^\[\d{4}-\d{2}-\d{2}, \d{2}:\d{2}:\d{2} UTC\] INFO - `
        * `^\R`
  2. In order, each of these lines: 
     1. Removes all lines without Database Failure or Test Failure
     2. Removes date and INFO from each line 
     3. Removes empty lines

</details>

### Data Notification Templates

Use these to notify stakeholders of Data Delays.

<details>
<summary><i>Data Source Delay Templates</i></summary>

Post notices to #data and cross-post to #whats-happening-at-gitlab

#### GitLab.com

We have identified a delay in the `GitLab.com` data refresh and this problem potentially also delays data for GitLab KPIs (e.g. MR Rate, TMAU) or SiSense dashboards. We are actively working on a resolution and will provide an update once the KPIs and SiSense dashboards have been brought up-to-date.

The `GitLab.com` data in the warehouse and downstream models is accurate as of `YYYY-MM-DD HH:MM UTC (HH:MM PST)`.

The DRI for this incident is `@username`.

The link to the Data Team Incident issue is <link>

`CC @Mek Stittri, @Christopher Lefelhocz, @Hila Qu, @WayneHaber,  @Steve Loyd, @lily, @kwiebers, @Davis Townsend, @s_awezec, @mkarampalas, @product-analysts`


#### Salesforce

Message: We have identified a delay in the `Salesforce` data refresh and this problem potentially impacts any Sales related KPIs or SiSense dashboards. We are actively working on a resolution and will provide an update once the KPIs and SiSense dashboards have been brought up-to-date.

The `Salesforce` data in the warehouse and downstream models is accurate as of YYYY-MM-DD HH:MM UTC (HH:MM PST).

The DRI for this incident is `@username`.

The link to the Data Team Incident issue is <link>

`CC @Jake Bielecki, @Matt Benzaquen, @Jack Brennan, @Craig Mestel`

#### Zuora

Message: We have identified a delay in the `Zuora` data refresh and this problem potentially impacts any Financial KPIs or SiSense dashboards. We are actively working on a resolution and will provide an update once the KPIs and SiSense dashboards have been brought up-to-date.

The `Zuora` data in the warehouse and downstream models is accurate as of YYYY-MM-DD HH:MM UTC (HH:MM PST).

The DRI for this incident is `@username`.

The link to the Data Team Incident issue is <link>

`CC @Jake Bielecki, @Matt Benzaquen, @Jack Brennan, @Craig Mestel`

#### General

We have identified a delay in the `DATA SOURCE` data refresh. We are actively working on a resolution and will provide an update once data has been brought up-to-date.

The `DATA SOURCE` data in the warehouse and downstream models is accurate as of YYYY-MM-DD HH:MM UTC (HH:MM PST).

The DRI for this incident is `@username`.

The link to the Data Team Incident issue is <link>

</details>

## Finishing the Day

* [ ] At the end of your working day post EOD message to slack along with a link to this issue in the above mentioned slack channels so that it is clear for the next triager what time to check for issues from.
* [ ] Leave a comment giving context on open items and issues. If it's relevant to a specific issue, consider commenting in that issue and then linking to your comment.
* [ ] List down the effort performed for today's triage.
<details>
<summary><i>Hours spent per activity for Data Analyst</i></summary>

- 
- 
- 

</details>

<details>
<summary><i>Hours spent per activity for Data Engineer</i></summary>

- 
- 
- 

</details>

* [ ] List down the Groundhog Issues
- 
- 
- 


/label ~"workflow::In dev" ~"Housekeeping" ~"Data Team" ~"Documentation" ~"Triage" ~"Priority::1-Ops"

/assign me
