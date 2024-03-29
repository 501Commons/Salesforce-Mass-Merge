/*
    Nonprofit Salesforce © 2022 by 501 Commons is licensed under CC BY 4.0
    All rights reserved.
    
    Redistribution and use in source and binary forms, with or without
    modification, are permitted provided that the following conditions are met:
    
    * Redistributions of source code must retain the above copyright
      notice, this list of conditions and the following disclaimer.
    * Redistributions in binary form must reproduce the above copyright
      notice, this list of conditions and the following disclaimer in the
      documentation and/or other materials provided with the distribution.
    * Neither the name of 501Commons.org nor the names of
      its contributors may be used to endorse or promote products derived
      from this software without specific prior written permission.
 
    THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
    "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT 
    LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS 
    FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE 
    COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, 
    INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, 
    BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; 
    LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER 
    CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT 
    LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN 
    ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE 
    POSSIBILITY OF SUCH DAMAGE.
*/

//
// Run Code FROM Developer Console
//  1) Debug -> Open Execute Anonymous Window
//  2) paste code
/*
      C501_MM_Rule_Discover_ContactsAccounts.get().execute(null);
*/
//  3) click Execute
//
// Abort scheduled jobs
/*
    List<CronJobDetail> scheduledJobs = [SELECT Id FROM CronJobDetail WHERE Name like 'C501_%'];
    for (CronJobDetail scheduledJob :scheduledJobs) {
        Id jobId = [SELECT Id FROM CronTrigger WHERE CronJobDetailId = :scheduledJob.Id].Id;
        System.abortJob(jobId);
    }
*/

public class C501_MM_Rule_Discover_ContactsAccounts implements Schedulable {

	// only need one instance of this class per transaction
	private static C501_MM_Rule_Discover_ContactsAccounts instance;
	public static C501_MM_Rule_Discover_ContactsAccounts get() {

		if (instance == null) instance = new C501_MM_Rule_Discover_ContactsAccounts();
		return instance;
	}

    public void C501_MM_Rule_Discover_ContactsAccounts() {
    }

    @future
    public static void executeFuture() {

        String strSoql = C501_MassMerge_SharedCode.GetC501MergeMetricQuery();
        if (String.isBlank(strSoql)) {
            return;
        }

        C501_MM_Rule_Discover_ContactsAccounts.get().ProcessMergeMetrics(strSoql);
    }

    public void execute(SchedulableContext SC) {

        String strSoql = C501_MassMerge_SharedCode.GetC501MergeMetricQuery();
        if (String.isBlank(strSoql)) {
            return;
        }

        Rule_Discover_ContactsAccounts_Batch batchDiscover = new Rule_Discover_ContactsAccounts_Batch();
        batchDiscover.query = strSoql;
        batchDiscover.start = DateTime.now();

        if (!Test.isRunningTest()) {
            Database.executeBatch(batchDiscover);
        } else {
            this.ProcessMergeMetrics(strSoql);
        }
    }

    public void executeMetricById(Id mergeMetricId) {

        String strSoql = C501_MassMerge_SharedCode.GetC501MergeMetricQuery();
        if (String.isBlank(strSoql)) {
            return;
        }

        // Add in specific Id query
        strSoql += ' AND Id = \'' + mergeMetricId + '\'';

        this.ProcessMergeMetrics(strSoql);
    }

    @future
    public static void executeMetricFuture(Id mergeMetricId) {
        C501_MM_Rule_Discover_ContactsAccounts.get().executeMetricById(mergeMetricId);
    }

    public void ProcessMergeMetrics(String strSoql) {

        // Trigger the Discover General Processing
        C501_MM_Rule_Discover.get().DiscoverGeneralProcessing();        

 		//
		// Perform all SOQL Queries outside of loop to avoid Salesforce Governor Limits: https://developer.salesforce.com/docs/atlas.en-us.apexcode.meta/apexcode/apex_gov_limits.htm
		//

        strSoql += ' ORDER BY Last_ContactsAccounts_Discover_Date__c ASC NULLS FIRST';
        strSoql += ' LIMIT ' + String.valueOf(C501_MassMerge_SharedCode.GetMaximumLimitSize(1));

        C501_MassMerge_SharedCode.OutputDebugLogText(LoggingLevel.DEBUG, '***** ' + String.valueOf(this).substring(0,String.valueOf(this).indexOf(':')) + '::ProcessMergeMetrics strSoql: ' + strSoql);

        List<C501_Merge_Metric__c> mergeMetrics = Database.Query(strSoql);

        Set<String> sourceReportIds = new Set<String>();
        for (C501_Merge_Metric__c mergeMetric : mergeMetrics) {
            if (!String.isBlank(mergeMetric.Source_Report__c)) {
                sourceReportIds.add(mergeMetric.Source_Report__c);
            }
        }

        List<Report> reports = [
            SELECT Id, Name, DeveloperName, FolderName, LastModifiedDate
            FROM Report
            WHERE Id in :sourceReportIds OR DeveloperName in :sourceReportIds];

        // Walk through Merge Metrics
        List<C501_Merge_Metric__c> mergeMetricsUpdate = new List<C501_Merge_Metric__c>();
        C501_MM_Rule_Discover discover = new C501_MM_Rule_Discover();

        // Discover any new merge candidates
        //
        for (C501_Merge_Metric__c mergeMetric : mergeMetrics) {

            String reportId;
            for (Report report :reports) {

                if ((String)report.Id == mergeMetric.Source_Report__c || report.DeveloperName == mergeMetric.Source_Report__c) {

                    reportId = (String)report.Id;

                    break;
                }
            }

            if (String.isBlank(reportId)) {
                continue;
            }

            mergeMetric.Last_ContactsAccounts_Discover_Date__c = System.now();
            mergeMetric.Unique_Id__c = reportId;

            C501_MassMerge_SharedCode.OutputDebugLogText(LoggingLevel.DEBUG, '***** ' + String.valueOf(this).substring(0,String.valueOf(this).indexOf(':')) + '::ProcessMergeMetrics update mergeMetric: ' + mergeMetric);

            C501_Merge_Metric__c mergeMetricUpdate = new C501_Merge_Metric__c(
                Id = mergeMetric.Id,
                Name = mergeMetric.Name,
                Last_ContactsAccounts_Discover_Date__c = System.now(),
                Unique_Id__c = reportId
            );

            C501_MassMerge_SharedCode.OutputDebugLogText(LoggingLevel.DEBUG, '***** ' + String.valueOf(this).substring(0,String.valueOf(this).indexOf(':')) + '::ProcessMergeMetrics update mergeMetric Name: ' + mergeMetricUpdate.Name + ' Last Account Discover Date (After Set): ' + mergeMetricUpdate.Last_ContactsAccounts_Discover_Date__c.format());

            Integer retriesRemaining = C501_MassMerge_SharedCode.MAX_RETRY_DATABASE;
            do {
                Database.SaveResult updateResult = Database.update(mergeMetricUpdate, false);
                if (!updateResult.isSuccess()) {
                    for(Database.Error resultError : updateResult.getErrors()) {
                        C501_MassMerge_SharedCode.LogDatabaseError(resultError, String.valueOf(this).substring(0,String.valueOf(this).indexOf(':')) + '::ProcessMergeMetrics');
                        if (!C501_MassMerge_SharedCode.RetryDatabaseError(resultError)) {
                            retriesRemaining = 0;
                        }
                    }                        
                } else {
                    break;
                }
            } while (retriesRemaining-- > 0);

            discover.ProcessReport(reportId, mergeMetric, C501_MassMerge_SharedCode.MergeObjectScenarios.Contacts_Across_Accounts.name());
            if (C501_MassMerge_SharedCode.NearSalesforceLimits()) {
                C501_MassMerge_SharedCode.OutputDebugLogText(LoggingLevel.WARN, '***** ' + String.valueOf(this).substring(0,String.valueOf(this).indexOf(':')) + '::ProcessMergeMetrics Near Salesforce Limits so kick out');
                break;
            }
        }
    }
}