/*
    Copyright (c) 2019, 501Commons.org
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
      C501_MM_Rule_Merge.get().execute(null);
*/
//  3) click Execute
//

public class C501_MM_Rule_Merge implements Schedulable {

	// only need one instance of this class per transaction
	private static C501_MM_Rule_Merge instance;
	public static C501_MM_Rule_Merge get() {

		if (instance == null) instance = new C501_MM_Rule_Merge();
		return instance;
	}

    public void C501_MM_Rule_Merge() {
    }

    public void execute(SchedulableContext SC) {

        String strSoql = C501_MassMerge_SharedCode.GetC501MergeMetricQuery();
        this.ProcessMergeMetrics(strSoql);
    }

    public void executeMetricById(Id mergeMetricId) {

        String strSoql = C501_MassMerge_SharedCode.GetC501MergeMetricQuery();

        // Add in specific Id query
        strSoql += ' AND Id = \'' + mergeMetricId + '\'';

        this.ProcessMergeMetrics(strSoql);
    }

    public void executeMetric(Id mergeMetricId) {
        this.executeMetricById(mergeMetricId);
    }

    public void ProcessMergeMetrics(String strSoql) {

 		//
		// Perform all SOQL Queries outside of loop to avoid Salesforce Governor Limits: https://developer.salesforce.com/docs/atlas.en-us.apexcode.meta/apexcode/apex_gov_limits.htm
		//

        strSoql += ' ORDER BY Last_Merge_Date__c ASC';

        System.debug(LoggingLevel.DEBUG, '***** ' + String.valueOf(this).substring(0,String.valueOf(this).indexOf(':')) + '::ProcessMergeMetrics strSoql: ' + strSoql);

        List<C501_Merge_Metric__c> mergeMetrics = Database.Query(strSoql);

        Set<String> sourceReportIds = new Set<String>();
        for (C501_Merge_Metric__c mergeMetric : mergeMetrics) {
            if (!String.isEmpty(mergeMetric.Source_Report__c)) {
                sourceReportIds.add(mergeMetric.Source_Report__c);
            }
        }

        List<Report> reports = [
            SELECT Id, DeveloperName, FolderName, LastModifiedDate
            FROM Report
            WHERE Id in :sourceReportIds OR DeveloperName in :sourceReportIds];

        // Walk through Merge Metrics
        List<C501_Merge_Metric__c> mergeMetricsUpdate = new List<C501_Merge_Metric__c>();

        Integer batchMergesScheduled = 0;
        for (C501_Merge_Metric__c mergeMetric :mergeMetrics) {

            if (C501_MassMerge_SharedCode.NearSalesforceLimits()) {
                break;
            }

            Boolean reportChangeDetected = false;
            Boolean reportFound = false;
            for (Report report :reports) {

                if ((String)report.Id == mergeMetric.Source_Report__c || report.DeveloperName == mergeMetric.Source_Report__c) {

                    reportFound = true;

                    // Check for report change
                    if (report.LastModifiedDate <> null && (mergeMetric.Source_Report_LastModifiedDate__c == null || mergeMetric.Source_Report_LastModifiedDate__c <> report.LastModifiedDate)) {

                        reportChangeDetected = true;
                        System.debug(LoggingLevel.DEBUG, '***** C501_MM_Rule_Merge:ProcessMergeMetrics - Report Change Detected - Merge Metric Name: ' + mergeMetric.Name + ' Id: ' + mergeMetric.Id + ' Report: ' + report.DeveloperName + ' report.LastModifiedDate: ' + report.LastModifiedDate  + ' mergeMetric.Source_Report_LastModifiedDate__c: ' + mergeMetric.Source_Report_LastModifiedDate__c);

                        mergeMetric.Source_Report_LastModifiedDate__c = report.LastModifiedDate;
                        mergeMetric.Unique_Id__c = report.Id;
                        mergeMetricsUpdate.add(mergeMetric);
                    }

                    break;
                }
            }

            if (reportChangeDetected || !reportFound) {

                if (!reportFound) {
                    System.debug(LoggingLevel.ERROR, '***** C501_MM_Rule_Merge:ProcessMergeMetrics - Report Not Found - Merge Metric Name: ' + mergeMetric.Name + ' Id: ' + mergeMetric.Id);
                }

                // Queue merge metric to delete children since report changed or report not found
                //

                String strSoqldeleteAccountMerges = C501_MassMerge_SharedCode.GetC501AccountMergeQuery();
                strSoqldeleteAccountMerges += ' AND Merge_Metric__c = \'' + mergeMetric.Id + '\'';

                System.debug(LoggingLevel.DEBUG, '***** ' + String.valueOf(this).substring(0,String.valueOf(this).indexOf(':')) + '::ProcessMergeMetrics strSoql: ' + strSoqldeleteAccountMerges);
                List<C501_Account_Merge__c> deleteAccountMerges = Database.query(strSoqldeleteAccountMerges);

                if (!deleteAccountMerges.isEmpty()) {
                    System.debug(LoggingLevel.DEBUG, '***** ' + String.valueOf(this).substring(0,String.valueOf(this).indexOf(':')) + '::ProcessMergeMetrics Delete Account Merges: ' + deleteAccountMerges);
                    delete deleteAccountMerges;
                }

                String strSoqldeleteContactMerges = C501_MassMerge_SharedCode.GetC501ContactMergeQuery();
                strSoqldeleteContactMerges += ' AND Merge_Metric__c = \'' + mergeMetric.Id + '\'';

                System.debug(LoggingLevel.DEBUG, '***** ' + String.valueOf(this).substring(0,String.valueOf(this).indexOf(':')) + '::ProcessMergeMetrics strSoql: ' + strSoqldeleteContactMerges);
                List<C501_Contact_Merge__c> deleteContactMerges = Database.query(strSoqldeleteContactMerges);

                if (!deleteContactMerges.isEmpty()) {
                    System.debug(LoggingLevel.DEBUG, '***** ' + String.valueOf(this).substring(0,String.valueOf(this).indexOf(':')) + '::ProcessMergeMetrics Delete Contact Merges: ' + deleteContactMerges);
                    delete deleteContactMerges;
                }

                continue;
            }

            mergeMetric.Last_Merge_Date__c = System.now();
            mergeMetricsUpdate.add(mergeMetric);

            C501_MassMerge_SharedCode_Account c501MassMerge_SharedCode_Account = new C501_MassMerge_SharedCode_Account();
            Boolean sequentialMergeRun = c501MassMerge_SharedCode_Account.ProcessObjectMergeCandidates(mergeMetric) > 0 ? true : false;

            C501_MassMerge_SharedCode_Contact c501MassMerge_SharedCode_Contact = new C501_MassMerge_SharedCode_Contact();
            batchMergesScheduled += c501MassMerge_SharedCode_Contact.ProcessObjectMergeCandidates(mergeMetric);

            // Check for any account merges which are currently sequential due to a Future call when merging.  Contact merges use Batch so are nonsequential
            if (sequentialMergeRun) {
                break;
            }
        }

        if (!mergeMetricsUpdate.isEmpty()) {
            Database.SaveResult[] updateResults = Database.update(mergeMetricsUpdate, false);
            for (Database.SaveResult updateResult :updateResults) {
                if (!updateResult.isSuccess()) {
                    for(Database.Error resultError : updateResult.getErrors()) {
                        C501_MassMerge_SharedCode.LogDatabaseError(resultError, String.valueOf(this).substring(0,String.valueOf(this).indexOf(':')) + '::ProcessMergeMetrics');
                    }                        
                }
            }
        }
    }
}