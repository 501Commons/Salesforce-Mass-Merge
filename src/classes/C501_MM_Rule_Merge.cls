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
        if (String.isBlank(strSoql)) {
            return;
        }

        // Handle Account Merging
        //
        if (C501_MassMerge_SharedCode.GetCountAccountMergeCandidates() > 0) {        
        
            if (!Test.isRunningTest()) {

                Rule_Merge_Batch batchMerge = new Rule_Merge_Batch();
                batchMerge.query = strSoql;
                batchMerge.start = Datetime.now();
                batchMerge.mergeCandidates = C501_MassMerge_SharedCode.GetCountMergeCandidates();
                batchMerge.mergeCandidateRetries = 0;
            
                Database.executeBatch(batchMerge);
            } else {

                this.ProcessMergeMetrics(strSoql, true);
            }
        }

        Mass_Merge_Config__c massMergeConfigSettings = Mass_Merge_Config__c.getOrgDefaults();
        if (Test.isRunningTest() || (massMergeConfigSettings <> null && !massMergeConfigSettings.Batch_Contact_Merging__c)) {

            // Handle Contact Merging
            //
            if (C501_MassMerge_SharedCode.GetCountContactMergeCandidates() > 0) {   

                // Merge Contacts outside of the executeBatch because Contact merge throws a NPSP Exception due to Queueable Jobs limit when called in a Future or Batch
                this.ProcessMergeMetrics(strSoql, false);
            }
        }
    }

    @future
    public static void executeFuture() {

        C501_MM_Rule_Merge.get().execute(null);
    }

    public void executeMetricById(Id mergeMetricId) {

        String strSoql = C501_MassMerge_SharedCode.GetC501MergeMetricQuery();
        if (String.isBlank(strSoql)) {
            return;
        }

        // Add in specific Id query
        strSoql += ' AND Id = \'' + mergeMetricId + '\'';

        this.ProcessMergeMetrics(strSoql, true);
        this.ProcessMergeMetrics(strSoql, false);
    }

    public void executeMetric(Id mergeMetricId) {
        this.executeMetricById(mergeMetricId);
    }

    public void ProcessMergeMetrics(String strSoql, Boolean processAccountMerge) {

 		//
		// Perform all SOQL Queries outside of loop to avoid Salesforce Governor Limits: https://developer.salesforce.com/docs/atlas.en-us.apexcode.meta/apexcode/apex_gov_limits.htm
		//

        strSoql += ' ORDER BY Last_Merge_Date__c ASC';
        strSoql += ' LIMIT ' + String.valueOf(C501_MassMerge_SharedCode.GetMaximumLimitSize(1));

        C501_MassMerge_SharedCode.OutputDebugLogText(LoggingLevel.DEBUG, '***** ' + String.valueOf(this).substring(0,String.valueOf(this).indexOf(':')) + '::ProcessMergeMetrics strSoql: ' + strSoql);

        List<C501_Merge_Metric__c> mergeMetrics = Database.Query(strSoql);

        Set<String> sourceReportIds = new Set<String>();
        for (C501_Merge_Metric__c mergeMetric : mergeMetrics) {
            if (!String.isEmpty(mergeMetric.Source_Report__c)) {
                sourceReportIds.add(mergeMetric.Source_Report__c);
            }
        }

        List<Report> reports = [
            SELECT Id, Name, DeveloperName, FolderName, LastModifiedDate
            FROM Report
            WHERE Id in :sourceReportIds OR DeveloperName in :sourceReportIds];

        // Walk through Merge Metrics
        Map<String, C501_Merge_Metric__c> mergeMetricsUpdate = new Map<String, C501_Merge_Metric__c>();

        Boolean stopMerging = false;
        do {
            for (C501_Merge_Metric__c mergeMetric :mergeMetrics) {

                if (C501_MassMerge_SharedCode.NearSalesforceLimits()) {
                    stopMerging = true;
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
                            C501_MassMerge_SharedCode.OutputDebugLogText(LoggingLevel.DEBUG, '***** C501_MM_Rule_Merge:ProcessMergeMetrics - Report Change Detected - Merge Metric Name: ' + mergeMetric.Name + ' Id: ' + mergeMetric.Id + ' Report: ' + report.DeveloperName + ' report.LastModifiedDate: ' + report.LastModifiedDate  + ' mergeMetric.Source_Report_LastModifiedDate__c: ' + mergeMetric.Source_Report_LastModifiedDate__c);

                            mergeMetric.Source_Report_LastModifiedDate__c = report.LastModifiedDate;
                            mergeMetric.Unique_Id__c = report.Id;
                            if (mergeMetricsUpdate.containsKey(mergeMetric.Id)) {
                                mergeMetricsUpdate.remove(mergeMetric.Id);
                            }

                            mergeMetricsUpdate.put(mergeMetric.Id, mergeMetric);
                        }

                        break;
                    }
                }

                if (reportChangeDetected || !reportFound) {

                    if (!reportFound) {
                        C501_MassMerge_SharedCode.OutputDebugLogText(LoggingLevel.ERROR, '***** C501_MM_Rule_Merge:ProcessMergeMetrics - Report Not Found - Merge Metric Name: ' + mergeMetric.Name + ' Id: ' + mergeMetric.Id);
                    }

                    // Queue merge metric to delete children since report changed or report not found
                    //

                    String strSoqldeleteAccountMerges = C501_MassMerge_SharedCode.GetC501AccountMergeQuery();
                    if (String.isBlank(strSoqldeleteAccountMerges)) {
                        return;
                    }
                    strSoqldeleteAccountMerges += ' AND Merge_Metric__c = \'' + mergeMetric.Id + '\'';

                    C501_MassMerge_SharedCode.OutputDebugLogText(LoggingLevel.DEBUG, '***** ' + String.valueOf(this).substring(0,String.valueOf(this).indexOf(':')) + '::ProcessMergeMetrics strSoql: ' + strSoqldeleteAccountMerges);
                    List<C501_Account_Merge__c> deleteAccountMerges = Database.query(strSoqldeleteAccountMerges);

                    if (!deleteAccountMerges.isEmpty()) {
                        C501_MassMerge_SharedCode.OutputDebugLogText(LoggingLevel.DEBUG, '***** ' + String.valueOf(this).substring(0,String.valueOf(this).indexOf(':')) + '::ProcessMergeMetrics Delete Account Merges: ' + deleteAccountMerges);
                        Database.delete(deleteAccountMerges, false);
                    }

                    String strSoqldeleteContactMerges = C501_MassMerge_SharedCode.GetC501ContactMergeQuery();
                    if (String.isBlank(strSoqldeleteContactMerges)) {
                        return;
                    }
                    strSoqldeleteContactMerges += ' AND Merge_Metric__c = \'' + mergeMetric.Id + '\'';

                    C501_MassMerge_SharedCode.OutputDebugLogText(LoggingLevel.DEBUG, '***** ' + String.valueOf(this).substring(0,String.valueOf(this).indexOf(':')) + '::ProcessMergeMetrics strSoql: ' + strSoqldeleteContactMerges);
                    List<C501_Contact_Merge__c> deleteContactMerges = Database.query(strSoqldeleteContactMerges);

                    if (!deleteContactMerges.isEmpty()) {
                        C501_MassMerge_SharedCode.OutputDebugLogText(LoggingLevel.DEBUG, '***** ' + String.valueOf(this).substring(0,String.valueOf(this).indexOf(':')) + '::ProcessMergeMetrics Delete Contact Merges: ' + deleteContactMerges);
                        Database.delete(deleteContactMerges, false);
                    }

                    continue;
                }

                mergeMetric.Last_Merge_Date__c = System.now();

                if (mergeMetricsUpdate.containsKey(mergeMetric.Id)) {
                    mergeMetricsUpdate.remove(mergeMetric.Id);
                }
                mergeMetricsUpdate.put(mergeMetric.Id, mergeMetric);

                if (C501_MassMerge_SharedCode.NearSalesforceLimits()) {
                    C501_MassMerge_SharedCode.OutputDebugLogText(LoggingLevel.WARN, '***** ' + String.valueOf(this).substring(0,String.valueOf(this).indexOf(':')) + '::ProcessMergeMetrics Near Salesforce Limits so kick out on processing');
                    
                    stopMerging = true;
                    break;
                }
        
                if (processAccountMerge) {
                    C501_MassMerge_SharedCode_Account c501MassMerge_SharedCode_Account = new C501_MassMerge_SharedCode_Account();
                    Integer numberMerged = c501MassMerge_SharedCode_Account.ProcessObjectMergeCandidates(mergeMetric);

                    if (Test.isRunningTest()) {
                        stopMerging = true;
                        break;
                    }
                } else {
                    C501_MassMerge_SharedCode_Contact c501MassMerge_SharedCode_Contact = new C501_MassMerge_SharedCode_Contact();
                    Integer numberMerged = c501MassMerge_SharedCode_Contact.ProcessObjectMergeCandidates(mergeMetric);

                    if (Test.isRunningTest()) {
                        stopMerging = true;
                        break;
                    }
                }
            }
        } while (!stopMerging && !C501_MassMerge_SharedCode.NearSalesforceLimits());
        
        if (!mergeMetricsUpdate.values().isEmpty()) {
            Database.SaveResult[] updateResults = Database.update(mergeMetricsUpdate.values(), false);
            for (Database.SaveResult updateResult :updateResults) {
                if (!updateResult.isSuccess()) {
                    for(Database.Error resultError : updateResult.getErrors()) {
                        C501_MassMerge_SharedCode.LogDatabaseError(resultError, String.valueOf(this).substring(0,String.valueOf(this).indexOf(':')) + '::ProcessMergeMetrics');
                    }                        
                }
            }
        }

        if (C501_MassMerge_SharedCode.NearSalesforceLimits()) {
            C501_MassMerge_SharedCode.OutputDebugLogText(LoggingLevel.WARN, '***** ' + String.valueOf(this).substring(0,String.valueOf(this).indexOf(':')) + '::ProcessMergeMetrics Near Salesforce Limits so kick out on processing');
        }

        // Flush all Debug Logs to the Database
        C501_MassMerge_SharedCode.OutputDebugLogFlush();
    }
}