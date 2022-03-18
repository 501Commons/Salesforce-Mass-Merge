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
    C501_MM_Process_Merge_Queries.get().execute(null);
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
// Reset Having Counts
/*
    for (C501_Merge_Metric__c mergeMetric : [SELECT Id FROM C501_Merge_Metric__c]) {
        mergeMetric.Last_Account_Discover_GroupByHavingCount__c = 0.0;
        mergeMetric.Last_Contact_Discover_GroupByHavingCount__c = 0.0;
        mergeMetric.Last_ContactsAcross_Discover_HavingCount__c = 0.0;
        
        update mergeMetric;
    }
*/

public class C501_MM_Process_Merge_Queries implements Schedulable {

	// only need one instance of this class per transaction
	private static C501_MM_Process_Merge_Queries instance;
	public static C501_MM_Process_Merge_Queries get() {

		if (instance == null) instance = new C501_MM_Process_Merge_Queries();
		return instance;
	}

    public void C501_MM_Process_Merge_Queries() {
    }

    @future
    public static void executeFuture() {

        String strSoql = C501_MassMerge_SharedCode.GetMergeQueries();
        if (String.isBlank(strSoql)) {
            return;
        }
        
        C501_MM_Process_Merge_Queries.get().ProcessMergeMetrics(strSoql);
    }

    public void execute(SchedulableContext SC) {

        String strSoql = C501_MassMerge_SharedCode.GetMergeQueries();
        if (String.isBlank(strSoql)) {
            return;
        }

        C501_MM_Process_Merge_Queries_Batch batchProcessMergeQueries = new C501_MM_Process_Merge_Queries_Batch();
        batchProcessMergeQueries.query = strSoql;
        batchProcessMergeQueries.start = Datetime.now();

        if (!Test.isRunningTest()) {
            Database.executeBatch(batchProcessMergeQueries);
        } else {
            this.ProcessMergeMetrics(strSoql);
        }
    }

    public void ProcessMergeMetrics(String strSoql) {

        // Trigger the Discover General Processing
        C501_MM_Rule_Discover.get().DiscoverGeneralProcessing();        

        C501_MM_Rule_Discover discover = new C501_MM_Rule_Discover();
        discover.ProcessMergeQueryAccounts(strSoql + ' AND Object_Type__c = \'Account\' LIMIT 40');
        discover.ProcessMergeQueryContacts(strSoql + ' AND Object_Type__c = \'Contact\' LIMIT 40');
    }
}