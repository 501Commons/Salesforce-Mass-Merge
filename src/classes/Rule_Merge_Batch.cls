// Run Code FROM Developer Console
//  1) Debug -> Open Execute Anonymous Window
//  2) paste code
/*
    Students Rebuild: Hunger Challenge (Production)

    Developer Console (steps to call method directly)

        Rule_Merge_Batch.SyncWebsiteTeams( 7012I0000014IoB');

*/
//  3) click Execute

global class Rule_Merge_Batch implements Database.Batchable<sObject>, Database.AllowsCallouts, Database.Stateful { 

    global String query;
    global Id queuableJobId;
    global Datetime start;
    global Integer mergeCandidates;
    global Integer mergeCandidateRetries;

    global Database.querylocator start(Database.BatchableContext BC) {

        C501_MassMerge_SharedCode.OutputDebugLogText(LoggingLevel.DEBUG, '***** Rule_Merge_Batch::start - query: ' + this.query + ' mergeCandidates: ' + this.mergeCandidates);
        return Database.getQueryLocator(this.query);
    }

    global void execute(Database.BatchableContext info, List<sObject> scope) {

        C501_MassMerge_SharedCode.OutputDebugLogText(LoggingLevel.DEBUG, '***** Rule_Merge_Batch::execute - scope: ' + scope + ' mergeCandidates: ' + this.mergeCandidates);

        List<Id> mergeMetricIds = new List<Id>();
        for (sObject scopeRecord :scope) {
            mergeMetricIds.add(((C501_Merge_Metric__c)scopeRecord).Id);
        }

		if (mergeMetricIds.isEmpty()) {
			return;
        }

        if (C501_MassMerge_SharedCode.GetCountAccountMergeCandidates() > 0) {

            // Batch transaction you can enqueue only 1 job
            if (Limits.getQueueableJobs() > 0) {
                return;
            }
    
            C501_MassMerge_SharedCode.OutputDebugLogText(LoggingLevel.DEBUG, '***** Rule_Merge_Batch::execute - Account Merge - start queueable');
            this.queuableJobId = System.enqueueJob(new C501_MassMerge_Batch());

            return;
        }

        // Default Batch Contact Merge is false in Batch because NPSP on Database.Merge call is creating at least 2 Queueable Jobs which is throwing an exception
        //
        // 2/25 - Database.merge call down the stack appears to be creating multiple queuable jobs - suspect somewhere down in the NPSP logic, try again after the next NPSP update
        Mass_Merge_Config__c massMergeConfigSettings = Mass_Merge_Config__c.getOrgDefaults();
        if (massMergeConfigSettings <> null && massMergeConfigSettings.Batch_Contact_Merging__c) {

            if (C501_MassMerge_SharedCode.GetCountContactMergeCandidates() > 0) {

                // Batch transaction you can enqueue only 1 job
                if (Limits.getQueueableJobs() > 0) {
                    return;
                }
        
                C501_MassMerge_SharedCode.OutputDebugLogText(LoggingLevel.DEBUG, '***** Rule_Merge_Batch::execute - Contact Merge');
                C501_MM_Rule_Merge.get().ProcessMergeMetrics(this.query, false);

                return;
            }
        }
    }

    global void finish(Database.BatchableContext info) {

        Mass_Merge_Config__c massMergeConfigSettings = Mass_Merge_Config__c.getOrgDefaults();
        if (massMergeConfigSettings <> null && massMergeConfigSettings.Disable_All_Services__c) {

            C501_MassMerge_SharedCode.OutputDebugLogText(LoggingLevel.DEBUG, '***** Rule_Merge_Batch::finish - Custom Settings - Merge Config : Disable All Services is set to true');
            return;
        }

        // Seconds = Convert from Millisconds / 1000 
        Long elapseSeconds = (Datetime.now().getTime() - this.start.getTime()) / 1000;

        // Performance improvement ideas
        //      1) Only check for any merge candidates that are scheduled for auto merging
        //      2) Check to see if any change in last minute to merge candidates count
        //
        Integer currentMergeCandidates = C501_MassMerge_SharedCode.GetCountMergeCandidates();
        
        C501_MassMerge_SharedCode.OutputDebugLogText(LoggingLevel.DEBUG, '***** Rule_Merge_Batch::finish - elapseSeconds: ' + elapseSeconds + ' start mergeCandidates: ' + this.mergeCandidates + ' current mergeCandidates: ' + currentMergeCandidates + ' mergeCandidateRetries: ' + this.mergeCandidateRetries);

        this.mergeCandidateRetries = (this.mergeCandidateRetries == null) ? 0 : this.mergeCandidateRetries;

        // Check if time elapse, any merge candidates available or test running
        if (elapseSeconds > (60 * 20) || currentMergeCandidates <= 0 || Test.isRunningTest() 
            || this.mergeCandidateRetries >= 4) {

            C501_MassMerge_SharedCode.OutputDebugLogText(LoggingLevel.DEBUG, '***** Rule_Merge_Batch::finish - done merging');
            return;
        }

        if (this.mergeCandidates <> currentMergeCandidates) {
            this.mergeCandidateRetries = 0;
        } else {
            this.mergeCandidateRetries++;
        }

        C501_MassMerge_SharedCode.OutputDebugLogText(LoggingLevel.DEBUG, '***** Rule_Merge_Batch::finish - batch another merge');

        Rule_Merge_Batch batchMerge = new Rule_Merge_Batch();
        batchMerge.query = this.query;
        batchMerge.start = this.start;
        batchMerge.mergeCandidates = currentMergeCandidates;
        batchMerge.queuableJobId = this.queuableJobId;
        batchMerge.mergeCandidateRetries = (this.mergeCandidateRetries == null) ? 0 : this.mergeCandidateRetries;
    
        if (!Test.isRunningTest()) {
            Database.executeBatch(batchMerge);
        }
    }
}