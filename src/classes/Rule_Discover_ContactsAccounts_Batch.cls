// Run Code FROM Developer Console
//  1) Debug -> Open Execute Anonymous Window
//  2) paste code
/*
    Students Rebuild: Hunger Challenge (Production)

    Developer Console (steps to call method directly)

        Rule_Discover_ContactsAccounts_Batch.SyncWebsiteTeams( 7012I0000014IoB');

*/
//  3) click Execute

global class Rule_Discover_ContactsAccounts_Batch implements Database.Batchable<sObject>, Database.AllowsCallouts, Database.Stateful { 

    global String query;
    global Datetime start;
    global Boolean stopProcessing = false;

    global Database.querylocator start(Database.BatchableContext BC) {

        C501_MassMerge_SharedCode.OutputDebugLogText(LoggingLevel.DEBUG, '***** Rule_Discover_ContactsAccounts_Batch::start - query: ' + this.query);
        return Database.getQueryLocator(this.query);
    }

    global void execute(Database.BatchableContext info, List<sObject> scope) {

        C501_MassMerge_SharedCode.OutputDebugLogText(LoggingLevel.DEBUG, '***** Rule_Discover_ContactsAccounts_Batch::execute - scope: ' + scope);

        Boolean processMergeMetrics = false;        
        List<Id> mergeMetricIds = new List<Id>();
        for (sObject scopeRecord :scope) {

            C501_Merge_Metric__c mergeRecord = (C501_Merge_Metric__c)scopeRecord;
            C501_MassMerge_SharedCode.OutputDebugLogText(LoggingLevel.DEBUG, '***** Rule_Discover_ContactsAccounts_Batch::execute - check having - mergeRecord.Last_ContactsAcross_Discover_HavingCount__c: ' + mergeRecord.Last_ContactsAcross_Discover_HavingCount__c);

            mergeMetricIds.add(mergeRecord.Id);

            if (mergeRecord.Last_ContactsAcross_Discover_HavingCount__c <> null &&
                ((Integer)mergeRecord.Last_ContactsAcross_Discover_HavingCount__c + C501_MassMerge_SharedCode.INCREMENT_MERGE_CANDIDATE_GROUPBY) >= C501_MassMerge_SharedCode.MaximumDiscoverHavingCount) {
   
                C501_MassMerge_SharedCode.OutputDebugLogText(LoggingLevel.DEBUG, '***** Rule_Discover_ContactsAccounts_Batch::execute - stopProcessing - mergeRecord.Last_ContactsAcross_Discover_HavingCount__c: ' + mergeRecord.Last_ContactsAcross_Discover_HavingCount__c);
                this.stopProcessing = true;

                Database.update(new C501_Merge_Metric__c(Id = mergeRecord.Id, Last_ContactsAcross_Discover_HavingCount__c = 0.0), false);            
            } else {
                    
                List<String> discoveryObjects = C501_MassMerge_SharedCode.MultiSelectPickListToList(mergeRecord.Discovery_Objects__c);
                if (discoveryObjects.contains(C501_MassMerge_SharedCode.MergeObjectScenarios.Contacts_Across_Accounts.name())) {
                    C501_MassMerge_SharedCode.OutputDebugLogText(LoggingLevel.DEBUG, '***** Rule_Discover_ContactsAccounts_Batch::execute - processMergeMetrics');
                    processMergeMetrics = true;
                }
            }
        }

        if (!processMergeMetrics) {
            this.stopProcessing = true;
            return;
        }

        if (this.stopProcessing) {
            return;
        }

		if (mergeMetricIds.isEmpty()) {
			return;
        }

        C501_MM_Rule_Discover_ContactsAccounts.get().ProcessMergeMetrics(this.query);
    }

    global void finish(Database.BatchableContext info) {

        Mass_Merge_Config__c massMergeConfigSettings = Mass_Merge_Config__c.getOrgDefaults();
        if (massMergeConfigSettings <> null && massMergeConfigSettings.Disable_All_Services__c) {

            C501_MassMerge_SharedCode.OutputDebugLogText(LoggingLevel.DEBUG, '***** Rule_Discover_ContactsAccounts_Batch::finish - Custom Settings - Merge Config : Disable All Services is set to true');
            return;
        }

        // Seconds = Convert from Millisconds / 1000 
        Long elapseSeconds = (Datetime.now().getTime() - this.start.getTime()) / 1000;
        C501_MassMerge_SharedCode.OutputDebugLogText(LoggingLevel.DEBUG, '***** Rule_Discover_ContactsAccounts_Batch::finish - elapseSeconds: ' + elapseSeconds + ' this.stopProcessing: ' + this.stopProcessing);

        if (elapseSeconds > (60 * 5) || this.stopProcessing || Test.isRunningTest()) {

            String strSoql = C501_MassMerge_SharedCode.GetMergeQueries();
            if (String.isBlank(strSoql)) {
                return;
            }

            C501_MM_Process_Merge_Queries_Batch batchMergeQueries = new C501_MM_Process_Merge_Queries_Batch();
            batchMergeQueries.query = strSoql;
            batchMergeQueries.start = Datetime.now();
    
            if (!Test.isRunningTest()) {
                Database.executeBatch(batchMergeQueries);               
            }
         
            return;
        }

        Rule_Discover_ContactsAccounts_Batch batchDiscover = new Rule_Discover_ContactsAccounts_Batch();
        batchDiscover.query = this.query;
        batchDiscover.start = this.start;
    
        if (!Test.isRunningTest()) {
            Database.executeBatch(batchDiscover);
        }
    }
}