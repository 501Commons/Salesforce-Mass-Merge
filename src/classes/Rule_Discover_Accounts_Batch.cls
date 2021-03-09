// Run Code FROM Developer Console
//  1) Debug -> Open Execute Anonymous Window
//  2) paste code
/*
    Students Rebuild: Hunger Challenge (Production)

    Developer Console (steps to call method directly)

        Rule_Discover_Accounts_Batch.SyncWebsiteTeams( 7012I0000014IoB');

*/
//  3) click Execute

global class Rule_Discover_Accounts_Batch implements Database.Batchable<sObject>, Database.AllowsCallouts, Database.Stateful { 

    global String query;
    global Datetime start;
    global Boolean stopProcessing = false;

    global Database.querylocator start(Database.BatchableContext BC) {

        C501_MassMerge_SharedCode.OutputDebugLogText(LoggingLevel.DEBUG, '***** Rule_Discover_Accounts_Batch::start - query: ' + this.query);
        return Database.getQueryLocator(this.query);
    }

    global void execute(Database.BatchableContext info, List<sObject> scope) {

        C501_MassMerge_SharedCode.OutputDebugLogText(LoggingLevel.DEBUG, '***** Rule_Discover_Accounts_Batch::execute - scope: ' + scope);

        Boolean processMergeMetrics = false;
        List<Id> mergeMetricIds = new List<Id>();
        for (sObject scopeRecord :scope) {

            C501_Merge_Metric__c mergeRecord = (C501_Merge_Metric__c)scopeRecord;

            mergeMetricIds.add(mergeRecord.Id);

            C501_MassMerge_SharedCode.OutputDebugLogText(LoggingLevel.DEBUG, '***** Rule_Discover_Accounts_Batch::execute - check having - mergeRecord.Last_Account_Discover_GroupByHavingCount__c: ' + mergeRecord.Last_Account_Discover_GroupByHavingCount__c);
            
            if (mergeRecord.Last_Account_Discover_GroupByHavingCount__c <> null &&
                ((Integer)mergeRecord.Last_Account_Discover_GroupByHavingCount__c + C501_MassMerge_SharedCode.INCREMENT_MERGE_CANDIDATE_GROUPBY) >= C501_MassMerge_SharedCode.MaximumDiscoverHavingCount) {
   
                C501_MassMerge_SharedCode.OutputDebugLogText(LoggingLevel.DEBUG, '***** Rule_Discover_Accounts_Batch::execute - stopProcessing - mergeRecord.Last_Account_Discover_GroupByHavingCount__c: ' + mergeRecord.Last_Account_Discover_GroupByHavingCount__c);
                this.stopProcessing = true;

                Database.update(new C501_Merge_Metric__c(Id = mergeRecord.Id, Last_Account_Discover_GroupByHavingCount__c = 0.0), false);            
            } else {
                    
                List<String> discoveryObjects = C501_MassMerge_SharedCode.MultiSelectPickListToList(mergeRecord.Discovery_Objects__c);
                if (discoveryObjects.contains(C501_MassMerge_SharedCode.MergeObjectScenarios.Account.name())) {
                    C501_MassMerge_SharedCode.OutputDebugLogText(LoggingLevel.DEBUG, '***** Rule_Discover_Accounts_Batch::execute - processMergeMetrics');
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

        C501_MM_Rule_Discover_Accounts.get().ProcessMergeMetrics(this.query);
    }

    global void finish(Database.BatchableContext info) {

        Mass_Merge_Config__c massMergeConfigSettings = Mass_Merge_Config__c.getOrgDefaults();
        if (massMergeConfigSettings <> null && massMergeConfigSettings.Disable_All_Services__c) {

            C501_MassMerge_SharedCode.OutputDebugLogText(LoggingLevel.DEBUG, '***** Rule_Discover_Accounts_Batch::finish - Custom Settings - Merge Config : Disable All Services is set to true');
            return;
        }

        // Seconds = Convert from Millisconds / 1000
        Long elapseSeconds = (Datetime.now().getTime() - this.start.getTime()) / 1000;
        C501_MassMerge_SharedCode.OutputDebugLogText(LoggingLevel.DEBUG, '***** Rule_Discover_Accounts_Batch::finish - elapseSeconds: ' + elapseSeconds + ' this.stopProcessing: ' + this.stopProcessing);

        if (elapseSeconds > (60 * 5) || this.stopProcessing || Test.isRunningTest()) {

            String strSoql = C501_MassMerge_SharedCode.GetC501MergeMetricQuery();
            if (String.isBlank(strSoql)) {
                return;
            }

            Rule_Discover_ContactsAccount_Batch batchDiscover = new Rule_Discover_ContactsAccount_Batch();
            batchDiscover.query = strSoql;
            batchDiscover.start = Datetime.now();
        
            if (!Test.isRunningTest()) {
                Database.executeBatch(batchDiscover);
            }
    
            return;
        }

        Rule_Discover_Accounts_Batch batchDiscover = new Rule_Discover_Accounts_Batch();
        batchDiscover.query = this.query;
        batchDiscover.start = this.start;
    
        if (!Test.isRunningTest()) {
            Database.executeBatch(batchDiscover);
        }
    }
}