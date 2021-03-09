global class Rule_Discover_ContactsAccount_Batch implements Database.Batchable<sObject>, Database.AllowsCallouts, Database.Stateful { 

    global String query;
    global Datetime start;
    global Boolean stopProcessing = false;

    global Database.querylocator start(Database.BatchableContext BC) {

        C501_MassMerge_SharedCode.OutputDebugLogText(LoggingLevel.DEBUG, '***** Rule_Discover_ContactsAccount_Batch::start - query: ' + this.query);
        return Database.getQueryLocator(this.query);
    }

    global void execute(Database.BatchableContext info, List<sObject> scope) {

        C501_MassMerge_SharedCode.OutputDebugLogText(LoggingLevel.DEBUG, '***** Rule_Discover_ContactsAccount_Batch::execute - scope: ' + scope);

        Boolean processMergeMetrics = false;
        List<Id> mergeMetricIds = new List<Id>();
        for (sObject scopeRecord :scope) {

            C501_Merge_Metric__c mergeRecord = (C501_Merge_Metric__c)scopeRecord;

            mergeMetricIds.add(mergeRecord.Id);
            C501_MassMerge_SharedCode.OutputDebugLogText(LoggingLevel.DEBUG, '***** Rule_Discover_ContactsAccount_Batch::execute - check having - mergeRecord.Last_Contact_Discover_GroupByHavingCount__c: ' + mergeRecord.Last_Contact_Discover_GroupByHavingCount__c);

            if (mergeRecord.Last_Contact_Discover_GroupByHavingCount__c <> null &&
                ((Integer)mergeRecord.Last_Contact_Discover_GroupByHavingCount__c + C501_MassMerge_SharedCode.INCREMENT_MERGE_CANDIDATE_GROUPBY) >= C501_MassMerge_SharedCode.MaximumDiscoverHavingCount) {
   
                    C501_MassMerge_SharedCode.OutputDebugLogText(LoggingLevel.DEBUG, '***** Rule_Discover_ContactsAccount_Batch::execute - stopProcessing - mergeRecord.Last_Contact_Discover_GroupByHavingCount__c: ' + mergeRecord.Last_Contact_Discover_GroupByHavingCount__c);
                    this.stopProcessing = true;

                Database.update(new C501_Merge_Metric__c(Id = mergeRecord.Id, Last_Contact_Discover_GroupByHavingCount__c = 0.0), false);            
            } else {

                List<String> discoveryObjects = C501_MassMerge_SharedCode.MultiSelectPickListToList(mergeRecord.Discovery_Objects__c);
                C501_MassMerge_SharedCode.OutputDebugLogText(LoggingLevel.DEBUG, '***** Rule_Discover_ContactsAccount_Batch::execute - discoveryObjects: ' + discoveryObjects);
                C501_MassMerge_SharedCode.OutputDebugLogText(LoggingLevel.DEBUG, '***** Rule_Discover_ContactsAccount_Batch::execute - C501_MassMerge_SharedCode.MergeObjectScenarios.Contacts_Within_Account.name(): ' + C501_MassMerge_SharedCode.MergeObjectScenarios.Contacts_Within_Account.name());

                if (discoveryObjects.contains(C501_MassMerge_SharedCode.MergeObjectScenarios.Contacts_Within_Account.name())) {
                    C501_MassMerge_SharedCode.OutputDebugLogText(LoggingLevel.DEBUG, '***** Rule_Discover_ContactsAccount_Batch::execute - processMergeMetrics');
                    processMergeMetrics = true;
                }
            }
        }

        if (!processMergeMetrics) {
            C501_MassMerge_SharedCode.OutputDebugLogText(LoggingLevel.DEBUG, '***** Rule_Discover_ContactsAccount_Batch::execute - !processMergeMetrics - set stopProcessing');
            this.stopProcessing = true;
            return;
        }

        if (this.stopProcessing) {
            return;
        }

		if (mergeMetricIds.isEmpty()) {
			return;
        }

        C501_MM_Rule_Discover_ContactsAccount.get().ProcessMergeMetrics(this.query);
    }

    global void finish(Database.BatchableContext info) {

        Mass_Merge_Config__c massMergeConfigSettings = Mass_Merge_Config__c.getOrgDefaults();
        if (massMergeConfigSettings <> null && massMergeConfigSettings.Disable_All_Services__c) {

            C501_MassMerge_SharedCode.OutputDebugLogText(LoggingLevel.DEBUG, '***** Rule_Discover_ContactsAccount_Batch::finish - Custom Settings - Merge Config : Disable All Services is set to true');
            return;
        }

        // Seconds = Convert from Millisconds / 1000
        Long elapseSeconds = (Datetime.now().getTime() - this.start.getTime()) / 1000;
        C501_MassMerge_SharedCode.OutputDebugLogText(LoggingLevel.DEBUG, '***** Rule_Discover_ContactsAccount_Batch::finish - elapseSeconds: ' + elapseSeconds + ' this.stopProcessing: ' + this.stopProcessing);

        // Check if there is a backlog of Merge Queries - don't want to create more if wasn't able to process the previous Merge Queries
        if (elapseSeconds > (60 * 5) || this.stopProcessing || Test.isRunningTest()) {

            String strSoql = C501_MassMerge_SharedCode.GetC501MergeMetricQuery();
            if (String.isBlank(strSoql)) {
                return;
            }
    
            Rule_Discover_ContactsAccounts_Batch batchDiscover = new Rule_Discover_ContactsAccounts_Batch();
            batchDiscover.query = strSoql;
            batchDiscover.start = Datetime.now();

            if (!Test.isRunningTest()) {
                Database.executeBatch(batchDiscover);
            }
    
            return;
        }

        Rule_Discover_ContactsAccount_Batch batchDiscover = new Rule_Discover_ContactsAccount_Batch();
        batchDiscover.query = this.query;
        batchDiscover.start = this.start;
    
        if (!Test.isRunningTest()) {
            Database.executeBatch(batchDiscover);
        }
    }
}