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

public without sharing class C501_MassMerge_SharedCode { 

    public enum MergeObjectScenarios { Account, Contacts_Within_Account, Contacts_Across_Accounts }

    // Available Fields for merging
    //
    public static Set<String> AddressTypesAvailable = new Set<String> {'Mailing', 'Shipping', 'Other', 'Billing'};
    public static Set<String> AddressFieldsAvailable = new Set<String> {'City', 'Country', 'GeocodeAccuracy', 'Latitude', 'Longitude', 'PostalCode', 'State', 'Street'};

    // public utility used to detect whether the Non Profit Starter Pack is installed in this instance.
    private static boolean fCheckedForNPSP = false;
    public static boolean IsNPSPInstalled {
        get {
            if (!fCheckedForNPSP) {
                Schema.SObjectType token = Schema.getGlobalDescribe().get('npe01__OppPayment__c');
                IsNPSPInstalled = (token != null);
                fCheckedForNPSP = true;
            }
            return IsNPSPInstalled;
        }
        set;
    }   

    // public utility used to get current namespace
    private static boolean fCheckedForCurrentNamespace = false;
    public static String CurrentNamespace {
        get {
            if (!fCheckedForCurrentNamespace) {
                String thisClassName = C501_MassMerge_SharedCode.class.getName();
                if (thisClassName.contains('.')) {
                    CurrentNamespace = thisClassName.split('\\.')[0] + '__';
                }
                fCheckedForCurrentNamespace = true;
            }
            return CurrentNamespace;
        }
        set;
    }   

    private static Boolean fCheckedIsSandbox = false;
    public static Boolean IsSandbox {
        get {
            if (!fCheckedIsSandbox) {
                IsSandbox = true;
                List<Organization> organizations = [SELECT IsSandbox FROM Organization LIMIT 1];
                if (!organizations.isEmpty()) {
                    IsSandbox = Boolean.valueOf(organizations[0].IsSandbox);
                }
                fCheckedIsSandbox = true;
            }

            return IsSandbox;
        }

        set;
    }

    public static List<String> MultiSelectPickListToList(String multiSelectPickList) {

        List<String> multiSelectList = new List<String>();
        
        if (String.isBlank(multiSelectPickList)) {
            return multiSelectList;
        }

        if (multiSelectPickList.contains(';')) {
            multiSelectList = multiSelectPickList.split(';');
        }
        else {
            multiSelectList.add(multiSelectPickList);
        }

        C501_MassMerge_SharedCode.OutputDebugLogText(LoggingLevel.DEBUG, '*****C501_MassMerge_SharedCode::MultiSelectPickListToList multiSelectList: ' + multiSelectList);
        return multiSelectList;
    }

    public static final Integer MAX_RETRY_DATABASE = 5;
    public static Boolean RetryDatabaseError(Database.Error resultError) {

        String statusCode = String.valueOf(resultError.getStatusCode());

        // UNABLE_TO_LOCK_ROW because can happen with all the event firing so retry
        if (statusCode.contains('UNABLE_TO_LOCK_ROW')) {
            return true;
        }

        return false;
    }

    public static Boolean LogDatabaseError(Database.Error resultError, String callingMethod) {

        String statusCode = String.valueOf(resultError.getStatusCode());

        // Ignore UNABLE_TO_LOCK_ROW because can happen with all the event firing - performance issue later to try an figure it out
        if (statusCode.contains('UNABLE_TO_LOCK_ROW')) {
//            return false;
        }

        // Ignore DUPLICATE_VALUE because just trying to insert and not checking if already there to reduce SOQL calls - let the DB block
        if (statusCode.contains('DUPLICATE_VALUE')) {
            return false;
        }

        // Sandbox ignore the INVALID_CROSS_REFERENCE_KEY expection because there is partial data in Sandbox
        if (C501_MassMerge_SharedCode.IsSandbox && (statusCode.contains('INVALID_CROSS_REFERENCE_KEY') || statusCode.contains('INSUFFICIENT_ACCESS_ON_CROSS_REFERENCE_ENTITY'))) {
            return false;
        }

        C501_MassMerge_SharedCode.OutputDebugLogText(LoggingLevel.ERROR, '***** ' + callingMethod + ' Database.SaveResult Error');
        C501_MassMerge_SharedCode.OutputDebugLogText(LoggingLevel.ERROR, statusCode + ': ' + resultError.getMessage());
        C501_MassMerge_SharedCode.OutputDebugLogText(LoggingLevel.ERROR, 'Fields that affected this error: ' + resultError.getFields());

        return true;
    }

    public static String GetMergeContactHouseholdId(C501_Contact_Merge__c mergeObject) {
        return mergeObject.Contact__r.AccountId;
    }

    public static String GetMergeTargetContactHouseholdId(C501_Contact_Merge__c mergeObject) {
        return mergeObject.Merge_Target_Contact__r.AccountId;
    }

    public static String GetMergeAccountHouseholdId(C501_Account_Merge__c mergeObject) {
        return mergeObject.Account__c;
    }

    public static String GetMergeTargetAccountHouseholdId(C501_Account_Merge__c mergeObject) {
        return mergeObject.Merge_Target_Account__c;
    }

    //
    //  Logging Thread: https://salesforce.stackexchange.com/questions/10624/can-i-prevent-an-insert-to-be-rolled-back-even-if-my-class-later-runs-into-an/10626
    //      Ideas on how to structure for future improvement ideas
    //
    private static List<C501_Debug_Log__c> insertDebugLogs = new List<C501_Debug_Log__c>();
    public static void OutputDebugLogFlush() {

        if (Test.isRunningTest()) {
            return;
        }

        // Clear out logs > 24 hours
        List<C501_Debug_Log__c> deleteDebugLogs = [SELECT Id FROM C501_Debug_Log__c WHERE isDeleted = false AND CreatedDate < TODAY LIMIT :GetMaximumDMLLimitSize()];
        if (!deleteDebugLogs.isEmpty()) {
            Database.delete(deleteDebugLogs, false);
        }


        if (!insertDebugLogs.isEmpty()) {

            Boolean insertError = false;
            Database.SaveResult[] insertResults = Database.insert(insertDebugLogs, false);
            for (Database.SaveResult result : insertResults) {
                if (!result.isSuccess()) {
                    insertError = true;
                    for (Database.Error resultError : result.getErrors()) {
                        C501_MassMerge_SharedCode.LogDatabaseError(resultError, '*****C501_MassMerge_SharedCode::OutputDebugLogFlush - insert Debug Logs');
                    }                        
                }
            }

            if (!insertError || (insertDebugLogs.size() > (C501_MassMerge_SharedCode.GetMaximumCollectionSize() / 2))) {
                insertDebugLogs.clear();
            }
        }
    }

    static final Integer MAX_DEBUG_LOG_TEXT_SIZE = 32768;
    public static void OutputDebugLogText(LoggingLevel logLevel, String logMessage) {
        
        System.debug(logLevel, logMessage);

        Mass_Merge_Config__c massMergeConfigSettings = Mass_Merge_Config__c.getOrgDefaults();
        Boolean enableDebugLogging = massMergeConfigSettings == null ? false : massMergeConfigSettings.Enable_Debug_Logging__c;
        Boolean enableErrorLogging = massMergeConfigSettings == null ? false : massMergeConfigSettings.Enable_Error_Logging__c;
        String logFilters = massMergeConfigSettings == null ? '' : massMergeConfigSettings.Log_Filter__c;

        // Check to disable logging
        if ((enableDebugLogging || enableErrorLogging) && massMergeConfigSettings <> null && massMergeConfigSettings.LastModifiedDate <> null && !String.isBlank(massMergeConfigSettings.Id)) {
            if (massMergeConfigSettings.LastModifiedDate < DateTime.now().addDays(-1)) {

                massMergeConfigSettings.Enable_Debug_Logging__c = false;
                massMergeConfigSettings.Enable_Error_Logging__c = false;

                Database.SaveResult result = Database.update(massMergeConfigSettings, false);
                for (Database.Error resultError : result.getErrors()) {
                    C501_MassMerge_SharedCode.LogDatabaseError(resultError, '*****C501_MassMerge_SharedCode::OutputDebugLogText - Update Custom Settings');
                }                        
            }
        }

        // Check if logMessage should be ignored
        Boolean ignoreLogMessage = false;
        if (!String.isBlank(logFilters)) {
            ignoreLogMessage = true;
            for (String logFilter :logFilters.split(',')) {
                if (logMessage.containsIgnoreCase(logFilter)) {
                    ignoreLogMessage = false;
                    break;
                }
            }
        }

        if (enableDebugLogging && logLevel == LoggingLevel.DEBUG && !ignoreLogMessage) {
            insertDebugLogs.add(new C501_Debug_Log__c(
                Log_Level__c = logLevel.name(),
                Debug_Text_Long__c = logMessage.Left(MAX_DEBUG_LOG_TEXT_SIZE)));
        }

        if (enableErrorLogging && logLevel == LoggingLevel.ERROR && !ignoreLogMessage) {
            insertDebugLogs.add(new C501_Debug_Log__c(
                Log_Level__c = logLevel.name(),
                Debug_Text_Long__c = logMessage.Left(MAX_DEBUG_LOG_TEXT_SIZE)));
        }

        if (insertDebugLogs.size() > 199) {
            C501_MassMerge_SharedCode.OutputDebugLogFlush();
        }
    }

    public static String GetSafeSoqlList(List<String> strList) {

        List<String> strCleanList = new List<String>();
        for (String strToken :strList) {
            if (String.isBlank(strToken)) {
                continue;
            }
            strCleanList.add(String.valueOf(strToken));
        }

        if (strCleanList.isEmpty()) {
            return '(\'\')';
        }

        String safeSoqlList = '(\'' + String.join(strCleanList, '\',\'') + '\')';

        return safeSoqlList;
    }

    public static String GetSafeSoqlList(Set<String> strSet) {

        if (strSet.isEmpty()) {
            return '(\'\')';
        }

        List<String> strList = new List<String>();
        for (String strToken :strSet) {
            strList.add(String.valueOf(strToken));
        }

        return C501_MassMerge_SharedCode.GetSafeSoqlList(strList);
    }

    public static String GetSafeSoqlList(Set<Id> idSet) {

        if (idSet.isEmpty()) {
            return '(\'\')';
        }

        List<String> strList = new List<String>();
        for (Id idToken :idSet) {
            strList.add(String.valueOf(idToken));
        }

        return C501_MassMerge_SharedCode.GetSafeSoqlList(strList);
    }

    /*******************************************************************************************************
    * @description returns whether the field should be treated as an NPSP hidden field, typically one
    * of our deprecated or system fields
    * @return boolean true if should be treated as hidden, false if not.
    */
    public static boolean isNPSPHiddenField(string strField) {
        list<string> listStrField = new list<string>{
            'npe01__SystemAccountProcessor__c',
            'npo02__SystemHouseholdProcessor__c',
            'npe01__SystemIsIndividual__c',
            'Current_Address__c'
        };

        for (string strF : listStrField) {
           if (strField.equalsIgnoreCase(strF))
               return true;
        }
        return false;
    }

    public static String GetAllIterableFields(String sObjectType) {

        Set<String> fieldNames = schema.describeSObjects(new String[] { sObjectType })[0].fields.getMap().keyset();
        List<String> iterableFields = new List<String>(fieldNames);

        return String.join(iterableFields, ',');
    }

    public static String GetC501MergeMetricQuery() {

        if (NearSalesforceLimits()) {
            return '';
        }

        String strSoql = 'SELECT';
        strSoql += ' Id, Name, Confidence__c,';
        strSoql += ' Total_Unassigned_Accounts__c, Total_Unassigned_Contacts__c,';
        strSoql += ' Source_Report__c, Source_Report_LastModifiedDate__c, Source_Object__c, Source_Object_Account_Id__c, Source_Object_Contact_Id__c,';
        strSoql += ' AutoMerge_Percentage__c, AutoMerge_Objects__c,';
        strSoql += ' Last_Discover_Date__c, Last_ContactsAccount_Discover_Date__c, Last_ContactsAccounts_Discover_Date__c, Last_Merge_Date__c,';
        strSoql += ' Last_Account_Discover_GroupByHavingCount__c, Last_Contact_Discover_GroupByHavingCount__c,';
        strSoql += ' Enable_Account_Merge_Discovery__c, Enable_Contact_Merge_Discovery__c, Discover_AutoMerge_Objects_Only__c';
        strSoql += ' FROM C501_Merge_Metric__c';
        strSoql += ' WHERE isDeleted = false';
        strSoql += ' AND Source_Object__c <> NULL AND Source_Report__c <> NULL AND (Source_Object_Account_Id__c <> NULL OR Source_Object_Contact_Id__c <> NULL)';

        if (Test.isRunningTest()) {        
            strSoql += ' AND Name LIKE \'TESTDATA%\'';
        }

        return strSoql;
    }

    public static String GetC501AccountMergeQuery() {

        if (NearSalesforceLimits()) {
            return '';
        }

        String strSoql = 'SELECT';
        strSoql += ' Id, Unique_Id__c,';
        strSoql += ' Account__c, Account__r.IsDeleted,';
        strSoql += ' Merge_Target_Account__c, Merge_Target_Account__r.IsDeleted,';
        strSoql += ' Merge_Confidence__c, Merge_Action__c,';
        strSoql += ' Merge_Metric__c, Merge_Metric__r.Name';
        strSoql += ' FROM C501_Account_Merge__c';
        strSoql += ' WHERE isDeleted = false';
        strSoql += ' AND Merge_Action__c <> \'Ignore\'';

        if (Test.isRunningTest()) {        
            strSoql += ' AND Merge_Metric__r.Name LIKE \'TESTDATA%\'';
        }

        return strSoql;
    }

    public static String GetC501ContactMergeQuery() {

        if (NearSalesforceLimits()) {
            return '';
        }

        String strSoql = 'SELECT';
        strSoql += ' Id, Unique_Id__c,';
        strSoql += ' Contact__c, Contact__r.AccountId, Contact__r.IsDeleted,';
        strSoql += ' Merge_Target_Contact__c, Merge_Target_Contact__r.AccountId, Merge_Target_Contact__r.IsDeleted,';
        strSoql += ' Merge_Confidence__c, Merge_Action__c,';
        strSoql += ' Merge_Metric__c, Merge_Metric__r.Name';
        strSoql += ' FROM C501_Contact_Merge__c';
        strSoql += ' WHERE isDeleted = false';
        strSoql += ' AND Merge_Action__c <> \'Ignore\'';

        if (Test.isRunningTest()) {        
            strSoql += ' AND Merge_Metric__r.Name LIKE \'TESTDATA%\'';
        }

        return strSoql;
    }

    public static void InitializeMassMergeInstances() {

        C501_MassMerge_SharedCode.ScheduleJobs();

        String reportFolder = 'Mass Merge Rules';
        if (Test.isRunningTest()) {
            reportFolder = 'Mass Merge Tests';
        }
        
        String strSoql = C501_MassMerge_SharedCode.GetC501MergeMetricQuery();
        if (String.isBlank(strSoql)) {
            return;
        }

        C501_MassMerge_SharedCode.OutputDebugLogText(LoggingLevel.DEBUG, '*****C501_MassMerge_SharedCode::GetC501MergeMetricQuery strSoql: ' + strSoql);

        List<C501_Merge_Metric__c> mergeMetrics = Database.Query(strSoql);

        // Initialize Merge Metrics - make sure there is a merge metric for each report in Mass Merge folder
        List<C501_Merge_Metric__c> mergeMetricsInsert = new List<C501_Merge_Metric__c>();
        for (Report report :[
            SELECT Id, DeveloperName, FolderName, Name
            FROM Report
            WHERE FolderName = :reportFolder]) {

                Boolean foundMergeMetric = false;
                for (C501_Merge_Metric__c mergeMetric :mergeMetrics) {

                    if (mergeMetric.Source_Report__c == (String)report.Id || mergeMetric.Source_Report__c == report.DeveloperName) {

                        foundMergeMetric = true;
                        break;
                    }
                }

                if (!foundMergeMetric) {

                    mergeMetricsInsert.add( new C501_Merge_Metric__c (
                        Name = (Test.isRunningTest() ? ('TESTDATA ' + report.Name) : report.Name),
                        Confidence__c = 90.0,
                        Source_Report__c = report.DeveloperName,
                        Source_Object__c = (report.DeveloperName.contains('Account') ? 'Account' : 'Contact'),
                        Source_Object_Account_Id__c = (report.DeveloperName.contains('Account') ? 'Id' : 'AccountId'),
                        Source_Object_Contact_Id__c = (report.DeveloperName.contains('Contact') ? 'Id' : 'Id'),
                        AutoMerge_Percentage__c = 100.0,
                        Unique_Id__c = report.Id
                    ));
                }
        }

        if (!mergeMetricsInsert.isEmpty()) {

            Database.SaveResult[] insertResults = Database.insert(mergeMetricsInsert, false);
            for (Database.SaveResult result : insertResults) {
                if (!result.isSuccess()) {
                    for(Database.Error resultError : result.getErrors()) {
                        C501_MassMerge_SharedCode.LogDatabaseError(resultError, 'C501_MassMerge_SharedCode::InitializeMassMergeInstances');
                    }                        
                }
            }
        }
    }

    public static void DeleteMergeMetricChildren(Set<Id> mergeMetricIds) {

        String strSoqlDeleteAccountMerges = C501_MassMerge_SharedCode.GetC501AccountMergeQuery();
        if (String.isBlank(strSoqlDeleteAccountMerges)) {
            return;
        }
        strSoqlDeleteAccountMerges += ' AND Merge_Metric__c in ' + C501_MassMerge_SharedCode.GetSafeSoqlList(mergeMetricIds);

        C501_MassMerge_SharedCode.OutputDebugLogText(LoggingLevel.DEBUG, '***** C501_MassMerge_SharedCode::DeleteMergeMetricChildren strSoqlDeleteAccountMerges: ' + strSoqlDeleteAccountMerges);
        List<C501_Account_Merge__c> deleteAccountMerges = Database.query(strSoqlDeleteAccountMerges);
        if (!deleteAccountMerges.isEmpty()) {
            Database.delete(deleteAccountMerges, false);
        }

        String strSoqlDeleteContactMerges = C501_MassMerge_SharedCode.GetC501ContactMergeQuery();
        if (String.isBlank(strSoqlDeleteContactMerges)) {
            return;
        }
        strSoqlDeleteContactMerges += ' AND Merge_Metric__c in ' + C501_MassMerge_SharedCode.GetSafeSoqlList(mergeMetricIds);

        C501_MassMerge_SharedCode.OutputDebugLogText(LoggingLevel.DEBUG, '***** C501_MassMerge_SharedCode::DeleteMergeMetricChildren strSoqlDeleteContactMerges: ' + strSoqlDeleteContactMerges);

        List<C501_Contact_Merge__c> deleteContactMerges = Database.query(strSoqlDeleteContactMerges);
        if (!deleteContactMerges.isEmpty()) {
            Database.delete(deleteContactMerges, false);
        }
    }
   
    /*
        @Param:List of selectOption to be sort.
        @Return: Sorted list of selectOptions by Label
    */

    public static List<selectOption> SelectOptionSortByLabel(List<selectOption> selectOptionsToSort) {

        if (selectOptionsToSort == null || selectOptionsToSort.size() <= 1) {
            return selectOptionsToSort;
        }

        List<SelectOption> lessSelectOption = new List<SelectOption>();
        List<SelectOption> greaterSelectOption = new List<SelectOption>();
        Integer pivot = selectOptionsToSort.size() / 2;
        
        //save the pivot and remove it FROM the selectOption list
        SelectOption pivotValue = selectOptionsToSort[pivot];
        selectOptionsToSort.remove(pivot);
        for(selectOption SO : selectOptionsToSort){
            if(SO.getLabel() <= pivotValue.getLabel()){
                lessSelectOption.add(SO);
            }else if(SO.getLabel() > pivotValue.getLabel()){
                greaterSelectOption.add(SO);   
            }
        }

        List<selectOption> sortedSelectOptions = new List<selectOption>(); 
        sortedSelectOptions.addAll(SelectOptionSortByLabel(lessSelectOption));
        sortedSelectOptions.add(pivotValue);
        sortedSelectOptions.addAll(SelectOptionSortByLabel(greaterSelectOption));

        return SortedSelectOptions;
    }

    public static void ProcessMergeCandidates(C501_Merge_Metric__c mergeMetric, List<C501_Account_Merge__c> mergeAccounts) {

        C501_MassMerge_SharedCode_Account c501MassMerge_SharedCode_Account = new C501_MassMerge_SharedCode_Account();
        c501MassMerge_SharedCode_Account.InsertObjectMergeCandidates(mergeMetric, mergeAccounts);

        c501MassMerge_SharedCode_Account.ProcessObjectMergeCandidates(mergeMetric);
    }

    public static Integer ProcessObjectMergeCandidates_Account(C501_Merge_Metric__c mergeMetric, List<C501_Account_Merge__c> pendingAutoMerges, Map<Id, Id> contactHouseholdMap) {

        List<String> automergeObjects = C501_MassMerge_SharedCode.MultiSelectPickListToList(mergeMetric.AutoMerge_Objects__c);
        if (automergeObjects.contains(C501_MassMerge_SharedCode.MergeObjectScenarios.Account.name())) {

            if (!pendingAutoMerges.isEmpty()) {
                C501_MassMerge_SharedCode_Account c501MassMerge_SharedCode_Account = new C501_MassMerge_SharedCode_Account();
                return c501MassMerge_SharedCode_Account.ProcessObjectMerges(new List<C501_Merge_Metric__c> {mergeMetric}, pendingAutoMerges, false);
            }
        }

        return 0;
    }

    public static Integer ProcessObjectMergeCandidates_Contact(C501_Merge_Metric__c mergeMetric, List<C501_Contact_Merge__c> pendingAutoMerges, Map<Id, Id> contactHouseholdMap) {

        List<String> automergeObjects = C501_MassMerge_SharedCode.MultiSelectPickListToList(mergeMetric.AutoMerge_Objects__c);
        if (automergeObjects.contains(C501_MassMerge_SharedCode.MergeObjectScenarios.Contacts_Within_Account.name().replace('_', ' ')) || automergeObjects.contains(C501_MassMerge_SharedCode.MergeObjectScenarios.Contacts_Across_Accounts.name().replace('_', ' '))) {

            List<C501_Contact_Merge__c> pendingAutoMergeCandidates = new List<C501_Contact_Merge__c>();
            for (C501_Contact_Merge__c pendingAutoMerge :pendingAutoMerges) {

                if (!contactHouseholdMap.containsKey(pendingAutoMerge.Contact__c) || !contactHouseholdMap.containsKey(pendingAutoMerge.Merge_Target_Contact__c)) {
                    continue;
                }

                // Check for valid household - for instance might not be NPSP households so Account could be null for a contact
                if (contactHouseholdMap.get(pendingAutoMerge.Contact__c) == null || contactHouseholdMap.get(pendingAutoMerge.Merge_Target_Contact__c) == null) {
                    continue;
                }

                if (automergeObjects.contains(C501_MassMerge_SharedCode.MergeObjectScenarios.Contacts_Within_Account.name().replace('_', ' ')) && contactHouseholdMap.get(pendingAutoMerge.Contact__c) == contactHouseholdMap.get(pendingAutoMerge.Merge_Target_Contact__c)) {
                    pendingAutoMergeCandidates.add(pendingAutoMerge);
                }
                
                if (automergeObjects.contains(C501_MassMerge_SharedCode.MergeObjectScenarios.Contacts_Across_Accounts.name().replace('_', ' ')) && contactHouseholdMap.get(pendingAutoMerge.Contact__c) != contactHouseholdMap.get(pendingAutoMerge.Merge_Target_Contact__c)) {
                    pendingAutoMergeCandidates.add(pendingAutoMerge);
                }
            }

            if (!pendingAutoMergeCandidates.isEmpty()) {
                C501_MassMerge_SharedCode_Contact c501MassMerge_SharedCode_Contact = new C501_MassMerge_SharedCode_Contact();
                return c501MassMerge_SharedCode_Contact.ProcessObjectMerges(new List<C501_Merge_Metric__c> {mergeMetric}, pendingAutoMergeCandidates, true);
            }
        }

        return 0;
    }

    public static void ScheduleJobs() {

        List<CronJobDetail> cronJobDetails = [SELECT Id, JobType, Name FROM CronJobDetail];
        Set<String> scheduledJobs = new Set<String>();
        for (CronJobDetail jobDetail :cronJobDetails) {

            scheduledJobs.add(jobDetail.Name);
        }

        String cronExpression, scheduleJobName;
        for (Integer i = 5; i <= 59; i += 5) {

            cronExpression = '0 ' + String.valueOf(i) + ' * * * ?';

            try {
                if (math.mod(i, 35) == 0) {

                    scheduleJobName = C501_MM_Rule_Discover_Accounts.class.getName() + '_' + (i <= 9 ? '0' : '') + String.valueOf(i);
                    if (!scheduledJobs.contains(scheduleJobName) && !Test.isRunningTest()) {
                        System.schedule(scheduleJobName, cronExpression, new C501_MM_Rule_Discover_Accounts());
                    }
                }
                else if (math.mod(i, 40) == 0) {

                    scheduleJobName = C501_MM_Rule_Discover_ContactsAccount.class.getName() + '_' + (i <= 9 ? '0' : '') + String.valueOf(i);
                    if (!scheduledJobs.contains(scheduleJobName) && !Test.isRunningTest()) {
                        System.schedule(scheduleJobName, cronExpression, new C501_MM_Rule_Discover_ContactsAccount());
                    }
                }
                else if (math.mod(i, 45) == 0) {

                    scheduleJobName = C501_MM_Rule_Discover_ContactsAccounts.class.getName() + '_' + (i <= 9 ? '0' : '') + String.valueOf(i);
                    if (!scheduledJobs.contains(scheduleJobName) && !Test.isRunningTest()) {
                        System.schedule(scheduleJobName, cronExpression, new C501_MM_Rule_Discover_ContactsAccounts());
                    }
                }
                else {
                    scheduleJobName = C501_MM_Rule_Merge.class.getName() + '_' + (i <= 9 ? '0' : '') + String.valueOf(i);

                    if (scheduledJobs.contains(scheduleJobName)) {
                        continue;
                    }

                    if (!Test.isRunningTest()) {
                        System.schedule(scheduleJobName, cronExpression, new C501_MM_Rule_Merge());
                    }
                }
            }
            catch (System.AsyncException exceptionAsync) {
                // Ignore Exception because is related to multiple futures trying to create schedule at the same time
            }
        }
    }

    public static void TestSetupMerge() {

        C501_MassMerge_SharedCode.InitializeMassMergeInstances();

        List<Account> listAccounts = new List<Account>();   
        Integer cAccounts = 10;     
        Integer matchIndex = 1;
        for (Integer i = 0; i < cAccounts; i++) {

            if (math.mod((i + 2), 2) == 0) {
                matchIndex++;
            }

            Account account = new Account(
                
                Name='TESTDATA Flintstone Household',
                BillingStreet = String.valueOf(matchIndex) + ' BillingStreet',
                BillingCity = 'BillingCity',
                BillingState = 'WA',
                BillingPostalCode =  '12345',
                Website = 'https://www.flintstone.org' + String.valueOf(matchIndex)
            );

            listAccounts.add(account);
        }                      

        insert listAccounts;

        Map<Id, Opportunity> listOpportunityHouseholds = new Map<Id, Opportunity>();
        List<Contact> listContacts = new List<Contact>();
        for (Integer i = 0; i < cAccounts; i++) {

            if (math.mod((i + 2), 2) == 0) {
                matchIndex++;
            }

            listOpportunityHouseholds.put(listAccounts[i].Id, new Opportunity(
                Name = 'TESTDATA Name ' + listAccounts[i].Id,
                StageName = 'Closed Won',
                CloseDate = System.today()
            ));

            listContacts.add( new Contact(
                LastName = 'TESTDATA Flintstone' + String.valueOf(matchIndex),
                FirstName = 'Fred' + String.valueOf(matchIndex),
                AccountId = listAccounts[i].Id,
                Birthdate = date.today().addYears(-21 - matchIndex),
                Email = String.valueOf(matchIndex) + 'fred@bedrock.org',
                Gender__c = 'Male'
            ));

            listContacts.add( new Contact(
                LastName = 'TESTDATA Flintstone' + String.valueOf(matchIndex),
                FirstName = 'Fred' + String.valueOf(matchIndex),
                AccountId = listAccounts[i].Id,
                Birthdate = date.today().addYears(-21 - matchIndex),
                Email = String.valueOf(matchIndex) + 'fred@bedrock.org',
                Gender__c = 'Male'
            ));

            listContacts.add( new Contact(
                LastName = 'TESTDATA Flintstone' + String.valueOf(matchIndex),
                FirstName = 'Wilma' + String.valueOf(matchIndex),
                AccountId = listAccounts[i].Id,
                Birthdate = date.today().addYears(-21 - matchIndex),
                Email = String.valueOf(matchIndex) + 'wilma@bedrock.org',
                Gender__c = 'Female'
            ));

            listContacts.add( new Contact(
                LastName = 'TESTDATA Flintstone' + String.valueOf(matchIndex),
                FirstName = 'Dino' + String.valueOf(matchIndex),
                AccountId = listAccounts[i].Id,
                Birthdate = date.today().addYears(-2 - matchIndex),
                Email = String.valueOf(matchIndex) + 'dino@bedrock.org'
            ));
        }

        if (!listOpportunityHouseholds.isEmpty() && !listOpportunityHouseholds.values().isEmpty()) {
            insert listOpportunityHouseholds.values();
        }

        if (!listContacts.isEmpty()) {
            insert listContacts;
        }

        //C501_MassMerge_SharedCode.PrintSalesforceStructure();
    }

    public static List<C501_Merge_Metric__c> TestDiscoverHelper(String queryNameCriteria) {

        C501_MassMerge_SharedCode.TestSetupMerge();

        // Reset report change detected during a merge process call
        C501_MM_Rule_Merge.get().execute(null);

        String strSoql = C501_MassMerge_SharedCode.GetC501MergeMetricQuery();
        if (String.isBlank(strSoql)) {
            return new List<C501_Merge_Metric__c>();
        }
        strSoql += ' AND Name LIKE \'%' + queryNameCriteria + '%\'';

        C501_MassMerge_SharedCode.OutputDebugLogText(LoggingLevel.DEBUG, '*****C501_MassMerge_SharedCode::TestDiscoverHelper strSoql: ' + strSoql);

        List<C501_Merge_Metric__c> mergeMetrics = Database.Query(strSoql);
        System.Assert(mergeMetrics.size() > 0);
        for (C501_Merge_Metric__c mergeMetric :mergeMetrics) {

            C501_MM_Rule_Discover_Accounts.get().executeMetricById(mergeMetric.Id);
            C501_MM_Rule_Discover_ContactsAccount.get().executeMetricById(mergeMetric.Id);
            C501_MM_Rule_Discover_ContactsAccounts.get().executeMetricById(mergeMetric.Id);
            mergeMetric.AutoMerge_Objects__c = 'Account;Contacts Within Account;Contacts Across Accounts';
        }

        Database.SaveResult[] updateResults = Database.update(mergeMetrics, false);
        for (Database.SaveResult updateResult :updateResults) {
            if (!updateResult.isSuccess()) {
                for(Database.Error resultError : updateResult.getErrors()) {
                    C501_MassMerge_SharedCode.LogDatabaseError(resultError, 'C501_MassMerge_SharedCode::TestDiscoverHelper');
                }                        
            }
        }

        //C501_MassMerge_SharedCode.PrintSalesforceStructure();

        return mergeMetrics;
    }

    public static String TestHelperAutoMergeObjects(String sourceObject, String autoMergeType) {

        if (sourceObject.equals('Account')) {
            if (autoMergeType.equals('Object')) {
                return C501_MassMerge_SharedCode.MergeObjectScenarios.Account.name();
            }
        }
        else if (sourceObject.equals('Contact')) {
            if (autoMergeType.equals('ObjectsWithinObject')) {
                return C501_MassMerge_SharedCode.MergeObjectScenarios.Contacts_Within_Account.name().replace('_', ' ');
            }
            else if (autoMergeType.equals('ObjectsAcrossObjects')) {
                return C501_MassMerge_SharedCode.MergeObjectScenarios.Contacts_Across_Accounts.name().replace('_', ' ');
            }
        }

        return null;
    }

    public static void PrintSalesforceLimits() {

        C501_MassMerge_SharedCode.OutputDebugLogText(LoggingLevel.DEBUG, '***** C501_MassMerge_SharedCode::PrintSalesforceLimits - Aggregate Queries ' + Limits.getAggregateQueries() + ' of ' + Limits.getLimitAggregateQueries());
        C501_MassMerge_SharedCode.OutputDebugLogText(LoggingLevel.DEBUG, '***** C501_MassMerge_SharedCode::PrintSalesforceLimits - DML Rows ' + Limits.getDMLRows() + ' of ' + Limits.getLimitDMLRows());
        C501_MassMerge_SharedCode.OutputDebugLogText(LoggingLevel.DEBUG, '***** C501_MassMerge_SharedCode::PrintSalesforceLimits - DML Statments ' + Limits.getDMLStatements() + ' of ' + Limits.getLimitDMLStatements());
        C501_MassMerge_SharedCode.OutputDebugLogText(LoggingLevel.DEBUG, '***** C501_MassMerge_SharedCode::PrintSalesforceLimits - Queies ' + Limits.getQueries() + ' of ' + Limits.getLimitQueries());
        C501_MassMerge_SharedCode.OutputDebugLogText(LoggingLevel.DEBUG, '***** C501_MassMerge_SharedCode::PrintSalesforceLimits - Query Rows ' + Limits.getQueryRows() + ' of ' + Limits.getLimitQueryRows());
        C501_MassMerge_SharedCode.OutputDebugLogText(LoggingLevel.DEBUG, '***** C501_MassMerge_SharedCode::PrintSalesforceLimits - Sosl Queries ' + Limits.getSoslQueries() + ' of ' + Limits.getLimitSoslQueries());
    }

    public static Integer GetMaximumDMLLimitSize() {

        Integer percentageMultiple = 98;
        Integer maximumLimitSize = ((Limits.getLimitQueryRows() - Limits.getQueryRows()) / 100) * percentageMultiple;
        Integer maximumDMLLimitSize = ((Limits.getLimitDMLRows() - Limits.getDMLRows()) / 100) * percentageMultiple;


        return maximumDMLLimitSize < maximumLimitSize ? maximumDMLLimitSize : maximumLimitSize;
    }

    public static Integer GetMaximumLimitSize(Integer groupByHavingCountMax) {

        C501_MassMerge_SharedCode.OutputDebugLogText(LoggingLevel.DEBUG, '***** C501_MassMerge_SharedCode::GetMaximumLimitSize - groupByHavingCountMax: ' + groupByHavingCountMax);

        Integer percentageMultiple = 98;
        Integer maximumLimitSize = ((Limits.getLimitQueryRows() - Limits.getQueryRows()) / 100) * percentageMultiple;
        C501_MassMerge_SharedCode.OutputDebugLogText(LoggingLevel.DEBUG, '***** C501_MassMerge_SharedCode::GetMaximumLimitSize - Limits.getLimitQueryRows(): ' + Limits.getLimitQueryRows() + ' Limits.getQueryRows(): ' + Limits.getQueryRows());

        // Check for groupByHavingCountMax because each aggregate row is a count of all the rows to create the aggregate
        //  https://developer.salesforce.com/docs/atlas.en-us.apexcode.meta/apexcode/langCon_apex_SOQL_agg_fns.htm
        if (groupByHavingCountMax > 1) {
            return maximumLimitSize / groupByHavingCountMax;
        }

        return maximumLimitSize;
    }

    public static Integer GetMaximumCollectionSize() {

        // Salesforce max collection size is 1000
        return C501_MassMerge_SharedCode.GetMaximumLimitSize(1) < 999 ? C501_MassMerge_SharedCode.GetMaximumLimitSize(1) : 999;
    }

    public static Boolean NearSalesforceLimits() {

        Boolean nearLimit = false;
        Integer percentageMultiple = 95;

        // https://developer.salesforce.com/docs/atlas.en-us.apexcode.meta/apexcode/apex_methods_system_limits.htm

        if (Limits.getAggregateQueries() > ((Limits.getLimitAggregateQueries() / 100) * percentageMultiple)) {
            C501_MassMerge_SharedCode.OutputDebugLogText(LoggingLevel.WARN, '***** C501_MassMerge_SharedCode::NearSalesforceLimits - Aggregate Queries ' + String.valueOf(percentageMultiple) + '% of Limit - ' + Limits.getAggregateQueries() + ' of ' + Limits.getLimitAggregateQueries());
            nearLimit = true;
        }

        if (Limits.getDMLRows() > ((Limits.getLimitDMLRows() / 100) * percentageMultiple)) {
            C501_MassMerge_SharedCode.OutputDebugLogText(LoggingLevel.WARN, '***** C501_MassMerge_SharedCode::NearSalesforceLimits - DML Rows ' + String.valueOf(percentageMultiple) + '% of Limit - ' + Limits.getDMLRows() + ' of ' + Limits.getLimitDMLRows());
            nearLimit = true;
        }

        if (Limits.getDMLStatements() > ((Limits.getLimitDMLStatements() / 100) * percentageMultiple)) {
            C501_MassMerge_SharedCode.OutputDebugLogText(LoggingLevel.WARN, '***** C501_MassMerge_SharedCode::NearSalesforceLimits - DML Statments ' + String.valueOf(percentageMultiple) + '% of Limit - ' + Limits.getDMLStatements() + ' of ' + Limits.getLimitDMLStatements());
            nearLimit = true;
        }

        if (Limits.getQueries() > ((Limits.getLimitQueries() / 100) * percentageMultiple)) {
            C501_MassMerge_SharedCode.OutputDebugLogText(LoggingLevel.WARN, '***** C501_MassMerge_SharedCode::NearSalesforceLimits - Queies ' + String.valueOf(percentageMultiple) + '% of Limit - ' + Limits.getQueries() + ' of ' + Limits.getLimitQueries());
            nearLimit = true;
        }

        if (Limits.getQueryRows() > ((Limits.getLimitQueryRows() / 100) * percentageMultiple)) {
            C501_MassMerge_SharedCode.OutputDebugLogText(LoggingLevel.WARN, '***** C501_MassMerge_SharedCode::NearSalesforceLimits - Query Rows ' + String.valueOf(percentageMultiple) + '% of Limit - ' + Limits.getQueryRows() + ' of ' + Limits.getLimitQueryRows());
            nearLimit = true;
        }

        if (Limits.getSoslQueries() > ((Limits.getLimitSoslQueries() / 100) * percentageMultiple)) {
            C501_MassMerge_SharedCode.OutputDebugLogText(LoggingLevel.WARN, '***** C501_MassMerge_SharedCode::NearSalesforceLimits - Sosl Queries ' + String.valueOf(percentageMultiple) + '% of Limit - ' + Limits.getSoslQueries() + ' of ' + Limits.getLimitSoslQueries());
            nearLimit = true;
        }

        return nearLimit;
    }

    public static void PrintSalesforceStructure() {

        C501_MassMerge_SharedCode.OutputDebugLogText(LoggingLevel.DEBUG, '***** C501_MassMerge_SharedCode::PrintSalesforceStructure');

        C501_MassMerge_SharedCode.PrintSalesforceLimits();

        for (Account account :[select Id, Name, Website, BillingStreet from Account WHERE Name LIKE 'TESTDATA%']) {
            C501_MassMerge_SharedCode.OutputDebugLogText(LoggingLevel.DEBUG, '***** C501_MassMerge_SharedCode::PrintSalesforceStructure - ' + String.valueOf(account));
        }
        for (Contact contact :[select Id, Account.Name, LastName, FirstName, Birthdate, Email from Contact WHERE LastName LIKE 'TESTDATA%']) {
            C501_MassMerge_SharedCode.OutputDebugLogText(LoggingLevel.DEBUG, '***** C501_MassMerge_SharedCode::PrintSalesforceStructure - ' + String.valueOf(contact));
        }
        for (Opportunity opportunity :[select Id, Name from Opportunity WHERE Name LIKE 'TESTDATA%']) {
            C501_MassMerge_SharedCode.OutputDebugLogText(LoggingLevel.DEBUG, '***** C501_MassMerge_SharedCode::PrintSalesforceStructure - ' + String.valueOf(opportunity));
        }
       
        String strSoql = C501_MassMerge_SharedCode.GetC501MergeMetricQuery();
        if (String.isBlank(strSoql)) {
            return;
        }
        String strSoqlAccountMerges = C501_MassMerge_SharedCode.GetC501AccountMergeQuery();
        if (String.isBlank(strSoqlAccountMerges)) {
            return;
        }
        String strSoqlContactMerges = C501_MassMerge_SharedCode.GetC501ContactMergeQuery();
        if (String.isBlank(strSoqlContactMerges)) {
            return;
        }

        C501_MassMerge_SharedCode.OutputDebugLogText(LoggingLevel.DEBUG, '*****C501_MassMerge_SharedCode::PrintSalesforceStructure strSoql: ' + strSoql);

        for (C501_Merge_Metric__c mergeMetric :Database.Query(strSoql)) {

            C501_MassMerge_SharedCode.OutputDebugLogText(LoggingLevel.DEBUG, '***** C501_MassMerge_SharedCode::PrintSalesforceStructure - Merge Metric: ' + mergeMetric.Name);

            for (C501_Account_Merge__c mergeAccount :Database.query(strSoqlAccountMerges + ' AND Merge_Metric__c = \'' + mergeMetric.Id + '\'')) {
                C501_MassMerge_SharedCode.OutputDebugLogText(LoggingLevel.DEBUG, '***** C501_MassMerge_SharedCode::PrintSalesforceStructure - Merge Accounts: ' + String.valueOf(mergeAccount));
            }
            for (C501_Contact_Merge__c mergeContact :Database.query(strSoqlContactMerges + ' AND Merge_Metric__c = \'' + mergeMetric.Id + '\'')) {
                C501_MassMerge_SharedCode.OutputDebugLogText(LoggingLevel.DEBUG, '***** C501_MassMerge_SharedCode::PrintSalesforceStructure - Merge Contacts: ' + String.valueOf(mergeContact));
            }
        }
    }
}