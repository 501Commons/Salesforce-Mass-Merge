/*
    Copyright (c) 2016, Salesforce.org
    All rights reserved.
    
    Redistribution and use in source and binary forms, with or without
    modification, are permitted provided that the following conditions are met:
    
    * Redistributions of source code must retain the above copyright
      notice, this list of conditions and the following disclaimer.
    * Redistributions in binary form must reproduce the above copyright
      notice, this list of conditions and the following disclaimer in the
      documentation and/or other materials provided with the distribution.
    * Neither the name of Salesforce.org nor the names of
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

public virtual without sharing class C501_CTRL_MassMergeAccounts {

    private C501_MassMerge_SharedCode_Account c501MassMerge_SharedCode_Account;
    private List<String> listStrFields;
    
    public boolean redirect { get; private set; }
    public ID idRedirect { get; private set; }
    
    // constructor
    public C501_CTRL_MassMergeAccounts() {

        C501_MassMerge_SharedCode.OutputDebugLogText(LoggingLevel.DEBUG, '***** C501_CTRL_MassMergeAccounts::C501_CTRL_MassMergeAccounts Constructor');

        redirect = false;
        c501MassMerge_SharedCode_Account = new C501_MassMerge_SharedCode_Account();
        
        // Get list of fields FROM fieldset
        listStrFields = new List<String>();
        for (Schema.FieldSetMember f : this.getFields()) {
            listStrFields.add(f.getFieldPath());
        }

        // Required fields
        if (!ListPartialContains(listStrFields, 'Id')) {
            listStrFields.add('Id');
        }
        if (!ListPartialContains(listStrFields, 'Name')) {
            listStrFields.add('Name');
        }
        if (!ListPartialContains(listStrFields, 'C501_Merge_Metric_Name__c')) {
            listStrFields.add('C501_Merge_Metric_Name__c');
        }
        if (!ListPartialContains(listStrFields, 'C501_Merge_Metric__c')) {
            listStrFields.add('C501_Merge_Metric__c');
        }
        if (!ListPartialContains(listStrFields, 'C501_Merge_Id__c')) {
            listStrFields.add('C501_Merge_Id__c');
        }
        if (!ListPartialContains(listStrFields, 'C501_Merge_Action__c')) {
            listStrFields.add('C501_Merge_Action__c');
        }
        if (!ListPartialContains(listStrFields, 'C501_Merge_Account__c')) {
            listStrFields.add('C501_Merge_Account__c');
        }
        if (!ListPartialContains(listStrFields, 'C501_Merge_Target__c')) {
            listStrFields.add('C501_Merge_Target__c');
        }
    }   
    
    //
    //  OnPageLoad added to do the inialization so that any DML calls while initializing are commited to the database.
    //      For Instance if you just reference listAccounts getter in a datatable then any DML calls are cached in memory and not committed until there is an action invoked (e.g., ChangeAccount, SaveAccount)
    //          Non-Action (DML Calls will be cached & not committed unless you trigger an action):     <apex:dataTable value="{!listAccounts}" ...
    //          Action (DML Calls will be committed):                                                   <apex:commandButton styleClass="slds-button slds-button_brand" action="{!SaveAccounts}" ...
    //
    public void OnPageLoad() {

        // Initialize data
        this.getFields();
        List<Account> displayObjects = this.listAccounts;
        List<SelectOption> objectsFilter = this.listSOAccounts;
        List<SelectOption> rulesFilter = this.listSORules;
    }

    private Boolean ListPartialContains(List<String> listStrFields, String fieldValue) {

        if (listStrFields == null) {
            return false;
        }

        for (String listValue :listStrFields) {
            if (listValue.contains(fieldValue)) {
                return true;
            }
        }

        return false;
    }
    
    public List<SelectOption> listSOAccounts {
        get {
            if (listSOAccounts == null )
                listSOAccounts = c501MassMerge_SharedCode_Account.ListObjects(objectFilter.C501_Merge_Rule__c);

            return listSOAccounts;
        }

        set;
        }

    public List<SelectOption> listSORules {
        get {
            if (listSORules == null ) {
                listSORules = new List<System.SelectOption>();
                listSORules.add(new SelectOption('', ''));

          		AggregateResult[] allAggregatedByRule = [
                    SELECT Merge_Metric__r.Name
                    FROM C501_Account_Merge__c
                    WHERE Merge_Action__c <> 'Ignore'
                    GROUP BY Merge_Metric__r.Name
                    ORDER BY Merge_Metric__r.Name ASC];

                for (AggregateResult result : allAggregatedByRule)  {
                    String ruleName = result.get('Name').toString();
                    listSORules.add(new SelectOption(ruleName, ruleName));
                }
            }

            return listSORules;
        }

        set;
        }

    public Schema.FieldSet fieldSetActive {
        get {

            Schema.FieldSet fieldSetCheck = SObjectType.Account.FieldSets.MassMergeAccounts;
            for (String fieldSetName :Schema.SObjectType.Account.fieldSets.getMap().keySet()) {

                // Check for custom fields set
                if (fieldSetName.containsIgnoreCase('massmergeaccounts_custom')) {
                    fieldSetCheck = Schema.SObjectType.Account.fieldSets.getMap().get(fieldSetName);
                    break;
                }
            }

            return fieldSetCheck;
        }

        set;
    }

    // getter - reference in Visualforce as fields
    public List<Schema.FieldSetMember> getFields() {
        return fieldSetActive.getFields();
    }

    public Account objectFilter {
        get {
            if (objectFilter == null) {
                objectFilter = new Account();
                
                objectFilter.C501_Merge_Rule__c = null;
            }
            return objectFilter;
        }
        set;
    }

    // the user has changed the Account dropdown
    public virtual PageReference ChangeAccount() {

        listAccounts = null;
        strSaveResults = null;

        return null;
    }

    // the user has changed the Rule dropdown
    public virtual PageReference ChangeRule() {

        listSOAccounts = null;
        listAccounts = null;
        strSaveResults = null;

        return null;
    }

    public virtual void ResetAccounts() {

        C501_MassMerge_SharedCode.OutputDebugLogText(LoggingLevel.DEBUG, '***** C501_CTRL_MassMergeAccounts::ResetAccounts');

        listSOAccounts = null;
        listAccounts = null;
    }

    // status string for reporting the results of saving.
    public String strSaveResults { get; set; }
      
    // The list of Accounts
    public List<Account> listAccounts {
        get {
            if (listAccounts == null) {
                FillListAccounts();
            }

            return listAccounts;
        }
        
        set;
    }
        
    // helper function that the client can override, to query for the appropriate fields in Accounts.
    public virtual void FillListAccounts() {

        String strSoql = C501_MassMerge_SharedCode.GetC501AccountMergeQuery();
        if (String.isBlank(strSoql)) {
            return;
        }
         
        String strRuleFilterSoql = '';
        if (!String.isBlank(objectFilter.C501_Merge_Rule__c)) {
            strRuleFilterSoql = ' AND Merge_Metric__r.Name = \'' + objectFilter.C501_Merge_Rule__c + '\'';
        }

        String strNameFilterSoql = '';
        if (!String.isBlank(objectFilter.C501_Merge_Name_Filter__c)) {      
            strNameFilterSoql += ' AND (Account__r.Name LIKE \'%' + objectFilter.C501_Merge_Name_Filter__c + '%\' OR Merge_Target_Account__r.Name LIKE \'%' + objectFilter.C501_Merge_Name_Filter__c + '%\')';
        }

        strSoql += strRuleFilterSoql + strNameFilterSoql + ' AND Merge_Action__c = null ORDER BY Account__r.Name ASC';
        C501_MassMerge_SharedCode.OutputDebugLogText(LoggingLevel.DEBUG, '***** ' + String.valueOf(this).substring(0,String.valueOf(this).indexOf(':')) + '::FillListAccounts ObjectFilter Query strSoql: ' + strSoql);

        Integer mergeIndex = 1;
        String mergeBuffer = '0';
		List<C501_Account_Merge__c> AccountsMerge = new List<C501_Account_Merge__c>();
        Set<Id> AccountsMergeIds = new Set<Id>();

        for (C501_Account_Merge__c AccountMerge :Database.query(strSoql)) {

            AccountsMerge.add(
                new C501_Account_Merge__c(
                    Id = AccountMerge.Id,
                    Merge_Action__c = AccountMerge.Merge_Action__c,
                    Account__c = AccountMerge.Account__c,
                    Merge_Target_Account__c = AccountMerge.Merge_Target_Account__c,
                    Merge_Metric__c = AccountMerge.Merge_Metric__c,
                    Merge_Metric_Name__c = 'Merge ' + mergeBuffer + mergeIndex + ' ' + AccountMerge.Merge_Metric__r.Name
            ));

            mergeIndex++;
            if (mergeIndex >= 10) {
                mergeBuffer = '';
            }

            AccountsMergeIds.add(AccountMerge.Account__c);
            AccountsMergeIds.add(AccountMerge.Merge_Target_Account__c);

            Integer maxRecordsDisplay = C501_MassMerge_SharedCode.SALESFORCE_MAXIMUM_COLLECTION_SIZE;
            Mass_Merge_Config__c massMergeConfigSettings = Mass_Merge_Config__c.getOrgDefaults();
            if (massMergeConfigSettings <> null && massMergeConfigSettings.Maximum_Records_Display__c > 0.0 && massMergeConfigSettings.Maximum_Records_Display__c <> maxRecordsDisplay) {
                maxRecordsDisplay = Integer.valueOf(massMergeConfigSettings.Maximum_Records_Display__c);
            }   

            // Max records display size check - get a View State Size Limit if records size exceeds limit
            if (mergeIndex >= maxRecordsDisplay) {
                ApexPages.addMessage(new ApexPages.message(ApexPages.Severity.WARNING, 'Maximum records exceeded so loading a subset of the total merge candidates.  Try adding Filter by Account or Filter by Rule criteria if you are having trouble finding a specific merge candidate.'));
                break;
            }
        }

        strSoql = 'SELECT ' + String.join(listStrFields, ',');
        strSoql += ' FROM Account WHERE Id in ' + C501_MassMerge_SharedCode.GetSafeSoqlList(AccountsMergeIds);
        strSoql += ' ORDER BY C501_Merge_Metric_Name__c ASC NULLS FIRST';
        //strSoql += ' LIMIT ' + String.valueOf(C501_MassMerge_SharedCode.GetMaximumCollectionSize());
        
        C501_MassMerge_SharedCode.OutputDebugLogText(LoggingLevel.DEBUG, '***** ' + String.valueOf(this).substring(0,String.valueOf(this).indexOf(':')) + '::FillListAccounts Account Query strSoql: ' + strSoql);

        Map<Id, Account> AccountMap = new Map<Id, Account>();
        for (Account objectRecord :Database.Query(strSoql) ) {
            AccountMap.put(objectRecord.Id, objectRecord);
        }

        listAccounts = new List<Account>();
        for (C501_Account_Merge__c mergeRecord :AccountsMerge) {

            C501_MassMerge_SharedCode.OutputDebugLogText(LoggingLevel.DEBUG, '***** ' + String.valueOf(this).substring(0,String.valueOf(this).indexOf(':')) + '::FillListAccounts mergeRecord Name: ' + mergeRecord.Merge_Metric_Name__c);

            try {

                Account listRecord1 = AccountMap.get(mergeRecord.Account__c).clone(false, false, false, false);
                listRecord1.Id = mergeRecord.Account__c;
                listRecord1.C501_Merge_Id__c = mergeRecord.Id;
                listRecord1.C501_Merge_Action__c = mergeRecord.Merge_Action__c;
                listRecord1.C501_Merge_Account__c = mergeRecord.Account__c;
                listRecord1.C501_Merge_Target__c = mergeRecord.Merge_Target_Account__c;
                listRecord1.C501_Merge_Metric_Name__c = mergeRecord.Merge_Metric_Name__c;
                listRecord1.C501_Merge_Metric__c = mergeRecord.Merge_Metric__c;

                C501_MassMerge_SharedCode.OutputDebugLogText(LoggingLevel.DEBUG, '***** ' + String.valueOf(this).substring(0,String.valueOf(this).indexOf(':')) + '::FillListAccounts listRecord1 Name: ' + listRecord1.C501_Merge_Metric_Name__c);

                // Switch Master and Duplicate
                Account listRecord2 = AccountMap.get(mergeRecord.Merge_Target_Account__c).clone(false, false, false, false);
                listRecord2.Id = mergeRecord.Merge_Target_Account__c;
                listRecord2.C501_Merge_Id__c = mergeRecord.Id;
                listRecord2.C501_Merge_Action__c = mergeRecord.Merge_Action__c;
                listRecord2.C501_Merge_Account__c = mergeRecord.Merge_Target_Account__c;
                listRecord2.C501_Merge_Target__c = mergeRecord.Account__c;
                listRecord2.C501_Merge_Metric_Name__c = mergeRecord.Merge_Metric_Name__c;
                listRecord2.C501_Merge_Metric__c = mergeRecord.Merge_Metric__c;

                C501_MassMerge_SharedCode.OutputDebugLogText(LoggingLevel.DEBUG, '***** ' + String.valueOf(this).substring(0,String.valueOf(this).indexOf(':')) + '::FillListAccounts listRecord2 Name: ' + listRecord2.C501_Merge_Metric_Name__c);

                listAccounts.add(listRecord1);
                listAccounts.add(listRecord2);
            }
            catch (Exception ex) {
                C501_MassMerge_SharedCode.OutputDebugLogText(LoggingLevel.DEBUG, '***** C501_CTRL_MassMergeAccounts::FillListAccounts listAccounts skipping merge record since exception when accessing - ' + ex.getMessage());
            }
        }

        C501_MassMerge_SharedCode.OutputDebugLogFlush();
    }
    
    public Account getTargetMerge(Account masterObject)
    {
        for (Account objectItem :listAccounts) {
            if (String.isBlank(objectItem.C501_Merge_Id__c) || String.isBlank(objectItem.Id) || String.isBlank(masterObject.Id) || String.isBlank(masterObject.C501_Merge_Id__c)
                || objectItem.Id == masterObject.Id || objectItem.C501_Merge_Id__c <> masterObject.C501_Merge_Id__c) {
                continue;
            }

            return objectItem;
        }

        return null;
    }

    //save the Accounts created or modified
    public virtual PageReference SaveAccounts() {
        
        if (listAccounts == null) return null;
        
        strSaveResults = null;
        
        try {
            Map<String, C501_Account_Merge__c> listAccountsMergesIgnore = new Map<String, C501_Account_Merge__c>();
            Map<String, C501_Account_Merge__c> listAccountsMergesMaster = new Map<String, C501_Account_Merge__c>();

            C501_MassMerge_SharedCode.OutputDebugLogText(LoggingLevel.DEBUG, '***** C501_CTRL_MassMergeAccounts::SaveAccounts listAccounts: ' + String.valueOf(listAccounts));

            for (Account account : listAccounts) {

                if (account.Id != null && account.C501_Merge_Action__c == 'Master') {

                    C501_MassMerge_SharedCode.OutputDebugLogText(LoggingLevel.DEBUG, '***** C501_CTRL_MassMergeAccounts::SaveAccounts Master Check Merge Action - Merge Rule: ' + account.C501_Merge_Metric_Name__c);

                    if (!listAccountsMergesMaster.containsKey(account.C501_Merge_Id__c)) {

                        Account targetObject = getTargetMerge(account);
                        if (targetObject == null) {
                            continue;
                        }

                        listAccountsMergesMaster.put(account.C501_Merge_Id__c, new C501_Account_Merge__c(
                            Id = account.C501_Merge_Id__c,
                            Account__c = account.Id,
                            Merge_Target_Account__c = targetObject.Id,
                            Merge_Action__c = account.C501_Merge_Action__c,
                            Merge_Metric__c = account.C501_Merge_Metric__c
                        ));
                    }
                }

                else if (account.Id != null && account.C501_Merge_Action__c == 'Ignore') {

                    C501_MassMerge_SharedCode.OutputDebugLogText(LoggingLevel.DEBUG, '***** C501_CTRL_MassMergeAccounts::SaveAccounts Ignore Check Merge Action - Merge Rule: ' + account.C501_Merge_Metric_Name__c);

                    if (!listAccountsMergesIgnore.containsKey(account.C501_Merge_Id__c)) {

                        Account targetObject = getTargetMerge(account);
                        if (targetObject == null) {
                            continue;
                        }

                        listAccountsMergesIgnore.put(account.C501_Merge_Id__c, new C501_Account_Merge__c(
                            Id = account.C501_Merge_Id__c,
                            Account__c = account.Id,
                            Merge_Target_Account__c = targetObject.Id,
                            Merge_Action__c = account.C501_Merge_Action__c
                        ));
                    }
                }
            }

            Integer mergesIgnoreSize = 0;
            if (!listAccountsMergesIgnore.isEmpty() && !listAccountsMergesIgnore.values().isEmpty()) {

                mergesIgnoreSize = listAccountsMergesIgnore.size();
                Database.SaveResult[] updateResults = Database.update(listAccountsMergesIgnore.values(), false);
                for (Database.SaveResult updateResult :updateResults) {
                    if (!updateResult.isSuccess()) {
                        for(Database.Error resultError : updateResult.getErrors()) {
                            C501_MassMerge_SharedCode.LogDatabaseError(resultError, String.valueOf(this).substring(0,String.valueOf(this).indexOf(':')) + '::SaveAccounts');
                        }                        
                    }
                }
            }

            Integer mergesMasterSize = 0;
            if (!listAccountsMergesMaster.isEmpty() && !listAccountsMergesMaster.values().isEmpty()) {

                mergesMasterSize = listAccountsMergesMaster.size();
                Database.SaveResult[] updateResults = Database.update(listAccountsMergesMaster.values(), false);
                for (Database.SaveResult updateResult :updateResults) {
                    if (!updateResult.isSuccess()) {
                        for(Database.Error resultError : updateResult.getErrors()) {
                            C501_MassMerge_SharedCode.LogDatabaseError(resultError, String.valueOf(this).substring(0,String.valueOf(this).indexOf(':')) + '::SaveAccounts');
                        }                        
                    }
                }
                c501MassMerge_SharedCode_Account.ProcessObjectMerges(new List<C501_Merge_Metric__c>{}, listAccountsMergesMaster.values());
            }

            strSaveResults = ( (mergesIgnoreSize + mergesMasterSize) * 2) + ' ' + Label.Merge_labelMassMergeSaveSuccess;
            ApexPages.addMessage(new ApexPages.message(ApexPages.Severity.CONFIRM, strSaveResults));

            // Refresh Account and Account Table
            ResetAccounts();

            // Trigger Merge Discover
            C501_MM_Rule_Discover_Accounts.executeFuture();
            C501_MM_Rule_Discover_ContactsAccount.executeFuture();
            C501_MM_Rule_Discover_ContactsAccounts.executeFuture();
            C501_MM_Process_Merge_Queries.executeFuture();

            C501_MassMerge_SharedCode.OutputDebugLogFlush();

            return null;            

        } catch (Exception e) {
            strSaveResults = Label.Merge_labelMassMergeErrorOnSave;
            ApexPages.addMessages(e); 
            return null;
        }
    }

    // this was a which can't be compiled in this class under api 31.  
    // but because it was marked public, it could not be completely removed.
    public static void CodeCoverageTests() {}
}