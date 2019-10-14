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

        System.debug(LoggingLevel.DEBUG, '***** C501_CTRL_MassMergeAccounts::C501_CTRL_MassMergeAccounts Constructor');

        redirect = false;
        c501MassMerge_SharedCode_Account = new C501_MassMerge_SharedCode_Account();
        
        // handle optional parameters (must use string, not ID, to handle null)
        string id = ApexPages.currentPage().getParameters().get('id');
        if (id != null && id != '') objectFilter.Id = id;
        
        // Get list of fields FROM fieldset
        listStrFields = new List<String>();
        for (Schema.FieldSetMember f : this.getFields()) {
            listStrFields.add(f.getFieldPath());
        }

        // Required fields
        if (!ListPartialcontains(listStrFields, 'Id')) {
            listStrFields.add('Id');
        }
        if (!ListPartialcontains(listStrFields, 'Name')) {
            listStrFields.add('Name');
        }
        if (!ListPartialcontains(listStrFields, 'C501_Merge_Metric_Name__c')) {
            listStrFields.add('C501_Merge_Metric_Name__c');
        }
        if (!ListPartialcontains(listStrFields, 'C501_Merge_Metric__c')) {
            listStrFields.add('C501_Merge_Metric__c');
        }
        if (!ListPartialcontains(listStrFields, 'C501_Merge_Id__c')) {
            listStrFields.add('C501_Merge_Id__c');
        }
        if (!ListPartialcontains(listStrFields, 'C501_Merge_Action__c')) {
            listStrFields.add('C501_Merge_Action__c');
        }
        if (!ListPartialcontains(listStrFields, 'C501_Merge_Account__c')) {
            listStrFields.add('C501_Merge_Account__c');
        }
        if (!ListPartialcontains(listStrFields, 'C501_Merge_Target__c')) {
            listStrFields.add('C501_Merge_Target__c');
        }
        if (!ListPartialcontains(listStrFields, 'C501_Merge_Confidence__c')) {
            listStrFields.add('C501_Merge_Confidence__c');
        }
    }   
    
    private Boolean ListPartialcontains(List<String> listStrFields, String fieldValue) {

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
                listSOAccounts = c501MassMerge_SharedCode_Account.ListObjectsWithConfidence(objectFilter.C501_Merge_Rule__c, objectFilter.C501_Merge_Confidence_From__c, objectFilter.C501_Merge_Confidence_To__c);

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
                if (fieldSetName.contains('massmergeaccounts_custom')) {
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
                objectFilter.C501_Merge_Confidence_From__c = 90.0;
                objectFilter.C501_Merge_Confidence_To__c = 100.0;
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
        objectFilter.Id = null;
        listAccounts = null;
        strSaveResults = null;

        return null;
    }

    // the user has changed the Confidence query
    public virtual PageReference ChangeMergeConfidence() {

        listSOAccounts = null;
        objectFilter.Id = null;
        listAccounts = null;
        strSaveResults = null;

        return null;
    }

    public virtual void ResetAccounts() {

        System.debug(LoggingLevel.DEBUG, '***** C501_CTRL_MassMergeAccounts::ResetAccounts');

        listSOAccounts = null;
        objectFilter.Id = null;
        listAccounts = null;
    }

    // status string for reporting the results of saving.
    public String strSaveResults { get; set; }
      
    // The list of Accounts.
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

		Map<Id,C501_Account_Merge__c> accountsMerge = new Map<Id,C501_Account_Merge__c>();

        if (objectFilter.Id <> null) {
        
            System.debug(LoggingLevel.DEBUG, '***** C501_CTRL_MassMergeAccounts::FillListAccounts objectFilter.Id: ' + objectFilter.Id);

            String strSoql = C501_MassMerge_SharedCode.GetC501AccountMergeQuery();
            strSoql += ' AND (Account__c = \'' + objectFilter.Id + '\' OR Merge_Target_Account__c = \'' + objectFilter.Id + '\') AND Merge_Action__c = null';
            strSoql += ' LIMIT 1';

            System.debug(LoggingLevel.DEBUG, '***** ' + String.valueOf(this).substring(0,String.valueOf(this).indexOf(':')) + '::FillListAccounts strSoql: ' + strSoql);

            for(C501_Account_Merge__c accountMerge : Database.query(strSoql)) {

                    System.debug(LoggingLevel.DEBUG, '***** C501_CTRL_MassMergeAccounts::FillListAccounts accountMerge: ' + accountMerge);

                    accountsMerge.put(
                        accountMerge.Account__c,
                        new C501_Account_Merge__c(
                            Id = accountMerge.Id,
                            Merge_Action__c = accountMerge.Merge_Action__c,
                            Merge_Confidence__c = null,
                            Account__c = accountMerge.Account__c,
                            Merge_Target_Account__c = accountMerge.Merge_Target_Account__c,
                            Merge_Metric__c = accountMerge.Merge_Metric__c,
                            Merge_Metric_Name__c = 'Merge ' + accountMerge.Merge_Metric__r.Name
                    ));

                    accountsMerge.put(
                        accountMerge.Merge_Target_Account__c,
                        new C501_Account_Merge__c(
                            Id = accountMerge.Id,
                            Merge_Action__c = accountMerge.Merge_Action__c,
                            Merge_Confidence__c = accountMerge.Merge_Confidence__c,
                            Account__c = accountMerge.Account__c,
                            Merge_Target_Account__c = accountMerge.Merge_Target_Account__c,
                            Merge_Metric__c = accountMerge.Merge_Metric__c,
                            Merge_Metric_Name__c = 'Merge ' + accountMerge.Merge_Metric__r.Name
                    ));
            }
        }
        else {

            Set<String> objectIds = new Set<String>();
            for (SelectOption option : listSOAccounts) {
                objectIds.add(option.getValue());

                // Performance: Only allow 50 at a time to load in the webpage
                if (objectIds.size() >= 50) {
                    break;
                }
            }

            Integer mergeIndex = 1;
            String mergeBuffer = '0';

            String mergeRule = objectFilter.C501_Merge_Rule__c;
            String strRuleSoql = '';
            if (mergeRule <> null && !String.isBlank(mergeRule)) {
                strRuleSoql = ' and Merge_Metric__r.Name = \'' + mergeRule + '\'';
            }

            String strSoql = C501_MassMerge_SharedCode.GetC501AccountMergeQuery();
            strSoql += ' AND (Account__c in ' + C501_MassMerge_SharedCode.GetSafeSoqlList(objectIds) + ' OR Merge_Target_Account__c in ' + C501_MassMerge_SharedCode.GetSafeSoqlList(objectIds) + ') AND Merge_Action__c = null';
            strSoql += strRuleSoql;
            strSoql += ' ORDER BY Account__r.Name ASC';

            System.debug(LoggingLevel.DEBUG, '***** ' + String.valueOf(this).substring(0,String.valueOf(this).indexOf(':')) + '::FillListAccounts strSoql: ' + strSoql);

            for(C501_Account_Merge__c accountMerge : Database.Query(strSoql)) {

                    if (accountsMerge.containsKey(accountMerge.Account__c) || accountsMerge.containsKey(accountMerge.Merge_Target_Account__c)) {
                        continue;
                    }

                    accountsMerge.put(
                        accountMerge.Account__c,
                        new C501_Account_Merge__c(
                            Id = accountMerge.Id,
                            Merge_Action__c = accountMerge.Merge_Action__c,
                            Merge_Confidence__c = null,
                            Account__c = accountMerge.Account__c,
                            Merge_Target_Account__c = accountMerge.Merge_Target_Account__c,
                            Merge_Metric__c = accountMerge.Merge_Metric__c,
                            Merge_Metric_Name__c = 'Merge ' + mergeBuffer + mergeIndex + ' ' + accountMerge.Merge_Metric__r.Name
                    ));

                    accountsMerge.put(
                        accountMerge.Merge_Target_Account__c,
                        new C501_Account_Merge__c(
                            Id = accountMerge.Id,
                            Merge_Action__c = accountMerge.Merge_Action__c,
                            Merge_Confidence__c = accountMerge.Merge_Confidence__c,
                            Account__c = accountMerge.Account__c,
                            Merge_Target_Account__c = accountMerge.Merge_Target_Account__c,
                            Merge_Metric__c = accountMerge.Merge_Metric__c,
                            Merge_Metric_Name__c = 'Merge ' + mergeBuffer + mergeIndex + ' ' + accountMerge.Merge_Metric__r.Name
                    ));

                    mergeIndex++;
                    if (mergeIndex >= 10) {
                        mergeBuffer = '';
                    }
            }
        }

        String strSoql = 'SELECT ' + String.join(listStrFields, ',');

        Set<Id> accountsMergeIds = accountsMerge.keySet();

        strSoql += ' FROM Account WHERE Id in ' + C501_MassMerge_SharedCode.GetSafeSoqlList(accountsMergeIds);
        strSoql += ' order by C501_Merge_Metric_Name__c ASC NULLS FIRST';
        
        System.debug(LoggingLevel.DEBUG, '***** ' + String.valueOf(this).substring(0,String.valueOf(this).indexOf(':')) + '::FillListAccounts strSoql: ' + strSoql);

        listAccounts = Database.Query(strSoql);

        for ( Account account :listAccounts ) {
            if (accountsMerge.containsKey(account.Id)) {
                C501_Account_Merge__c mergeAccount = accountsMerge.get(account.Id);
                account.C501_Merge_Id__c = mergeAccount.Id;
                account.C501_Merge_Action__c = mergeAccount.Merge_Action__c;
                account.C501_Merge_Confidence__c = mergeAccount.Merge_Confidence__c;
                account.C501_Merge_Account__c = mergeAccount.Account__c;
                account.C501_Merge_Target__c = mergeAccount.Merge_Target_Account__c;
                account.C501_Merge_Metric_Name__c = mergeAccount.Merge_Metric_Name__c;
                account.C501_Merge_Metric__c = mergeAccount.Merge_Metric__c;
            }
        }

        // Update Accounts w/ Merge fields then query to get the correct sort order by Metric Name
        Database.SaveResult[] updateResults = Database.update(listAccounts, false);
        for (Database.SaveResult updateResult :updateResults) {
            if (!updateResult.isSuccess()) {
                for(Database.Error resultError : updateResult.getErrors()) {
                    C501_MassMerge_SharedCode.LogDatabaseError(resultError, String.valueOf(this).substring(0,String.valueOf(this).indexOf(':')) + '::FillListAccounts');
                }                        
            }
        }

        System.debug(LoggingLevel.DEBUG, '***** ' + String.valueOf(this).substring(0,String.valueOf(this).indexOf(':')) + '::FillListAccounts strSoql: ' + strSoql);
        listAccounts = Database.Query(strSoql);
    }
    
    // used to track the Id's of all Account records that get modified.
    public Set<ID> setAccountsIdDirty {
        get {
            if (setAccountsIdDirty == null) {
                setAccountsIdDirty = new Set<ID>();
            }
            return setAccountsIdDirty;
        }
        set;
    }
    
    // to allow vf page to see how many Account records are dirty
    public Integer numberOfRecordsDirty {
        get { 
            if (setAccountsIdDirty == null)
                return 0;
            else
                return setAccountsIdDirty.size();
        }
    }
    
    // this gets called whenever a field in the Accounts datatable is modified.
    // use this to remember which existing Accounts records have been modified.
    public virtual PageReference AccountsRecordModified() {
        String strId = ApexPages.currentPage().getParameters().get('usersIdDirtied');
        if (strId != null && strId != '') setAccountsIdDirty.add(strId);
        return null;
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

            System.debug(LoggingLevel.DEBUG, '***** C501_CTRL_MassMergeAccounts::SaveAccounts listAccounts: ' + String.valueOf(listAccounts));

            for (Account account : listAccounts) {

                if (account.Id != null && account.C501_Merge_Action__c == 'Master') {

                    System.debug(LoggingLevel.DEBUG, '***** C501_CTRL_MassMergeAccounts::SaveAccounts Master Check Merge Action - Merge Rule: ' + account.C501_Merge_Metric_Name__c);

                    // we couldn't reliably mark dirty records in all scenarios,
                    // so we've decided it is better to always update all records.
                    //if (setAccountsIdDirty.contains(account.Id)) 

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
                            Merge_Metric__c = account.C501_Merge_Metric__c,
                            // Set merge Confidence to 100% to ensure merge regardless of automerge percentage
                            Merge_Confidence__c = 100.0
                        ));
                    }
                }

                else if (account.Id != null && account.C501_Merge_Action__c == 'Ignore') {

                    System.debug(LoggingLevel.DEBUG, '***** C501_CTRL_MassMergeAccounts::SaveAccounts Ignore Check Merge Action - Merge Rule: ' + account.C501_Merge_Metric_Name__c);

                    // we couldn't reliably mark dirty records in all scenarios,
                    // so we've decided it is better to always update all records.
                    //if (setAccountsIdDirty.contains(account.Id)) 

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
                c501MassMerge_SharedCode_Account.ProcessObjectMerges(new List<C501_Merge_Metric__c>{}, listAccountsMergesMaster.values(), false);
            }

            strSaveResults = ( (mergesIgnoreSize + mergesMasterSize) * 2) + ' ' + Label.Merge_labelMassMergeSaveSuccess;
            ApexPages.addMessage(new ApexPages.message(ApexPages.Severity.CONFIRM, strSaveResults));

            // successful save, so clear out dirty list
            setAccountsIdDirty = null;

            // Refresh Account and Account Table
            ResetAccounts();

            return null;            

        } catch (Exception e) {
            strSaveResults = Label.Merge_labelMassMergeErrorOnSave;
            ApexPages.addMessages(e); 
            return null;
        }
    }

    //save the Accounts modified, and then close the page.
    public virtual PageReference SaveAndCloseAccounts() {
        SaveAccounts();
        if (ApexPages.hasMessages(ApexPages.Severity.ERROR) ||
           ApexPages.hasMessages(ApexPages.Severity.FATAL) ||
            ApexPages.hasMessages(ApexPages.Severity.WARNING)) {
                return null; // don't close window.
        }
        return Cancel();    
    }
    
    // user wants to close this page
    public virtual PageReference Cancel() {
        // figure out which object to return to.
        idRedirect = objectFilter.Id;
        
        // trigger our redirect component to redirect using javascript.
        redirect = true;
        return null;
    }
    
    // this was a which can't be compiled in this class under api 31.  
    // but because it was marked public, it could not be completely removed.
    public static void CodeCoverageTests() {}
}