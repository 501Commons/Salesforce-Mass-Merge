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

public virtual without sharing class C501_CTRL_MassMergeContacts {

    private C501_MassMerge_SharedCode_Contact c501MassMerge_SharedCode_Contact;
    private List<String> listStrFields;
    
    public boolean redirect { get; private set; }
    public ID idRedirect { get; private set; }
    
    // constructor
    public C501_CTRL_MassMergeContacts() {

        C501_MassMerge_SharedCode.OutputDebugLogText(LoggingLevel.DEBUG, '***** C501_CTRL_MassMergeContacts::C501_CTRL_MassMergeContacts Constructor');

        redirect = false;
        c501MassMerge_SharedCode_Contact = new C501_MassMerge_SharedCode_Contact();
        
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
        if (!ListPartialContains(listStrFields, 'C501_Merge_Contact__c')) {
            listStrFields.add('C501_Merge_Contact__c');
        }
        if (!ListPartialContains(listStrFields, 'C501_Merge_Target__c')) {
            listStrFields.add('C501_Merge_Target__c');
        }
        if (!ListPartialContains(listStrFields, 'C501_Merge_Confidence__c')) {
            listStrFields.add('C501_Merge_Confidence__c');
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
        List<Contact> displayObjects = this.listContacts;
        List<SelectOption> objectsFilter = this.listSOContacts;
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

    public List<SelectOption> listSOContacts {
        get {
            if (listSOContacts == null )
                listSOContacts = c501MassMerge_SharedCode_Contact.listObjectsWithConfidence(objectFilter.C501_Merge_Rule__c, objectFilter.C501_Merge_Confidence_From__c, objectFilter.C501_Merge_Confidence_To__c);

            return listSOContacts;
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
                    FROM C501_Contact_Merge__c
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

            Schema.FieldSet fieldSetCheck = SObjectType.Contact.FieldSets.MassMergeContacts;
            for (String fieldSetName :Schema.SObjectType.Contact.fieldSets.getMap().keySet()) {

                // Check for custom fields set
                if (fieldSetName.containsIgnoreCase('massmergecontacts_custom')) {
                    fieldSetCheck = Schema.SObjectType.Contact.fieldSets.getMap().get(fieldSetName);
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

    public Contact objectFilter {
        get {
            if (objectFilter == null) {
                objectFilter = new Contact();
                
                objectFilter.C501_Merge_Rule__c = null;
                objectFilter.C501_Merge_Confidence_From__c = 0.0;
                objectFilter.C501_Merge_Confidence_To__c = 100.0;
            }
            return objectFilter;
        }
        set;
    }

    // the user has changed the Contact dropdown
    public virtual PageReference ChangeContact() {

        listContacts = null;
        strSaveResults = null;

        return null;
    }

    // the user has changed the Rule dropdown
    public virtual PageReference ChangeRule() {

        listSOContacts = null;
        listContacts = null;
        strSaveResults = null;

        return null;
    }

    public virtual void ResetContacts() {

        C501_MassMerge_SharedCode.OutputDebugLogText(LoggingLevel.DEBUG, '***** C501_CTRL_MassMergeContacts::ResetContacts');

        listSOContacts = null;
        listContacts = null;
    }

    // status string for reporting the results of saving.
    public String strSaveResults { get; set; }
      
    // The list of contacts.
    public List<Contact> listContacts {
        get {
            if (listContacts == null) {
                FillListContacts();
            }

            return listContacts;
        }
        
        set;
    }
        
    // helper function that the client can override, to query for the appropriate fields in Contacts.
    public virtual void FillListContacts() {

        String strSoql = C501_MassMerge_SharedCode.GetC501ContactMergeQuery();
        if (String.isBlank(strSoql)) {
            return;
        }
          
        String strRuleFilterSoql = '';
        if (!String.isBlank(objectFilter.C501_Merge_Rule__c)) {
            strRuleFilterSoql = ' AND Merge_Metric__r.Name = \'' + objectFilter.C501_Merge_Rule__c + '\'';
        }

        String strNameFilterSoql = '';
        if (!String.isBlank(objectFilter.C501_Merge_Name_Filter__c)) {      
            
            // Filter out MiddleName which doesn't work with Name LIKE SOQL
            List<String> nameTokens = objectFilter.C501_Merge_Name_Filter__c.split(' ');
            String shortName = nameTokens[0];
            shortName += (nameTokens.size() > 1) ? '%' + nameTokens[nameTokens.size() - 1] : '';
                        
            strNameFilterSoql += ' AND (Contact__r.Name LIKE \'%' + shortName + '%\' OR Merge_Target_Contact__r.Name LIKE \'%' + shortName + '%\')';
        }

        strSoql += strRuleFilterSoql + strNameFilterSoql + ' AND Merge_Action__c = null ORDER BY Contact__r.Name ASC';
        C501_MassMerge_SharedCode.OutputDebugLogText(LoggingLevel.DEBUG, '***** ' + String.valueOf(this).substring(0,String.valueOf(this).indexOf(':')) + '::FillListContacts ObjectFilter Query strSoql: ' + strSoql);

        Integer mergeIndex = 1;
        String mergeBuffer = '0';
		List<C501_Contact_Merge__c> contactsMerge = new List<C501_Contact_Merge__c>();
        Set<Id> contactsMergeIds = new Set<Id>();

        for (C501_Contact_Merge__c contactMerge :Database.query(strSoql)) {

            contactsMerge.add(
                new C501_Contact_Merge__c(
                    Id = contactMerge.Id,
                    Merge_Action__c = contactMerge.Merge_Action__c,
                    Merge_Confidence__c = null,
                    Contact__c = contactMerge.Contact__c,
                    Merge_Target_Contact__c = contactMerge.Merge_Target_Contact__c,
                    Merge_Metric__c = contactMerge.Merge_Metric__c,
                    Merge_Metric_Name__c = 'Merge ' + mergeBuffer + mergeIndex + ' ' + contactMerge.Merge_Metric__r.Name
            ));

            mergeIndex++;
            if (mergeIndex >= 10) {
                mergeBuffer = '';
            }

            contactsMergeIds.add(contactMerge.Contact__c);
            contactsMergeIds.add(contactMerge.Merge_Target_Contact__c);

            // Max collection size check for at least half because merge candidates generate double the Contact records (checking for more because hitting View State Size Limit)
            if (mergeIndex >= (C501_MassMerge_SharedCode.SALESFORCE_MAXIMUM_COLLECTION_SIZE / 3)) {
                ApexPages.addMessage(new ApexPages.message(ApexPages.Severity.WARNING, 'Maximum records exceeded so loading a subset of the total merge candidates.  Try adding Filter by Contact or Filter by Rule criteria if you are having trouble finding a specific merge candidate.'));
                break;
            }
        }

        strSoql = 'SELECT ' + String.join(listStrFields, ',');
        strSoql += ' FROM Contact WHERE Id in ' + C501_MassMerge_SharedCode.GetSafeSoqlList(contactsMergeIds);
        strSoql += ' ORDER BY C501_Merge_Metric_Name__c ASC NULLS FIRST';
        //strSoql += ' LIMIT ' + String.valueOf(C501_MassMerge_SharedCode.GetMaximumCollectionSize());
        
        C501_MassMerge_SharedCode.OutputDebugLogText(LoggingLevel.DEBUG, '***** ' + String.valueOf(this).substring(0,String.valueOf(this).indexOf(':')) + '::FillListContacts Contact Query strSoql: ' + strSoql);

        Map<Id, Contact> contactMap = new Map<Id, Contact>();
        for (Contact objectRecord :Database.Query(strSoql) ) {
            contactMap.put(objectRecord.Id, objectRecord);
        }

        listContacts = new List<Contact>();
        for (C501_Contact_Merge__c mergeRecord :contactsMerge) {

            C501_MassMerge_SharedCode.OutputDebugLogText(LoggingLevel.DEBUG, '***** ' + String.valueOf(this).substring(0,String.valueOf(this).indexOf(':')) + '::FillListContacts mergeRecord Name: ' + mergeRecord.Merge_Metric_Name__c);

            Contact listRecord = contactMap.get(mergeRecord.Contact__c).clone(false, false, false, false);
            listRecord.C501_Merge_Id__c = mergeRecord.Id;
            listRecord.C501_Merge_Action__c = mergeRecord.Merge_Action__c;
            listRecord.C501_Merge_Confidence__c = mergeRecord.Merge_Confidence__c;
            listRecord.C501_Merge_Contact__c = mergeRecord.Contact__c;
            listRecord.C501_Merge_Target__c = mergeRecord.Merge_Target_Contact__c;
            listRecord.C501_Merge_Metric_Name__c = mergeRecord.Merge_Metric_Name__c;
            listRecord.C501_Merge_Metric__c = mergeRecord.Merge_Metric__c;
            listContacts.add(listRecord);

            C501_MassMerge_SharedCode.OutputDebugLogText(LoggingLevel.DEBUG, '***** ' + String.valueOf(this).substring(0,String.valueOf(this).indexOf(':')) + '::FillListContacts listRecord Name: ' + listRecord.C501_Merge_Metric_Name__c);

            // Switch Master and Duplicate
            listRecord = contactMap.get(mergeRecord.Merge_Target_Contact__c).clone(false, false, false, false);
            listRecord.C501_Merge_Id__c = mergeRecord.Id;
            listRecord.C501_Merge_Action__c = mergeRecord.Merge_Action__c;
            listRecord.C501_Merge_Confidence__c = mergeRecord.Merge_Confidence__c;
            listRecord.C501_Merge_Contact__c = mergeRecord.Merge_Target_Contact__c;
            listRecord.C501_Merge_Target__c = mergeRecord.Contact__c;
            listRecord.C501_Merge_Metric_Name__c = mergeRecord.Merge_Metric_Name__c;
            listRecord.C501_Merge_Metric__c = mergeRecord.Merge_Metric__c;
            listContacts.add(listRecord);

            C501_MassMerge_SharedCode.OutputDebugLogText(LoggingLevel.DEBUG, '***** ' + String.valueOf(this).substring(0,String.valueOf(this).indexOf(':')) + '::FillListContacts listRecord Name: ' + listRecord.C501_Merge_Metric_Name__c);
        }

        C501_MassMerge_SharedCode.OutputDebugLogFlush();
    }
    
    public Contact getTargetMerge(Contact masterObject)
    {
        for (Contact objectItem :listContacts) {
            if (String.isBlank(objectItem.C501_Merge_Id__c) || String.isBlank(objectItem.Id) || String.isBlank(masterObject.Id) || String.isBlank(masterObject.C501_Merge_Id__c)
                || objectItem.Id == masterObject.Id || objectItem.C501_Merge_Id__c <> masterObject.C501_Merge_Id__c) {
                continue;
            }

            return objectItem;
        }

        return null;
    }

    //save the Contacts created or modified
    public virtual PageReference SaveContacts() {
        
        if (listContacts == null) return null;
        
        strSaveResults = null;
        
        try {
            Map<String, C501_Contact_Merge__c> listContactsMergesIgnore = new Map<String, C501_Contact_Merge__c>();
            Map<String, C501_Contact_Merge__c> listContactsMergesMaster = new Map<String, C501_Contact_Merge__c>();

            C501_MassMerge_SharedCode.OutputDebugLogText(LoggingLevel.DEBUG, '***** C501_CTRL_MassMergeContacts::SaveContacts listContacts: ' + String.valueOf(listContacts));

            for (Contact contact : listContacts) {

                if (contact.Id != null && contact.C501_Merge_Action__c == 'Master') {

                    C501_MassMerge_SharedCode.OutputDebugLogText(LoggingLevel.DEBUG, '***** C501_CTRL_MassMergeContacts::SaveContacts Check Master Action - Merge Rule: ' + contact.C501_Merge_Metric_Name__c);

                    if (!listContactsMergesMaster.containsKey(contact.C501_Merge_Id__c)) {

                        Contact targetObject = getTargetMerge(contact);
                        if (targetObject == null) {
                            continue;
                        }

                        C501_MassMerge_SharedCode.OutputDebugLogText(LoggingLevel.DEBUG, '***** C501_CTRL_MassMergeContacts::SaveContacts master contact.Id: ' + contact.Id);

                        listContactsMergesMaster.put(contact.C501_Merge_Id__c, new C501_Contact_Merge__c(
                            Id = contact.C501_Merge_Id__c,
                            Contact__c = contact.Id,
                            Merge_Target_Contact__c = targetObject.Id,
                            Merge_Action__c = contact.C501_Merge_Action__c,
                            Merge_Metric__c = contact.C501_Merge_Metric__c,
                            // Set merge Confidence to 100% to ensure merge regardless of automerge percentage
                            Merge_Confidence__c = 100.0
                        ));

                        C501_MassMerge_SharedCode.OutputDebugLogText(LoggingLevel.DEBUG, '***** C501_CTRL_MassMergeContacts::SaveContacts master listContactsMergesMaster: ' + listContactsMergesMaster);

                    }
                }

                else if (contact.Id != null && contact.C501_Merge_Action__c == 'Ignore') {

                    C501_MassMerge_SharedCode.OutputDebugLogText(LoggingLevel.DEBUG, '***** C501_CTRL_MassMergeContacts::SaveContacts Check Ignore Action - Merge Rule: ' + contact.C501_Merge_Metric_Name__c);

                    if (!listContactsMergesIgnore.containsKey(contact.C501_Merge_Id__c)) {

                        Contact targetObject = getTargetMerge(contact);
                        if (targetObject == null) {
                            continue;
                        }

                        listContactsMergesIgnore.put(contact.C501_Merge_Id__c, new C501_Contact_Merge__c(
                            Id = contact.C501_Merge_Id__c,
                            Contact__c = contact.Id,
                            Merge_Target_Contact__c = targetObject.Id,
                            Merge_Action__c = contact.C501_Merge_Action__c
                        ));
                    }
                }
            }

            Integer mergesIgnoreSize = 0;
            if (!listContactsMergesIgnore.values().isEmpty()) {

                mergesIgnoreSize = listContactsMergesIgnore.size();
                Database.SaveResult[] updateResults = Database.update(listContactsMergesIgnore.values(), false);
                for (Database.SaveResult updateResult :updateResults) {
                    if (!updateResult.isSuccess()) {
                        for(Database.Error resultError : updateResult.getErrors()) {
                            C501_MassMerge_SharedCode.LogDatabaseError(resultError, String.valueOf(this).substring(0,String.valueOf(this).indexOf(':')) + '::SaveContacts');
                        }                        
                    }
                }
            }

            Integer mergesMasterSize = 0;
            if (!listContactsMergesMaster.values().isEmpty()) {

                mergesMasterSize = listContactsMergesMaster.size();
                Database.SaveResult[] updateResults = Database.update(listContactsMergesMaster.values(), false);
                for (Database.SaveResult updateResult :updateResults) {
                    if (!updateResult.isSuccess()) {
                        for(Database.Error resultError : updateResult.getErrors()) {
                            C501_MassMerge_SharedCode.LogDatabaseError(resultError, String.valueOf(this).substring(0,String.valueOf(this).indexOf(':')) + '::SaveContacts');
                        }                        
                    }
                }
                c501MassMerge_SharedCode_Contact.ProcessObjectMerges(new List<C501_Merge_Metric__c>(), listContactsMergesMaster.values());
            }

            strSaveResults = ( (mergesIgnoreSize + mergesMasterSize) * 2) + ' ' + Label.Merge_labelMassMergeSaveSuccess;
            ApexPages.addMessage(new ApexPages.message(ApexPages.Severity.CONFIRM, strSaveResults));

            // Refresh Contact and Contact Table
            ResetContacts();

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