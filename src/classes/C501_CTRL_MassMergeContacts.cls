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

public virtual with sharing class C501_CTRL_MassMergeContacts {

    private C501_MassMerge_SharedCode_Contact c501MassMerge_SharedCode_Contact;
    private List<String> listStrFields;
    
    public boolean redirect { get; private set; }
    public ID idRedirect { get; private set; }
    
    // constructor
    public C501_CTRL_MassMergeContacts() {

        System.debug(LoggingLevel.DEBUG, '***** C501_CTRL_MassMergeContacts::C501_CTRL_MassMergeContacts Constructor');

        redirect = false;
        c501MassMerge_SharedCode_Contact = new C501_MassMerge_SharedCode_Contact();
        
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
        if (!ListPartialcontains(listStrFields, 'C501_Merge_Contact__c')) {
            listStrFields.add('C501_Merge_Contact__c');
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
                if (fieldSetName.contains('massmergecontacts_custom')) {
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
                objectFilter.C501_Merge_Confidence_From__c = 90.0;
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
        objectFilter.Id = null;
        listContacts = null;
        strSaveResults = null;

        return null;
    }

    // the user has changed the Confidence query
    public virtual PageReference ChangeMergeConfidence() {

        listSOContacts = null;
        objectFilter.Id = null;
        listContacts = null;
        strSaveResults = null;

        return null;
    }

    public virtual void ResetContacts() {

        System.debug(LoggingLevel.DEBUG, '***** C501_CTRL_MassMergeContacts::ResetContacts');

        listSOContacts = null;
        objectFilter.Id = null;
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
        
    // helper function that the client can override, to query for the appropriate fields in contacts.
    public virtual void FillListContacts() {

		Map<Id,C501_Contact_Merge__c> contactsMerge = new Map<Id,C501_Contact_Merge__c>();

        if (objectFilter.Id <> null) {
        
            String strSoql = C501_MassMerge_SharedCode.GetC501ContactMergeQuery();
            strSoql += ' AND (Contact__c = \'' + objectFilter.Id + '\' OR Merge_Target_Contact__c = \'' + objectFilter.Id + '\') AND Merge_Action__c = null';
            strSoql += ' LIMIT 1';

            System.debug(LoggingLevel.DEBUG, '***** ' + String.valueOf(this).substring(0,String.valueOf(this).indexOf(':')) + '::FillListContacts strSoql: ' + strSoql);

            for(C501_Contact_Merge__c contactMerge :Database.query(strSoql)) {

                    contactsMerge.put(
                        contactMerge.Contact__c,
                        new C501_Contact_Merge__c(
                            Id = contactMerge.Id,
                            Merge_Action__c = contactMerge.Merge_Action__c,
                            Merge_Confidence__c = null,
                            Contact__c = contactMerge.Contact__c,
                            Merge_Target_Contact__c = contactMerge.Merge_Target_Contact__c,
                            Merge_Metric__c = contactMerge.Merge_Metric__c,
                            Merge_Metric_Name__c = 'Merge ' + contactMerge.Merge_Metric__r.Name
                    ));

                    contactsMerge.put(
                        contactMerge.Merge_Target_Contact__c,
                        new C501_Contact_Merge__c(
                            Id = contactMerge.Id,
                            Merge_Action__c = contactMerge.Merge_Action__c,
                            Merge_Confidence__c = contactMerge.Merge_Confidence__c,
                            Contact__c = contactMerge.Contact__c,
                            Merge_Target_Contact__c = contactMerge.Merge_Target_Contact__c,
                            Merge_Metric__c = contactMerge.Merge_Metric__c,
                            Merge_Metric_Name__c = 'Merge ' + contactMerge.Merge_Metric__r.Name
                    ));
            }
        }
        else {

            Set<String> objectIds = new Set<String>();
            for (SelectOption option : listSOContacts) {
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

            String strSoql = C501_MassMerge_SharedCode.GetC501ContactMergeQuery();
            strSoql += ' AND (Contact__c in ' + C501_MassMerge_SharedCode.GetSafeSoqlList(objectIds) + ' OR Merge_Target_Contact__c in ' + C501_MassMerge_SharedCode.GetSafeSoqlList(objectIds) + ') AND Merge_Action__c = null';
            strSoql += strRuleSoql;
            strSoql += ' ORDER BY Contact__r.Name ASC';

            System.debug(LoggingLevel.DEBUG, '***** ' + String.valueOf(this).substring(0,String.valueOf(this).indexOf(':')) + '::FillListContacts strSoql: ' + strSoql);

            for(C501_Contact_Merge__c contactMerge : Database.Query(strSoql)) {

                    if (contactsMerge.containsKey(contactMerge.Contact__c) || contactsMerge.containsKey(contactMerge.Merge_Target_Contact__c)) {
                        continue;
                    }

                    contactsMerge.put(
                        contactMerge.Contact__c,
                        new C501_Contact_Merge__c(
                            Id = contactMerge.Id,
                            Merge_Action__c = contactMerge.Merge_Action__c,
                            Merge_Confidence__c = null,
                            Contact__c = contactMerge.Contact__c,
                            Merge_Target_Contact__c = contactMerge.Merge_Target_Contact__c,
                            Merge_Metric__c = contactMerge.Merge_Metric__c,
                            Merge_Metric_Name__c = 'Merge ' + mergeBuffer + mergeIndex + ' ' + contactMerge.Merge_Metric__r.Name
                    ));

                    contactsMerge.put(
                        contactMerge.Merge_Target_Contact__c,
                        new C501_Contact_Merge__c(
                            Id = contactMerge.Id,
                            Merge_Action__c = contactMerge.Merge_Action__c,
                            Merge_Confidence__c = contactMerge.Merge_Confidence__c,
                            Contact__c = contactMerge.Contact__c,
                            Merge_Target_Contact__c = contactMerge.Merge_Target_Contact__c,
                            Merge_Metric__c = contactMerge.Merge_Metric__c,
                            Merge_Metric_Name__c = 'Merge ' + mergeBuffer + mergeIndex + ' ' + contactMerge.Merge_Metric__r.Name
                    ));

                    mergeIndex++;
                    if (mergeIndex >= 10) {
                        mergeBuffer = '';
                    }
            }
        }

        String strSoql = 'SELECT ' + String.join(listStrFields, ',');

        Set<Id> contactsMergeIds = contactsMerge.keySet();
        
        strSoql += ' FROM Contact WHERE Id in ' + C501_MassMerge_SharedCode.GetSafeSoqlList(contactsMergeIds);
        strSoql += ' order by C501_Merge_Metric_Name__c ASC NULLS FIRST';
        
        System.debug(LoggingLevel.DEBUG, '***** ' + String.valueOf(this).substring(0,String.valueOf(this).indexOf(':')) + '::FillListContacts strSoql: ' + strSoql);

        listContacts = Database.Query(strSoql);

        for ( Contact Contact :listContacts ) {
            if (contactsMerge.containsKey(contact.Id)) {
                C501_Contact_Merge__c mergeContact = contactsMerge.get(contact.Id);
                contact.C501_Merge_Id__c = mergeContact.Id;
                contact.C501_Merge_Action__c = mergeContact.Merge_Action__c;
                contact.C501_Merge_Confidence__c = mergeContact.Merge_Confidence__c;
                contact.C501_Merge_Contact__c = mergeContact.Contact__c;
                contact.C501_Merge_Target__c = mergeContact.Merge_Target_Contact__c;
                contact.C501_Merge_Metric_Name__c = mergeContact.Merge_Metric_Name__c;
                contact.C501_Merge_Metric__c = mergeContact.Merge_Metric__c;
            }
        }

        // Update Contacts w/ Merge fields then query to get the correct sort order by Metric Name
        update listContacts;

        System.debug(LoggingLevel.DEBUG, '***** ' + String.valueOf(this).substring(0,String.valueOf(this).indexOf(':')) + '::FillListContacts strSoql: ' + strSoql);

        listContacts = Database.Query(strSoql);
    }
    
    // used to track the Id's of all Contact records that get modified.
    public Set<ID> setContactsIdDirty {
        get {
            if (setContactsIdDirty == null) {
                setContactsIdDirty = new Set<ID>();
            }
            return setContactsIdDirty;
        }
        set;
    }
    
    // to allow vf page to see how many Contact records are dirty
    public Integer numberOfRecordsDirty {
        get { 
            if (setContactsIdDirty == null)
                return 0;
            else
                return setContactsIdDirty.size();
        }
    }
    
    // this gets called whenever a field in the Contacts datatable is modified.
    // use this to remember which existing Contacts records have been modified.
    public virtual PageReference ContactsRecordModified() {
        String strId = ApexPages.currentPage().getParameters().get('usersIdDirtied');
        if (strId != null && strId != '') setContactsIdDirty.add(strId);
        return null;
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

            System.debug(LoggingLevel.DEBUG, '***** C501_CTRL_MassMergeContacts::SaveContacts listContacts: ' + String.valueOf(listContacts));

            for (Contact contact : listContacts) {

                if (contact.Id != null && contact.C501_Merge_Action__c == 'Master') {

                    System.debug(LoggingLevel.DEBUG, '***** C501_CTRL_MassMergeContacts::SaveContacts Check Master Action - Merge Rule: ' + contact.C501_Merge_Metric_Name__c);

                    // we couldn't reliably mark dirty records in all scenarios,
                    // so we've decided it is better to always update all records.
                    //if (setContactsIdDirty.contains(contact.Id)) 

                    if (!listContactsMergesMaster.containsKey(contact.C501_Merge_Id__c)) {

                        Contact targetObject = getTargetMerge(contact);
                        if (targetObject == null) {
                            continue;
                        }

                        listContactsMergesMaster.put(contact.C501_Merge_Id__c, new C501_Contact_Merge__c(
                            Id = contact.C501_Merge_Id__c,
                            Contact__c = contact.Id,
                            Merge_Target_Contact__c = targetObject.Id,
                            Merge_Action__c = contact.C501_Merge_Action__c,
                            Merge_Metric__c = contact.C501_Merge_Metric__c,
                            // Set merge Confidence to 100% to ensure merge regardless of automerge percentage
                            Merge_Confidence__c = 100.0
                        ));
                    }
                }

                else if (contact.Id != null && contact.C501_Merge_Action__c == 'Ignore') {

                    System.debug(LoggingLevel.DEBUG, '***** C501_CTRL_MassMergeContacts::SaveContacts Check Ignore Action - Merge Rule: ' + contact.C501_Merge_Metric_Name__c);

                    // we couldn't reliably mark dirty records in all scenarios,
                    // so we've decided it is better to always update all records.
                    //if (setContactsIdDirty.contains(contact.Id)) 

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

            if (!listContactsMergesIgnore.values().isEmpty()) {
                update listContactsMergesIgnore.values();
            }

            if (!listContactsMergesMaster.values().isEmpty()) {

                update listContactsMergesMaster.values();
                c501MassMerge_SharedCode_Contact.ProcessObjectMerges(new List<C501_Merge_Metric__c>(), listContactsMergesMaster.values(), true);
            }

            strSaveResults = ( (listContactsMergesIgnore.values().size() + listContactsMergesMaster.values().size()) * 2) + ' ' + Label.Merge_labelMassMergeSaveSuccess;
            ApexPages.addMessage(new ApexPages.message(ApexPages.Severity.CONFIRM, strSaveResults));

            // successful save, so clear out dirty list
            setContactsIdDirty = null;

            // Refresh Contact and Contact Table
            ResetContacts();

            return null;            

        } catch (Exception e) {
            strSaveResults = Label.Merge_labelMassMergeErrorOnSave;
            ApexPages.addMessages(e); 
            return null;
        }
    }

    //save the Contacts modified, and then close the page.
    public virtual PageReference SaveAndCloseContacts() {
        SaveContacts();
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