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
// GENERIC CLASS - modify of the C501_MassMerge_SharedCode_[Object].cls classes then copy to the all the others
//
//      1) Make changes to the class
//      2) Copy/Paste entire class to all other C501_MassMerge_SharedCode_[Object].cls classes
//      3) Search and replace all [Object] references with [Target Object]
//
//  This is a workaround for Generic classes since <T> - Templating not supported in Apex
//

public without sharing class C501_MassMerge_SharedCode_Account { 

    public class C501_MassMerge_SharedCode_AccountException extends Exception {}

    public static String WhereFilterByMerges(String objectId) {

        // Where update to exclude previously found Accounts
        //
        String strSoqlWhere = '';

        return strSoqlWhere;

        // Disabled exclude to allow different rules to find the same pair because different rules can have different merge settings (e.g., 1 auto merge but the other doesn't)
/*
        Datetime currentDateTime = Datetime.now();
        Integer current24Hour = Integer.valueOf(currentDateTime.format('HH'));
        List<Id> listAccountIds = new List<Id>();

        String strSoql = C501_MassMerge_SharedCode.GetC501AccountMergeQuery();
        if (String.isBlank(strSoql)) {
            return '';
        }
        C501_MassMerge_SharedCode.OutputDebugLogText(LoggingLevel.DEBUG, '*****C501_MassMerge_SharedCode_Account::WhereFilterByMerges strSoql: ' + strSoql);
        Integer hourMode = math.mod(current24Hour, 3);
        for (C501_Account_Merge__c mergeRecord :Database.query(strSoql)) {

            Boolean addAccount = false;
            Boolean addAccountTarget = false;

            switch on hourMode {
                when 1 {
                    addAccount = true;
                }
                when 2 {
                    addAccountTarget = true;
                }
                when else {
                    addAccount = true;
                    addAccountTarget = true;
                }
            }

            if (addAccount && !listAccountIds.contains(mergeRecord.Account__c)) {
                listAccountIds.add(mergeRecord.Account__c);
            }
            if (addAccountTarget && !listAccountIds.contains(mergeRecord.Merge_Target_Account__c)) {
                listAccountIds.add(mergeRecord.Merge_Target_Account__c);
            }
        }

        if (!listAccountIds.isEmpty()) {

            Integer offset = (listAccountIds.size() <= (24 * 4)) ? listAccountIds.size() - 1 : listAccountIds.size() / 24;
            Integer startIndex = (listAccountIds.size() < (24 * 4)) ? 0 : current24Hour * offset;
            Integer endIndex = startIndex + offset > listAccountIds.size() - 1 ? listAccountIds.size() - 1 : startIndex + offset;

            Boolean firstItem = true;

            // Salesforce where max length is 4000
            while (startIndex <= endIndex && strSoqlWhere.length() < 3950) {
                if (!firstItem) {
                    strSoqlWhere += ',';
                }
                else {
                    strSoqlWhere += ' and (NOT ' + objectId + ' IN (';
                    firstItem = false;
                }
                strSoqlWhere += '\'' + listAccountIds[startIndex++] + '\'';
            }

            if (!firstItem) {
                strSoqlWhere += '))';
            }
        }

        return strSoqlWhere;
*/
    }

    // the list of Accounts
    public List<SelectOption> ListObjects(String mergeRule) {

        C501_MassMerge_SharedCode.OutputDebugLogText(LoggingLevel.DEBUG, '***** C501_MassMerge_SharedCode_Account::ListObjects - mergeRule: ' + mergeRule);

        Map<String, SelectOption> mapSelectOption = new Map<String, SelectOption>();
        mapSelectOption.put('', new SelectOption('', ''));

        String strSoqlFrom = ' FROM C501_Account_Merge__c';
        String strSoqlWhere = ' WHERE Merge_Action__c = null';
        strSoqlWhere += ' AND isDeleted = false';

        if (!String.isBlank(mergeRule)) {
            strSoqlWhere += ' AND Merge_Metric__r.Name = \'' + mergeRule + '\'';
        }

        String strSoqlSelect = 'SELECT Account__r.Name, Merge_Target_Account__r.Name';
        C501_MassMerge_SharedCode.OutputDebugLogText(LoggingLevel.DEBUG, '***** ' + String.valueOf(this).substring(0,String.valueOf(this).indexOf(':')) + '::ListObjects strSoql: ' + strSoqlSelect + strSoqlFrom + strSoqlWhere);
        for (C501_Account_Merge__c mergeRecord : Database.query(strSoqlSelect + strSoqlFrom + strSoqlWhere)) {

            if (mapSelectOption.size() > C501_MassMerge_SharedCode.MAXIMUM_COLLECTION_SIZE) {
                break;
            }

            try {

                if (String.isBlank(mergeRecord.Account__c) || String.isBlank(mergeRecord.Account__r.Name) || String.isBlank(mergeRecord.Merge_Target_Account__c) || String.isBlank(mergeRecord.Merge_Target_Account__r.Name)) {
                    continue;
                }
            }
            catch(Exception ex) {

                C501_MassMerge_SharedCode.OutputDebugLogText(LoggingLevel.DEBUG, '***** ' + String.valueOf(this).substring(0,String.valueOf(this).indexOf(':')) + '::ListObjects Invalid Lookup values to skip - ' + ex.getMessage());
                continue;
            }

            mapSelectOption.put(
                mergeRecord.Account__r.Name,
                new SelectOption(mergeRecord.Account__r.Name, mergeRecord.Account__r.Name));

            mapSelectOption.put(
                mergeRecord.Merge_Target_Account__r.Name,
                new SelectOption(mergeRecord.Merge_Target_Account__r.Name, mergeRecord.Merge_Target_Account__r.Name));
        }

        C501_MassMerge_SharedCode.OutputDebugLogText(LoggingLevel.DEBUG, '***** ' + String.valueOf(this).substring(0,String.valueOf(this).indexOf(':')) + '::ListObjects mapSelectOption: ' + mapSelectOption);

        List<SelectOption> listSOSorted = new List<SelectOption>();
        listSOSorted = C501_MassMerge_SharedCode.SelectOptionSortByLabel(mapSelectOption.values());

        return listSOSorted;
    }

    public void InsertObjectMergeCandidates(List<C501_Account_Merge__c> mergeObjects) {

        if (mergeObjects == null || mergeObjects.isEmpty()) {
            return;
        }

        //
		// Perform all DML (Data Manipulation Language) calls (insert, update, merge, delete, restore) outside of loop to avoid Salesforce Governor Limits: https://developer.salesforce.com/docs/atlas.en-us.apexcode.meta/apexcode/apex_gov_limits.htm
        //

        Set<String> mergeObjectsExisting = new Set<String>();
        List<C501_Account_Merge__c> deleteAccountMerges = new List<C501_Account_Merge__c>();

        String strSoql = C501_MassMerge_SharedCode.GetC501AccountMergeQuery();
        if (String.isBlank(strSoql)) {
            return;
        }
        strSoql += ' LIMIT ' + String.valueOf(C501_MassMerge_SharedCode.GetMaximumLimitSize(1));

        C501_MassMerge_SharedCode.OutputDebugLogText(LoggingLevel.DEBUG, '***** ' + String.valueOf(this).substring(0,String.valueOf(this).indexOf(':')) + '::InsertObjectMergeCandidates strSoql: ' + strSoql);

        for (C501_Account_Merge__c mergeObject :Database.query(strSoql)) {

            if (mergeObject.Account__c == null || mergeObject.Merge_Target_Account__c == null) {

                deleteAccountMerges.add(mergeObject);
                continue;
            }

            if (mergeObject.Account__r.IsDeleted) {

                deleteAccountMerges.add(mergeObject);
                continue;
            }

            if (mergeObject.Merge_Target_Account__r.IsDeleted) {

                deleteAccountMerges.add(mergeObject);
                continue;
            }

            mergeObjectsExisting.add(mergeObject.Unique_Id__c);
        }

        if (!deleteAccountMerges.isEmpty()) {
           C501_MassMerge_SharedCode.OutputDebugLogText(LoggingLevel.DEBUG, '***** ' + String.valueOf(this).substring(0,String.valueOf(this).indexOf(':')) + '::InsertObjectMergeCandidates Remove Invalid Account Merges: ' + deleteAccountMerges);
            Database.delete(deleteAccountMerges, false);
        }

		if (mergeObjects <> null && !mergeObjects.isEmpty()) {

            List<C501_Account_Merge__c> mergeObjectsInsert = new List<C501_Account_Merge__c>();
            for (C501_Account_Merge__c mergeObject :mergeObjects) {

                if (!mergeObjectsExisting.contains(mergeObject.Unique_Id__c)) {

                    mergeObjectsInsert.add(mergeObject);
                    mergeObjectsExisting.add(mergeObject.Unique_Id__c);
                }
            }

            if (!mergeObjectsInsert.isEmpty()) {

                Database.SaveResult[] insertResults = Database.insert(mergeObjectsInsert, false);
                for (Database.SaveResult result : insertResults) {
                    if (!result.isSuccess()) {
                        for(Database.Error resultError : result.getErrors()) {
                            C501_MassMerge_SharedCode.LogDatabaseError(resultError, String.valueOf(this).substring(0,String.valueOf(this).indexOf(':')) + '::InsertObjectMergeCandidates');
                        }                        
                    }
                }
            }
		}
    }

    public Integer ProcessObjectMergeCandidates(C501_Merge_Metric__c mergeMetric) {

        //
		// Perform all DML (Data Manipulation Language) calls (insert, update, merge, delete, restore) outside of loop to avoid Salesforce Governor Limits: https://developer.salesforce.com/docs/atlas.en-us.apexcode.meta/apexcode/apex_gov_limits.htm
        //

        String mergeMetricName = mergeMetric.Name;

        List<C501_Account_Merge__c> deleteAccountMerges = new List<C501_Account_Merge__c>();
        List<C501_Account_Merge__c> pendingMerges = new List<C501_Account_Merge__c>();
        List<C501_Account_Merge__c> pendingAutoMerges = new List<C501_Account_Merge__c>();

        Map<Id, Id> AccountHouseholdMap = new Map<Id, Id>();

        String strSoql = C501_MassMerge_SharedCode.GetC501AccountMergeQuery();
        if (String.isBlank(strSoql)) {
            return 0;
        }
        strSoql += ' AND Merge_Metric__r.Name = :mergeMetricName';
        strSoql += ' LIMIT ' + String.valueOf(C501_MassMerge_SharedCode.GetMaximumLimitSize(1));

        C501_MassMerge_SharedCode.OutputDebugLogText(LoggingLevel.DEBUG, '***** ' + String.valueOf(this).substring(0,String.valueOf(this).indexOf(':')) + '::ProcessObjectMergeCandidates strSoql: ' + strSoql);

        for (C501_Account_Merge__c mergeObject : Database.Query(strSoql)) {

            if (mergeObject.Account__c == null || mergeObject.Merge_Target_Account__c == null ||
                mergeObject.Account__r.IsDeleted || mergeObject.Merge_Target_Account__r.IsDeleted) {

                deleteAccountMerges.add(mergeObject);
                continue;
            }

            AccountHouseholdMap.put(mergeObject.Account__c, C501_MassMerge_SharedCode.GetMergeAccountHouseholdId(mergeObject));
            AccountHouseholdMap.put(mergeObject.Merge_Target_Account__c, C501_MassMerge_SharedCode.GetMergeTargetAccountHouseholdId(mergeObject));

            if (mergeObject.Merge_Action__c == 'Master') {
                pendingMerges.add(mergeObject);
            }
            else if (String.isBlank(mergeObject.Merge_Action__c)) {
                pendingAutoMerges.add(mergeObject);
            }
        }

        if (!deleteAccountMerges.isEmpty()) {
           C501_MassMerge_SharedCode.OutputDebugLogText(LoggingLevel.DEBUG, '***** ' + String.valueOf(this).substring(0,String.valueOf(this).indexOf(':')) + '::ProcessObjectMergeCandidates Remove Invalid Account Merges: ' + deleteAccountMerges);
            Database.delete(deleteAccountMerges, false);
        }

        // Check for batch queued merges
        if (!pendingMerges.isEmpty()) {

            return this.ProcessObjectMerges(new List<C501_Merge_Metric__c> {mergeMetric}, pendingMerges);
        }

        // Check if AutoMerge Enabled
        if (String.isBlank(mergeMetric.AutoMerge_Objects__c)) {
            return 0;
        }

        return C501_MassMerge_SharedCode.ProcessObjectMergeCandidates_Account(mergeMetric, pendingAutoMerges, AccountHouseholdMap);
    }

    // Legacy Method for Managed Package
    public Integer ProcessObjectMerges(List<C501_Account_Merge__c> objectMerges)
    {
        return this.ProcessObjectMerges(new List<C501_Merge_Metric__c>(), objectMerges);
    }

    public Integer ProcessObjectMerges(List<C501_Merge_Metric__c> mergeMetrics, List<C501_Account_Merge__c> objectMerges)
    {
        // Jobs with Status = 'Holding' applies to batch jobs in the Apex flex queue (max allowed 100)
        //  https://developer.salesforce.com/docs/atlas.en-us.object_reference.meta/api/sforce_api_objects_asyncapexjob.htm
        List<AsyncApexJob> flexQueueJobs = [SELECT Id FROM AsyncApexJob WHERE Status = 'Holding'];
        C501_MassMerge_SharedCode.OutputDebugLogText(LoggingLevel.DEBUG, '***** C501_MassMerge_SharedCode_Account::ProcessObjectMerges - flexQueueJobs ' + flexQueueJobs);
        C501_MassMerge_SharedCode.OutputDebugLogText(LoggingLevel.DEBUG, '***** C501_MassMerge_SharedCode_Account::ProcessObjectMerges - QueuableJobs ' + Limits.getQueueableJobs() + ' of ' + Limits.getLimitQueueableJobs());
        Integer remainingFlexQueueJobs = Limits.getLimitQueueableJobs() - flexQueueJobs.size();

        Set<String> mergeMetricSet = new Set<String>();
        for (C501_Merge_Metric__c mergeMetric :mergeMetrics) {
            mergeMetricSet.add(mergeMetric.Id);
        }

        Integer numberObjectMerges = 0;
        Set<Id> objectsProcessed = new Set<Id>();
        List<C501_Account_Merge__c> deleteAccountMerges = new List<C501_Account_Merge__c>();
        for (C501_Account_Merge__c mergeCandidate : objectMerges) {

            // Check Account and Target Account valid
            if (String.isBlank(mergeCandidate.Account__c) || String.isBlank(mergeCandidate.Merge_Target_Account__c)) {

                deleteAccountMerges.add(mergeCandidate);
                continue;
            }

            // Check Account and Target Account same Id
            if (mergeCandidate.Account__c == mergeCandidate.Merge_Target_Account__c) {

                deleteAccountMerges.add(mergeCandidate);
                continue;
            }

            // Check whether Account or Target Account Id merge already attempted
            if (objectsProcessed.contains(mergeCandidate.Account__c) || objectsProcessed.contains(mergeCandidate.Merge_Target_Account__c)) {

                deleteAccountMerges.add(mergeCandidate);
                continue;
            }

            if (!mergeMetricSet.contains(mergeCandidate.Merge_Metric__c)) {

                String strSoql = C501_MassMerge_SharedCode.GetC501MergeMetricQuery();
                if (String.isBlank(strSoql)) {

                    return numberObjectMerges;
                }

                C501_MassMerge_SharedCode.OutputDebugLogText(LoggingLevel.DEBUG, '***** ' + String.valueOf(this).substring(0,String.valueOf(this).indexOf(':')) + '::ProcessObjectMerges strSoql: ' + strSoql);

                mergeMetrics = Database.Query(strSoql);

                for (C501_Merge_Metric__c mergeMetric :mergeMetrics) {
                    mergeMetricSet.add(mergeMetric.Id);
                }

                if (!mergeMetricSet.contains(mergeCandidate.Merge_Metric__c)) {
                    throw new C501_MassMerge_SharedCode_AccountException('Account Merge Data Error - Invalid Merge Metric: ' + mergeCandidate.Merge_Metric__c + ' on Account Merge Id: ' + mergeCandidate.Id);
                }
            }
            
            objectsProcessed.add(mergeCandidate.Account__c);
            objectsProcessed.add(mergeCandidate.Merge_Target_Account__c);

            C501_MassMerge_SharedCode.OutputDebugLogText(LoggingLevel.DEBUG, '***** ' + String.valueOf(this).substring(0,String.valueOf(this).indexOf(':')) + '::ProcessObjectMerges ObjectsMerge');

            ObjectsMerge( 
                new Account(
                    Id = mergeCandidate.Account__c
                ),
                new List<Account> { new Account(
                    Id = mergeCandidate.Merge_Target_Account__c
                )
            } );

            numberObjectMerges++;

            break;
        }

        if (!deleteAccountMerges.isEmpty()) {
            C501_MassMerge_SharedCode.OutputDebugLogText(LoggingLevel.DEBUG, '***** ' + String.valueOf(this).substring(0,String.valueOf(this).indexOf(':')) + '::ProcessObjectMerges Remove Invalid Account Merges: ' + deleteAccountMerges);
            Database.delete(deleteAccountMerges, false);
        }

        return numberObjectMerges;
    }

    private void BackfillBlankFields(Account updateRecord, Set<String> blankFields, List<String> availableFieldValues) {

        for (String fieldName :blankFields) {

            if (availableFieldValues.isEmpty()) {
                break;
            }

            String fieldValue = String.valueOf(updateRecord.get(fieldName));
            if (String.isBlank(fieldValue)) {

                try{
                    String availableFieldValue = availableFieldValues[0];
                    availableFieldValues.remove(availableFieldValues.indexOf(availableFieldValue));
                    updateRecord.put(fieldName, availableFieldValue);
                } catch (Exception e) {
                    C501_MassMerge_SharedCode.OutputDebugLogText(LoggingLevel.ERROR, '***** C501_MassMerge_SharedCode_Object::ObjectsFieldMerge_Account_BackfillFields Update Merge Field - fieldName: ' + fieldName + ' Exception: ' + String.valueOf(e.getMessage()));
                }
            }
        }
    }

    private void BackfillCheckFields(String fieldName, String updateFieldValue, String availableFieldValue, Set<String> usedFieldValues, Set<String> blankFields, List<String> availableFieldValues ) {

        if (!String.isBlank(updateFieldValue)) {

            usedFieldValues.add(updateFieldValue);

            while (availableFieldValues.contains(updateFieldValue)) {
                availableFieldValues.remove(availableFieldValues.indexOf(updateFieldValue));
            }
        }
        else {
            blankFields.add(fieldName);
        }

        if (!String.isBlank(availableFieldValue) && !usedFieldValues.contains(availableFieldValue)) {
            availableFieldValues.add(availableFieldValue);
        }
    }

    public Account ObjectsFieldMerge(Account masterObject, List<Account> duplicateObjects) {

        Set<Id> mergeCandidates = new Set<Id> { masterObject.Id };
        for (Account objectItem :duplicateObjects) {
            mergeCandidates.add(objectItem.Id);
        }

        Map<String, Schema.SObjectField> objectFields = Schema.SObjectType.Account.fields.getMap();
        Map<String, Schema.DescribeFieldResult> fieldMap = new Map<String, Schema.DescribeFieldResult>();

        // Construct the query string
        String strSoql = 'Select ';
        for (String fieldName : objectFields.keySet()) {

            Schema.SObjectField f = objectFields.get(fieldName);
            Schema.DescribeFieldResult fResult = f.getDescribe();

            // Ignore fields that have an identical API name as an npsp prefixed field
            // This is due to a bug in dynamic SOQL, which causes querying for two real fields, 
            // e.g. npsp__foo__c and foo__c, to throw a duplicate field selected error.
            // Only select the NPSP namespaced field and ignore the same field without namespace
/*            if (!fieldName.startsWith('npsp__') && 
                objectFields.containsKey(UTIL_Namespace.StrTokenNSPrefix(fieldName))) {
                
                continue;
            }
*/

            // Only include the fields which are updateable and accessible and not unique or hidden
            //  externalId is fine as long as not a unique externalId
            //      Step 1: Update all master records while excluding unqiue value
            //      Step 2: Merge the duplicate into the master (thus deleting the duplicate)
            //      Step 3: Attempt to update unique values on the master
            if (fResult.isUpdateable() && fResult.isAccessible() && !C501_MassMerge_SharedCode.isNPSPHiddenField(fieldname)) {

                // If the field is type lookup, then ignore in the merge fields because is handled during the object merge
                string relName = fResult.getRelationshipName();
                if (String.isBlank(relName)) {
                    strSoql += (fieldName + ',');
                } else {
                    //C501_MassMerge_SharedCode.OutputDebugLogText(LoggingLevel.DEBUG, '***** C501_MassMerge_SharedCode_Account::ObjectsFieldMerge - skip relationship field: ' + fieldName + ' relName: ' + relName + ' fResult: ' + fResult);
                    continue;
                }

                fieldMap.put(fieldName, fResult);
            }
            else {
                //C501_MassMerge_SharedCode.OutputDebugLogText(LoggingLevel.DEBUG, '***** C501_MassMerge_SharedCode_Account::ObjectsFieldMerge - skip field based on fResult: ' + fieldName + ' fResult: ' + fResult);
            }
        }

        strSoql += ' Id FROM Account WHERE Id IN ' + C501_MassMerge_SharedCode.GetSafeSoqlList(mergeCandidates);
        strSoql += ' LIMIT ' + String.valueOf(C501_MassMerge_SharedCode.GetMaximumLimitSize(1));

        // Master object to hold the Unique values for attempted update after the merge
        Account masterUniqueObjectQuery = new Account(
            Id = masterObject.Id
        );
        
        List<Account> objectList;
        try {

            objectList = Database.query(strSoql); // Query the records

            // Ensure we got back the same number of records as expected. In case any record got deleted/moved since last search.
            if (objectList == null || objectList.size() != mergeCandidates.size()) {

                C501_MassMerge_SharedCode.OutputDebugLogText(LoggingLevel.ERROR, '***** C501_MassMerge_SharedCode_Account::ObjectsFieldMerge Query failed to return expected number of objects');
                return null;
            }

            // Get the Master Query Object
            Account masterObjectQuery = null;
            for (Integer objectIndex = 0; objectIndex < objectList.size(); objectIndex++) {
                if (objectList[objectIndex].Id == masterObject.Id) {

                    masterObjectQuery = objectList[objectIndex];
                    break;
                }
            }

            // Ensure we got back the same number of records as expected. In case any record got deleted/moved since last search.
            if (masterObjectQuery == null) {

                C501_MassMerge_SharedCode.OutputDebugLogText(LoggingLevel.ERROR, '***** C501_MassMerge_SharedCode_Account::ObjectsFieldMerge Query failed to find the master record fields');
                return null;
            }

            List<C501_Merge_Account_History__c> insertMergeHistories = new List<C501_Merge_Account_History__c>();

            // Merge fields into master
            Set<String> masterUsedEmailValues = new Set<String>();
            Set<String> masterBlankEmailFields = new Set<String>();
            List<String> availableEmailValues = new List<String>();
            Set<String> masterUsedPhoneValues = new Set<String>();
            Set<String> masterBlankPhoneFields = new Set<String>();
            List<String> availablePhoneValues = new List<String>();
            Set<String> addressTypesSkip = new Set<String>();

            // Assign Master Record
            for (Account objectItem :objectList) {

                // Check to assign master object
                if (masterObjectQuery.Id == objectItem.get('Id')) {

                    // Check Address
                    //
                    for (String addressType :C501_MassMerge_SharedCode_Object.AddressTypesAvailable) {
                        for (String addressField :C501_MassMerge_SharedCode_Object.AddressFieldsAvailable) {

                            C501_MassMerge_SharedCode.OutputDebugLogText(LoggingLevel.DEBUG, '***** C501_MassMerge_SharedCode_Account::ObjectsFieldMerge - check address field: ' + addressType + addressField);

                            try {
                                if (masterObjectQuery.get(addressType + addressField) != null &&
                                    !String.isBlank(String.valueOf(masterObjectQuery.get(addressType + addressField)))) {

                                    C501_MassMerge_SharedCode.OutputDebugLogText(LoggingLevel.DEBUG, '***** C501_MassMerge_SharedCode_Account::ObjectsFieldMerge - skip address field: ' + addressType + addressField);

                                    addressTypesSkip.add(addressType);
                                    break;
                                }
                            }
                            catch (Exception e) {
                                
                                if (!e.getMessage().contains('Invalid field')) {
                                    C501_MassMerge_SharedCode.OutputDebugLogText(LoggingLevel.ERROR, '***** C501_MassMerge_SharedCode_Account::ObjectsFieldMerge check address field exception: ' + String.valueOf(e.getMessage()));
                                }
                            }
                        }
                    }

                    break;
                }
            }

            C501_MassMerge_SharedCode.OutputDebugLogText(LoggingLevel.DEBUG, '***** C501_MassMerge_SharedCode_Account::ObjectsFieldMerge - addressTypesSkip: ' + addressTypesSkip);

            Mass_Merge_Config__c massMergeConfigSettings = Mass_Merge_Config__c.getOrgDefaults();
            Boolean writeAllMergeHistoryFields = massMergeConfigSettings == null ? false : massMergeConfigSettings.Merge_History_Write_All__c;
    
            for (Account objectItem :objectList) {

                // Ignore Master Object
                if (masterObjectQuery.Id == objectItem.get('Id')) {
                    continue;
                }

                String mergeActionIgnore = 'Ignore Duplicate';
                String mergeActionUse = 'Use Duplicate';
                String mergeActionError = 'Error';

                // Same Id history so at least 1 record of the merge in history
                insertMergeHistories.add(new C501_Merge_Account_History__c(
                    Account__c = masterObjectQuery.Id,
                    Merge_Value__c = masterObjectQuery.Id,
                    Ignore_Value__c = String.valueOf(objectItem.get('Id')),
                    Field__c =  'Id',
                    Action__c = mergeActionIgnore
                ));

                for (String fieldName :fieldMap.keySet()) {

                    String mergeAction = mergeActionIgnore;
                    Boolean skipMasterFieldUpdate = false;
                    String masterFieldValue = String.valueOf(masterObjectQuery.get(fieldName));
                    String duplicateFieldValue = String.valueOf(objectItem.get(fieldName));

                    C501_MassMerge_SharedCode.OutputDebugLogText(LoggingLevel.DEBUG, '***** C501_MassMerge_SharedCode_Account::ObjectsFieldMerge - fieldName: ' + fieldName + ' masterFieldValue: ' + masterFieldValue + ' duplicateFieldValue: ' + duplicateFieldValue);

                    // Handle Phone
                    //      Future Update: Change to if fieldName contains Phone and Datatype = Phone or can determine if a Phone field based on format? xxxx-xxx
                    if (C501_MassMerge_SharedCode_Object.ObjectsFieldMerge_Account_HandlePhone(fieldName, fieldMap.get(fieldName))) {

                        this.BackfillCheckFields(fieldName, masterFieldValue, duplicateFieldValue, masterUsedPhoneValues, masterBlankPhoneFields, availablePhoneValues);
                    }

                    // Handle Email
                    //      Future Update: Change to if fieldName contains Email and Datatype = Email
                    if (C501_MassMerge_SharedCode_Object.ObjectsFieldMerge_Account_HandleEmail(fieldName, fieldMap.get(fieldName))) {

                        this.BackfillCheckFields(fieldName, masterFieldValue, duplicateFieldValue, masterUsedEmailValues, masterBlankEmailFields, availableEmailValues);
                    }

                    // Handle Address
                    //      Check to skip address if the master a valid value for any of the address type fields
                    //
                    for (String addressType : addressTypesSkip) {
                        for (String addressField : C501_MassMerge_SharedCode.AddressFieldsAvailable) {
                            if (fieldName.equalsIgnoreCase(addressType + addressField)) {
                                C501_MassMerge_SharedCode.OutputDebugLogText(LoggingLevel.DEBUG, '***** C501_MassMerge_SharedCode_Account::ObjectsFieldMerge - skip address field: ' + fieldName);
                                skipMasterFieldUpdate = true;
                            }
                        }
                    }

                    Boolean writeHistoryRecord = false;
                    if (skipMasterFieldUpdate) {
                        if (!String.isBlank(duplicateFieldValue) && masterFieldValue <> duplicateFieldValue) {
                            writeHistoryRecord = true;
                        }
                    }
                    else {

                        if (String.isBlank(masterFieldValue) && !String.isBlank(duplicateFieldValue)) {

                            writeHistoryRecord = writeAllMergeHistoryFields;

                            try {

                                if (fieldMap.get(fieldName).isUnique()) {

                                    // Get Field directly from get call so proper Object type is used
                                    masterUniqueObjectQuery.put(fieldName, objectItem.get(fieldName));

                                } else {

                                    // Get Field directly from get call so proper Object type is used
                                    masterObjectQuery.put(fieldName, objectItem.get(fieldName));
                                }

                                mergeAction = mergeActionUse;

                            } catch (Exception e) {

                                C501_MassMerge_SharedCode.OutputDebugLogText(LoggingLevel.ERROR, '***** C501_MassMerge_SharedCode_Account::ObjectsFieldMerge Update Merge Standard Field - fieldName: ' + fieldName + ' Exception: ' + String.valueOf(e.getMessage()));
                                mergeAction = mergeActionError;
                                return null;
                            }
                        }
                        else if (!String.isBlank(masterFieldValue) && !String.isBlank(duplicateFieldValue) && masterFieldValue <> duplicateFieldValue) {
                            writeHistoryRecord = true;
                        }
                    }

                    if (writeHistoryRecord) {
                        insertMergeHistories.add(new C501_Merge_Account_History__c(
                            Account__c = masterObjectQuery.Id,
                            Merge_Value__c = String.isBlank(masterFieldValue) ? '(blank)' : masterFieldValue,
                            Ignore_Value__c = String.isBlank(duplicateFieldValue) ? '(blank)' : duplicateFieldValue,
                            Field__c =  fieldName,
                            Action__c = mergeAction
                        ));
                    }
                }
            }

            // Check to backfill emails
            //
            this.BackfillBlankFields(masterObjectQuery, masterBlankEmailFields, availableEmailValues);

            // Check to backfill phones
            //
            this.BackfillBlankFields(masterObjectQuery, masterBlankPhoneFields, availablePhoneValues);

            Database.SaveResult updateResult = Database.update(masterObjectQuery, false);
            if (!updateResult.isSuccess()) {

                for (Database.Error resultError : updateResult.getErrors()) {

                    C501_MassMerge_SharedCode.LogDatabaseError(resultError, String.valueOf(this).substring(0,String.valueOf(this).indexOf(':')) + '::ObjectsFieldMerge');
                }

                C501_MassMerge_SharedCode.OutputDebugLogText(LoggingLevel.ERROR, '***** C501_MassMerge_SharedCode_Account::ObjectsFieldMerge - Database.update: ' + updateResult);
                return null;
            }

            if (!insertMergeHistories.isEmpty()) {
                Database.SaveResult[] insertResults = Database.insert(insertMergeHistories, false);
                for (Database.SaveResult insertResult :insertResults) {
                    if (!insertResult.isSuccess()) {
                        for (Database.Error resultError : insertResult.getErrors()) {
                            C501_MassMerge_SharedCode.LogDatabaseError(resultError, String.valueOf(this).substring(0,String.valueOf(this).indexOf(':')) + '::ObjectsFieldMerge');
                        }
                    }
                }
            }
        } catch (Exception e) {

            C501_MassMerge_SharedCode.OutputDebugLogText(LoggingLevel.ERROR, '***** C501_MassMerge_SharedCode_Account::ObjectsFieldMerge Update fields exception: ' + String.valueOf(e.getMessage()));
            return null;
        }

        C501_MassMerge_SharedCode.OutputDebugLogFlush();
        return masterUniqueObjectQuery;
    }

    public void ObjectsMerge(Account masterObject, List<Account> duplicateObjects) {

        if (masterObject == null || duplicateObjects == null || duplicateObjects.isEmpty()) {
            return;
        }

        C501_MassMerge_SharedCode_Object.ObjectsMerge_Account(masterObject, duplicateObjects);

        // Merge fields into master
        Account masterObjectUniqueValues = ObjectsFieldMerge(masterObject, duplicateObjects);
        if (masterObjectUniqueValues <> null) {

            C501_MassMerge_SharedCode.OutputDebugLogText(LoggingLevel.DEBUG, '***** C501_MassMerge_SharedCode_Account::ObjectsMerge Account Merge Master: ' + masterObject.Id + ' Duplicate: ' + duplicateObjects[0].Id);

            // Merge Accounts into master
            Database.MergeResult[] objectResults = Database.merge(masterObject, duplicateObjects, false);
            for(Database.MergeResult res : objectResults) {
                if (!res.isSuccess()) {
                    for (Database.Error err : res.getErrors()) {
                        C501_MassMerge_SharedCode.OutputDebugLogText(LoggingLevel.ERROR, '***** C501_MassMerge_SharedCode_Account::ObjectsMerge Account Merge Exception: ' + err.getMessage());
                    }

                    throw new C501_MassMerge_SharedCode_AccountException('Account Merge Attempt - ' + String.valueOf(res.getErrors()));
                }
            }

            // Attempt to update unique values since the duplicate object is now deleted
            //  If any failures in here they can be ignored because just making an attempt to bring over Unique Values after the merge is already over
            //      Could wrap all this in a transaction if need to unwind the whole thing.
            //      If getting errors here then before the merge (or maybe can update the deleted duplicate or force delete on the record) recommend blank out the unique values on the duplicate but that might cause validation errors so we need to not rethrow any exceptions
            Database.SaveResult updateResult = Database.update(masterObjectUniqueValues, false);
            if (!updateResult.isSuccess()) {

                for (Database.Error resultError : updateResult.getErrors()) {

                    C501_MassMerge_SharedCode.LogDatabaseError(resultError, String.valueOf(this).substring(0,String.valueOf(this).indexOf(':')) + '::ObjectsMerge Unique Values');
                }

                C501_MassMerge_SharedCode.OutputDebugLogText(LoggingLevel.ERROR, '***** C501_MassMerge_SharedCode_Account::ObjectsMerge Unique Values - Database.update: ' + updateResult);
            }
        }

        Set<Id> objectIds = new Set<Id> { masterObject.Id };
        for (Account duplicateObject :duplicateObjects) {
            objectIds.add(duplicateObject.Id);
        }

        String strSoql = C501_MassMerge_SharedCode.GetC501AccountMergeQuery();
        if (String.isBlank(strSoql)) {
            return;
        }
        strSoql += ' AND (Account__c = null OR Merge_Target_Account__c = null OR Account__c in ' + C501_MassMerge_SharedCode.GetSafeSoqlList(objectIds) + ' OR Merge_Target_Account__c in ' + C501_MassMerge_SharedCode.GetSafeSoqlList(objectIds) + ')';
        strSoql += ' LIMIT ' + String.valueOf(C501_MassMerge_SharedCode.GetMaximumLimitSize(1));

        C501_MassMerge_SharedCode.OutputDebugLogText(LoggingLevel.DEBUG, '***** ' + String.valueOf(this).substring(0,String.valueOf(this).indexOf(':')) + '::ObjectsMerge strSoql: ' + strSoql);
        
        List<C501_Account_Merge__c> deleteAccountMerges = Database.query(strSoql);

        if (!deleteAccountMerges.isEmpty()) {
           C501_MassMerge_SharedCode.OutputDebugLogText(LoggingLevel.DEBUG, '***** ' + String.valueOf(this).substring(0,String.valueOf(this).indexOf(':')) + '::ProcessObjectMerges Remove Invalid Account Merges: ' + deleteAccountMerges);
            Database.delete(deleteAccountMerges, false);
        }

        C501_MassMerge_SharedCode.OutputDebugLogFlush();
   }
}