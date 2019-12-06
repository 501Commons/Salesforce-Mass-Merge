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
    public List<SelectOption> ListObjectsWithConfidence(String mergeRule, Decimal fromMergeConfidence, Decimal toMergeConfidence) {

        C501_MassMerge_SharedCode.OutputDebugLogText(LoggingLevel.DEBUG, '***** C501_MassMerge_SharedCode_Account::ListObjectsWithConfidence - mergeRule: ' + mergeRule 
            + ' fromMergeConfidence: ' + fromMergeConfidence.toPlainString()
            + ' toMergeConfidence: ' + toMergeConfidence.toPlainString());

        Map<String, SelectOption> mapSelectOption = new Map<String, SelectOption>();
        mapSelectOption.put('', new SelectOption('', ''));

        String strSoqlFrom = ' FROM C501_Account_Merge__c';
        String strSoqlWhere = ' WHERE Merge_Confidence__c >= '  + fromMergeConfidence;
        strSoqlWhere += '  AND Merge_Confidence__c <= ' + toMergeConfidence;
        strSoqlWhere += '  AND Merge_Action__c = null';
        strSoqlWhere += '  AND isDeleted = false';
        if (!String.isBlank(mergeRule)) {
            strSoqlWhere += '  AND Merge_Metric__r.Name = \'' + mergeRule + '\'';
        }

        // Divide the Limit by 2 because the merge objects hold a pair of objects that are split into 2 for display
        String strSoqlLimit = ' LIMIT ' + String.valueOf(C501_MassMerge_SharedCode.GetMaximumCollectionSize() / 2);

        // Get Accounts using the Account_c field
        //
        String strSoqlSelect = 'SELECT Account__c, Account__r.Name';
        String strSoqlGroupBy = ' GROUP BY Account__c, Account__r.Name';
        C501_MassMerge_SharedCode.OutputDebugLogText(LoggingLevel.DEBUG, '***** ' + String.valueOf(this).substring(0,String.valueOf(this).indexOf(':')) + '::ListObjectsWithConfidence strSoql1: ' + strSoqlSelect + strSoqlFrom + strSoqlWhere + strSoqlGroupBy + strSoqlLimit);
        for (AggregateResult result : Database.query(strSoqlSelect + strSoqlFrom + strSoqlWhere + strSoqlGroupBy + strSoqlLimit)) {

            // Attempt to get based on Namespace
            String objectFieldValue;
            try {
                objectFieldValue = result.get(C501_MassMerge_SharedCode.CurrentNamespace + 'Account__c').toString();
            }
            catch (System.SObjectException objectException) {
                objectFieldValue = result.get('Account__c').toString();
            }

            mapSelectOption.put(
                objectFieldValue,
                new SelectOption(objectFieldValue, result.get('Name').toString()));
        }

        // Get Accounts using the Merge_Target_Account_c field
        //
        strSoqlSelect = 'SELECT Merge_Target_Account__c, Merge_Target_Account__r.Name';
        strSoqlGroupBy = ' GROUP BY Merge_Target_Account__c, Merge_Target_Account__r.Name';
        C501_MassMerge_SharedCode.OutputDebugLogText(LoggingLevel.DEBUG, '***** ' + String.valueOf(this).substring(0,String.valueOf(this).indexOf(':')) + '::ListObjectsWithConfidence strSoql2: ' + strSoqlSelect + strSoqlFrom + strSoqlWhere + strSoqlGroupBy + strSoqlLimit);
        for (AggregateResult result : Database.query(strSoqlSelect + strSoqlFrom + strSoqlWhere + strSoqlGroupBy + strSoqlLimit)) {

            // Attempt to get based on Namespace
            String objectFieldValue;
            try {
                objectFieldValue = result.get(C501_MassMerge_SharedCode.CurrentNamespace + 'Merge_Target_Account__c').toString();
            }
            catch (System.SObjectException objectException) {
                objectFieldValue = result.get('Merge_Target_Account__c').toString();
            }

            mapSelectOption.put(
                objectFieldValue,
                new SelectOption(objectFieldValue, result.get('Name').toString()));
        }

        List<SelectOption> listSOSorted = new List<SelectOption>();
        listSOSorted = C501_MassMerge_SharedCode.SelectOptionSortByLabel(mapSelectOption.values());

        return listSOSorted;
    }

    public void InsertObjectMergeCandidates(C501_Merge_Metric__c mergeMetric, List<C501_Account_Merge__c> mergeObjects) {

        if (mergeObjects == null || mergeObjects.isEmpty()) {
            return;
        }

        //
		// Perform all DML (Data Manipulation Language) calls (insert, update, merge, delete, restore) outside of loop to avoid Salesforce Governor Limits: https://developer.salesforce.com/docs/atlas.en-us.apexcode.meta/apexcode/apex_gov_limits.htm
        //

        String mergeMetricName = mergeMetric.Name;

        Set<String> mergeObjectsExisting = new Set<String>();
        List<C501_Account_Merge__c> deleteAccountMerges = new List<C501_Account_Merge__c>();

        String strSoql = C501_MassMerge_SharedCode.GetC501AccountMergeQuery();
        if (String.isBlank(strSoql)) {
            return;
        }
        strSoql += ' AND Merge_Metric__r.Name = \'' + mergeMetricName + '\'';
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

        for(C501_Account_Merge__c mergeObject : Database.Query(strSoql)) {

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

            return this.ProcessObjectMerges(new List<C501_Merge_Metric__c> {mergeMetric}, pendingMerges, false);
        }

        // Check if AutoMerge Enabled
        if (String.isBlank(mergeMetric.AutoMerge_Objects__c)) {
            return 0;
        }

        return C501_MassMerge_SharedCode.ProcessObjectMergeCandidates_Account(mergeMetric, pendingAutoMerges, AccountHouseholdMap);
    }

    // Legacy Method for Managed Package
    public Integer ProcessObjectMerges(List<C501_Account_Merge__c> objectMerges, Boolean batchMerge)
    {
        return this.ProcessObjectMerges(new List<C501_Merge_Metric__c>(), objectMerges, batchMerge);
    }

    public Integer ProcessObjectMerges(List<C501_Merge_Metric__c> mergeMetrics, List<C501_Account_Merge__c> objectMerges, Boolean batchMerge)
    {

        // Jobs with Status = 'Holding' applies to batch jobs in the Apex flex queue (max allowed 100)
        //  https://developer.salesforce.com/docs/atlas.en-us.object_reference.meta/api/sforce_api_objects_asyncapexjob.htm
        List<AsyncApexJob> flexQueueJobs = [SELECT Id FROM AsyncApexJob WHERE Status = 'Holding'];
        C501_MassMerge_SharedCode.OutputDebugLogText(LoggingLevel.DEBUG, '***** C501_MassMerge_SharedCode_Account::ProcessObjectMerges - flexQueueJobs ' + flexQueueJobs);
        C501_MassMerge_SharedCode.OutputDebugLogText(LoggingLevel.DEBUG, '***** C501_MassMerge_SharedCode_Account::ProcessObjectMerges - QueuableJobs ' + Limits.getQueueableJobs() + ' of ' + Limits.getLimitQueueableJobs());
        Integer remainingFlexQueueJobs = Limits.getLimitQueueableJobs() - flexQueueJobs.size();

        Map<String, Decimal> mergeMetricMap = new Map<String, Decimal>();
        for (C501_Merge_Metric__c mergeMetric :mergeMetrics) {
            mergeMetricMap.put(mergeMetric.Id, mergeMetric.AutoMerge_Percentage__c);
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

            // Check Automerge Percentage
            if (!mergeMetricMap.containsKey(mergeCandidate.Merge_Metric__c)) {
                String strSoql = C501_MassMerge_SharedCode.GetC501MergeMetricQuery();
                if (String.isBlank(strSoql)) {
                    return numberObjectMerges;
                }

                C501_MassMerge_SharedCode.OutputDebugLogText(LoggingLevel.DEBUG, '***** ' + String.valueOf(this).substring(0,String.valueOf(this).indexOf(':')) + '::ProcessObjectMerges strSoql: ' + strSoql);

                mergeMetrics = Database.Query(strSoql);

                for (C501_Merge_Metric__c mergeMetric :mergeMetrics) {
                    mergeMetricMap.put(mergeMetric.Id, mergeMetric.AutoMerge_Percentage__c);
                }

                if (!mergeMetricMap.containsKey(mergeCandidate.Merge_Metric__c)) {
                    throw new C501_MassMerge_SharedCode_AccountException('Account Merge Data Error - Invalid Merge Metric: ' + mergeCandidate.Merge_Metric__c + ' on Account Merge Id: ' + mergeCandidate.Id);
                }
            }
            
            // Check to merge if Merge Confidence is greater than or equal to AutoMerge Confidence
            //      No Merge:   Merge Confidence 50%, AutoMerge Confidence 90%
            //      No Merge:   Merge Confidence 90%, AutoMerge Confidence 100%
            //      Merge:      Merge Confidence 90%, AutoMerge Confidence 0%
            //      Merge:      Merge Confidence 90%, AutoMerge Confidence 90%
            //      Merge:      Merge Confidence 90%, AutoMerge Confidence 80%
            if (mergeCandidate.Merge_Confidence__c < mergeMetricMap.get(mergeCandidate.Merge_Metric__c)) {
                continue;
            }

            objectsProcessed.add(mergeCandidate.Account__c);
            objectsProcessed.add(mergeCandidate.Merge_Target_Account__c);

            if (batchMerge) {

                C501_MassMerge_SharedCode.OutputDebugLogText(LoggingLevel.DEBUG, '***** ' + String.valueOf(this).substring(0,String.valueOf(this).indexOf(':')) + '::ProcessObjectMerges BatchMerge');

                if (remainingFlexQueueJobs-- <= 0) {
                    break;
                }

                // NPSP - Not supported - throws error in Future call.  Check back if Salesforce fixes issue
                // https://github.com/SalesforceFoundation/Cumulus/issues/4178                

                // Schedule Batch Job in Flex Queue (can schedule up to 100 jobs)
                C501_MassMerge_Batch batchMergeRequest = new C501_MassMerge_Batch();
                batchMergeRequest.query = 'SELECT Id, Account__c, Merge_Target_Account__c FROM C501_Account_Merge__c WHERE Id = \'' + mergeCandidate.Id + '\'';
                batchMergeRequest.AccountMerge = true;
                Database.executeBatch(batchMergeRequest);

                numberObjectMerges++; 
            }
            else {

                C501_MassMerge_SharedCode.OutputDebugLogText(LoggingLevel.DEBUG, '***** ' + String.valueOf(this).substring(0,String.valueOf(this).indexOf(':')) + '::ProcessObjectMerges ObjectsMerge');

                ObjectsMerge( 
                    new Account(
                        Id = mergeCandidate.Account__c
                    ),
                    new List<Account> { new Account(
                        Id = mergeCandidate.Merge_Target_Account__c
                    )
                } );

                // Limit 1 non-batch merge at a time to avoid any governor limit issues
                numberObjectMerges++;

                break;
            }
        }

        if (!deleteAccountMerges.isEmpty()) {
           C501_MassMerge_SharedCode.OutputDebugLogText(LoggingLevel.DEBUG, '***** ' + String.valueOf(this).substring(0,String.valueOf(this).indexOf(':')) + '::ProcessObjectMerges Remove Invalid Account Merges: ' + deleteAccountMerges);
            Database.delete(deleteAccountMerges, false);
        }

        return numberObjectMerges;
    }

    private void BackfillBlankFields(SObject updateRecord, Set<String> blankFields, List<String> availableFieldValues) {

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

    public void ObjectsFieldMerge(Account masterObject, List<Account> duplicateObjects) {

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

            // Only include the fields which are updateable and accessible and not hidden, unique, or externalid
            if (fResult.isUpdateable() && fResult.isAccessible() && !C501_MassMerge_SharedCode.isNPSPHiddenField(fieldname)) {

                // Special check issue related to Gender__c vs c501mm__Gender__c appears to be same field - ToDo: Remove Gender__c from this managed package
                if (fieldName.toLowerCase().contains('gender__c')) {
                    //C501_MassMerge_SharedCode.OutputDebugLogText(LoggingLevel.DEBUG, '***** C501_MassMerge_SharedCode::ObjectsFieldMerge - skip duplicate field: ' + fieldName + ' strSoql: ' + strSoql);
                    continue;
                }

                // If the field is type lookup, then ignore in the merge fields because is handled during the object merge
                string relName = fResult.getRelationshipName();
                if (string.isBlank(relName)) {
                    strSoql += (fieldName + ',');
                } else {
                    //C501_MassMerge_SharedCode.OutputDebugLogText(LoggingLevel.DEBUG, '***** C501_MassMerge_SharedCode::ObjectsFieldMerge - skip relationship field: ' + fieldName + ' relName: ' + relName + ' fResult: ' + fResult);
                    continue;
                }

                fieldMap.put(fieldName, fResult);
            }
            else {
                //C501_MassMerge_SharedCode.OutputDebugLogText(LoggingLevel.DEBUG, '***** C501_MassMerge_SharedCode::ObjectsFieldMerge - skip field based on fResult: ' + fieldName + ' fResult: ' + fResult);
            }
        }

        strSoql += ' Id from Account where Id IN ' + C501_MassMerge_SharedCode.GetSafeSoqlList(mergeCandidates);
        strSoql += ' LIMIT ' + String.valueOf(C501_MassMerge_SharedCode.GetMaximumLimitSize(1));
        
        List<Account> objectList;
        try {
            objectList = Database.query(strSoql); // Query the records

            // Ensure we got back the same number of records as expected. In case any record got deleted/moved since last search.
            if (objectList == null || objectList.size() != mergeCandidates.size()) {

                C501_MassMerge_SharedCode.OutputDebugLogText(LoggingLevel.ERROR, '***** C501_MassMerge_SharedCode::ObjectsFieldMerge Query failed to return expected number of objects');

                return;
            }

            List<C501_Merge_Account_History__c> insertMergeHistories = new List<C501_Merge_Account_History__c>();

            // Merge fields into master
            SObject masterRecord = null;
            Set<String> masterUsedEmailValues = new Set<String>();
            Set<String> masterBlankEmailFields = new Set<String>();
            List<String> availableEmailValues = new List<String>();
            Set<String> masterUsedPhoneValues = new Set<String>();
            Set<String> masterBlankPhoneFields = new Set<String>();
            List<String> availablePhoneValues = new List<String>();
            Set<String> addressTypesSkip = new Set<String>();

            // Assign Master Record
            for (SObject objectItem :objectList) {

                // Check to assign master object
                if (masterObject.Id == objectItem.get('Id')) {

                    masterRecord = objectItem;

                    // Check Address
                    //
                    for (String addressType :C501_MassMerge_SharedCode_Object.AddressTypesAvailable) {
                        for (String addressField :C501_MassMerge_SharedCode_Object.AddressFieldsAvailable) {

                            C501_MassMerge_SharedCode.OutputDebugLogText(LoggingLevel.DEBUG, '***** C501_MassMerge_SharedCode::ObjectsFieldMerge - check address field: ' + addressType + addressField);

                            try {
                                if (masterRecord.get(addressType + addressField) != null &&
                                    !String.isBlank(String.valueOf(masterRecord.get(addressType + addressField)))) {

                                    C501_MassMerge_SharedCode.OutputDebugLogText(LoggingLevel.DEBUG, '***** C501_MassMerge_SharedCode::ObjectsFieldMerge - skip address field: ' + addressType + addressField);

                                    addressTypesSkip.add(addressType);
                                    break;
                                }
                            }
                            catch (Exception e) {
                                if (!e.getMessage().contains('Invalid field')) {
                                    C501_MassMerge_SharedCode.OutputDebugLogText(LoggingLevel.ERROR, '***** C501_MassMerge_SharedCode::ObjectsFieldMerge check address field exception: ' + String.valueOf(e.getMessage()));
                                }
                            }
                        }
                    }

                    break;
                }
            }

            C501_MassMerge_SharedCode.OutputDebugLogText(LoggingLevel.DEBUG, '***** C501_MassMerge_SharedCode::ObjectsFieldMerge - addressTypesSkip: ' + addressTypesSkip);

            if (masterRecord == null) {
                C501_MassMerge_SharedCode.OutputDebugLogText(LoggingLevel.ERROR, '***** C501_MassMerge_SharedCode::ObjectsFieldMerge Unable to Query Master Object -  masterObject: ' + masterObject);
                throw new C501_MassMerge_SharedCode_AccountException('Account Merge Unable to find Master Object - ' + String.valueOf(masterObject));
            }

            for (SObject objectItem :objectList) {

                // Check to assign master object
                if (masterRecord.Id == objectItem.get('Id')) {
                    continue;
                }

                for (String fieldName :fieldMap.keySet()) {

                    Boolean skipMasterFieldUpdate = false;
                    String masterFieldValue = String.valueOf(masterRecord.get(fieldName));
                    String duplicateFieldValue = String.valueOf(objectItem.get(fieldName));

                    C501_MassMerge_SharedCode.OutputDebugLogText(LoggingLevel.DEBUG, '***** C501_MassMerge_SharedCode::ObjectsFieldMerge - fieldName: ' + fieldName + ' masterFieldValue: ' + masterFieldValue + ' duplicateFieldValue: ' + duplicateFieldValue);

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
                                C501_MassMerge_SharedCode.OutputDebugLogText(LoggingLevel.DEBUG, '***** C501_MassMerge_SharedCode::ObjectsFieldMerge - skip address field: ' + fieldName);
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

                            try{
                                masterRecord.put(fieldName, objectItem.get(fieldName));
                            } catch (Exception e) {
                                C501_MassMerge_SharedCode.OutputDebugLogText(LoggingLevel.ERROR, '***** C501_MassMerge_SharedCode::ObjectsFieldMerge Update Merge Standard Field - fieldName: ' + fieldName + ' Exception: ' + String.valueOf(e.getMessage()));
                            }
                        }
                        else if (!String.isBlank(masterFieldValue) && !String.isBlank(duplicateFieldValue) && masterFieldValue <> duplicateFieldValue) {
                            writeHistoryRecord = true;
                        }
                    }

                    if (writeHistoryRecord) {
                        insertMergeHistories.add(new C501_Merge_Account_History__c(
                            Account__c = masterRecord.Id,
                            Merge_Value__c = String.isBlank(masterFieldValue) ? '(blank)' : masterFieldValue,
                            Ignore_Value__c = String.isBlank(duplicateFieldValue) ? '(blank)' : duplicateFieldValue,
                            Field__c =  fieldName
                        ));
                    }
                }
            }

            // Check to backfill emails
            //
            this.BackfillBlankFields(masterRecord, masterBlankEmailFields, availableEmailValues);

            // Check to backfill phones
            //
            this.BackfillBlankFields(masterRecord, masterBlankPhoneFields, availablePhoneValues);

            Database.SaveResult updateResult = Database.update((Account)masterRecord, false);
            if (!updateResult.isSuccess()) {
                for (Database.Error resultError : updateResult.getErrors()) {
                    C501_MassMerge_SharedCode.LogDatabaseError(resultError, String.valueOf(this).substring(0,String.valueOf(this).indexOf(':')) + '::ObjectsFieldMerge');
                }
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
            C501_MassMerge_SharedCode.OutputDebugLogText(LoggingLevel.ERROR, '***** C501_MassMerge_SharedCode::ObjectsFieldMerge Update fields exception: ' + String.valueOf(e.getMessage()));
        }

        C501_MassMerge_SharedCode.OutputDebugLogFlush();
    }

    public void ObjectsMerge(Account masterObject, List<Account> duplicateObjects) {

        if (masterObject == null || duplicateObjects == null || duplicateObjects.isEmpty()) {
            return;
        }

        C501_MassMerge_SharedCode_Object.ObjectsMerge_Account(masterObject, duplicateObjects);
 
        // Merge fields into master
        ObjectsFieldMerge(masterObject, duplicateObjects);

        // Merge Accounts into master
        Database.MergeResult[] objectResults = Database.merge(masterObject, duplicateObjects, false);
        for(Database.MergeResult res : objectResults) {
            if (!res.isSuccess()) {
                for (Database.Error err : res.getErrors()) {
                    C501_MassMerge_SharedCode.OutputDebugLogText(LoggingLevel.ERROR, '***** C501_MassMerge_SharedCode::ObjectsMerge Account Merge Exception: ' + err.getMessage());
                }

                throw new C501_MassMerge_SharedCode_AccountException('Account Merge Attempt - ' + String.valueOf(res.getErrors()));
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
   }
}