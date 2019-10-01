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

public without sharing class C501_MassMerge_SharedCode_Contact { 

    public class C501_MassMerge_SharedCode_ContactException extends Exception {}

    // the list of Contacts
    public List<SelectOption> ListObjectsWithConfidence(String mergeRule, Decimal fromMergeConfidence, Decimal toMergeConfidence) {

        System.debug(LoggingLevel.DEBUG, '***** C501_MassMerge_SharedCode_Contact::ListObjectsWithConfidence - mergeRule: ' + mergeRule 
            + ' fromMergeConfidence: ' + fromMergeConfidence.toPlainString()
            + ' toMergeConfidence: ' + toMergeConfidence.toPlainString());

        Set<String> setSOUnique = new Set<String>();
        List<SelectOption> listSO = new List<SelectOption>();
        listSO.add(new SelectOption('', ''));

        if (String.isBlank(mergeRule)) {
            for (AggregateResult result : [
                SELECT Contact__c, Contact__r.Name
                FROM C501_Contact_Merge__c
                WHERE Merge_Confidence__c >= :fromMergeConfidence
                    and Merge_Confidence__c <= :toMergeConfidence
                    and Merge_Action__c = null 
                    and isDeleted = false
                GROUP BY Contact__c, Contact__r.Name
                LIMIT 999]) {

                    // Attempt to get based on Namespace
                    String objectFieldValue;
                    try {
                        objectFieldValue = result.get(C501_MassMerge_SharedCode.CurrentNamespace + 'Contact__c').toString();
                    }
                    catch (System.SObjectException objectException) {
                        objectFieldValue = result.get('Contact__c').toString();
                    }

                    listSO.add(new SelectOption(objectFieldValue,
                        result.get('Name').toString()));
            }
        }
        else {
            for (AggregateResult result : [
                SELECT Contact__c, Contact__r.Name
                FROM C501_Contact_Merge__c
                WHERE Merge_Confidence__c >= :fromMergeConfidence
                    and Merge_Confidence__c <= :toMergeConfidence
                    and Merge_Action__c = null
                    and isDeleted = false
                    and Merge_Metric__r.Name = :mergeRule
                GROUP BY Contact__c, Contact__r.Name
                LIMIT 999]) {

                    // Attempt to get based on Namespace
                    String objectFieldValue;
                    try {
                        objectFieldValue = result.get(C501_MassMerge_SharedCode.CurrentNamespace + 'Contact__c').toString();
                    }
                    catch (System.SObjectException objectException) {
                        objectFieldValue = result.get('Contact__c').toString();
                    }

                    listSO.add(new SelectOption(objectFieldValue,
                        result.get('Name').toString()));
            }
        }

        List<SelectOption> listSOSorted = new List<SelectOption>();
        listSOSorted = C501_MassMerge_SharedCode.SelectOptionSortByLabel(listSO);

        return listSOSorted;
    }

    public void InsertObjectMergeCandidates(C501_Merge_Metric__c mergeMetric, List<C501_Contact_Merge__c> mergeObjects) {

        if (mergeObjects == null || mergeObjects.isEmpty()) {
            return;
        }

        //
		// Perform all DML (Data Manipulation Language) calls (insert, update, merge, delete, restore) outside of loop to avoid Salesforce Governor Limits: https://developer.salesforce.com/docs/atlas.en-us.apexcode.meta/apexcode/apex_gov_limits.htm
        //

        String mergeMetricName = mergeMetric.Name;

        Set<String> mergeObjectsExisting = new Set<String>();
        List<C501_Contact_Merge__c> deleteContactMerges = new List<C501_Contact_Merge__c>();

        Boolean isSandbox = false;
        List<Organization> organizations = [SELECT IsSandbox FROM Organization LIMIT 1];
        if (!organizations.isEmpty()) {
            isSandbox = Boolean.valueOf(organizations[0].IsSandbox);
        }

        String strSoql = C501_MassMerge_SharedCode.GetC501ContactMergeQuery();
        strSoql += ' AND Merge_Metric__r.Name = \'' + mergeMetricName + '\'';

        System.debug(LoggingLevel.DEBUG, '***** ' + String.valueOf(this).substring(0,String.valueOf(this).indexOf(':')) + '::InsertObjectMergeCandidates strSoql: ' + strSoql);

        for (C501_Contact_Merge__c mergeObject :Database.query(strSoql)) {

            if (mergeObject.Contact__c == null || mergeObject.Merge_Target_Contact__c == null) {

                deleteContactMerges.add(mergeObject);
                continue;
            }

            if (mergeObject.Contact__r.IsDeleted) {

                deleteContactMerges.add(mergeObject);
                continue;
            }

            if (mergeObject.Merge_Target_Contact__r.IsDeleted) {

                deleteContactMerges.add(mergeObject);
                continue;
            }

            mergeObjectsExisting.add(mergeObject.Unique_Id__c);
        }

        if (!deleteContactMerges.isEmpty()) {
           System.debug(LoggingLevel.DEBUG, '***** ' + String.valueOf(this).substring(0,String.valueOf(this).indexOf(':')) + '::InsertObjectMergeCandidates Remove Invalid Contact Merges: ' + deleteContactMerges);
            delete deleteContactMerges;
        }

		if (mergeObjects <> null && !mergeObjects.isEmpty()) {

            List<C501_Contact_Merge__c> mergeObjectsInsert = new List<C501_Contact_Merge__c>();
            for (C501_Contact_Merge__c mergeObject :mergeObjects) {

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

                            String statusCode = String.valueOf(resultError.getStatusCode());

                            // Sandbox ignore the INVALID_CROSS_REFERENCE_KEY expection because there is partial data in Sandbox
                            if (isSandbox && statusCode.contains('INVALID_CROSS_REFERENCE_KEY')) {
                                continue;
                            }

                            // Ignore DUPLICATE_VALUE because just trying to insert and not checking if already there to reduce SOQL calls - let the DB block
                            if (statusCode.contains('DUPLICATE_VALUE')) {
                                continue;
                            }

                            System.debug(LoggingLevel.ERROR, '***** C501_MassMerge_SharedCode_Contact::InsertObjectMergeCandidates Database.SaveResult Error');
                            System.debug(LoggingLevel.ERROR, statusCode + ': ' + resultError.getMessage());
                            System.debug(LoggingLevel.ERROR, 'Fields that affected this error: ' + resultError.getFields());
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

        List<C501_Contact_Merge__c> deleteContactMerges = new List<C501_Contact_Merge__c>();
        List<C501_Contact_Merge__c> pendingMerges = new List<C501_Contact_Merge__c>();
        List<C501_Contact_Merge__c> pendingAutoMerges = new List<C501_Contact_Merge__c>();

        Map<Id, Id> ContactHouseholdMap = new Map<Id, Id>();

        String strSoql = C501_MassMerge_SharedCode.GetC501ContactMergeQuery();
        strSoql += ' AND Merge_Metric__r.Name = :mergeMetricName AND Merge_Action__c <> \'Ignore\'';

        System.debug(LoggingLevel.DEBUG, '***** ' + String.valueOf(this).substring(0,String.valueOf(this).indexOf(':')) + '::ProcessObjectMergeCandidates strSoql: ' + strSoql);

        for(C501_Contact_Merge__c mergeObject : Database.Query(strSoql)) {

            if (mergeObject.Contact__c == null || mergeObject.Merge_Target_Contact__c == null ||
                mergeObject.Contact__r.IsDeleted || mergeObject.Merge_Target_Contact__r.IsDeleted) {

                deleteContactMerges.add(mergeObject);
                continue;
            }

            ContactHouseholdMap.put(mergeObject.Contact__c, C501_MassMerge_SharedCode.GetMergeContactHouseholdId(mergeObject));
            ContactHouseholdMap.put(mergeObject.Merge_Target_Contact__c, C501_MassMerge_SharedCode.GetMergeTargetContactHouseholdId(mergeObject));

            if (mergeObject.Merge_Action__c == 'Master') {
                pendingMerges.add(mergeObject);
            }
            else if (String.isBlank(mergeObject.Merge_Action__c)) {
                pendingAutoMerges.add(mergeObject);
            }
        }

        if (!deleteContactMerges.isEmpty()) {
           System.debug(LoggingLevel.DEBUG, '***** ' + String.valueOf(this).substring(0,String.valueOf(this).indexOf(':')) + '::ProcessObjectMergeCandidates Remove Invalid Contact Merges: ' + deleteContactMerges);
            delete deleteContactMerges;
        }

        // Check for batch queued merges
        if (!pendingMerges.isEmpty()) {

            ProcessObjectMerges(new List<C501_Merge_Metric__c> {mergeMetric}, pendingMerges, false);
            return pendingMerges.size();
        }

        // Check if AutoMerge Enabled
        if (String.isBlank(mergeMetric.AutoMerge_Objects__c)) {
            return 0;
        }

        return C501_MassMerge_SharedCode.ProcessObjectMergeCandidates_Contact(mergeMetric, pendingAutoMerges, ContactHouseholdMap);
    }

    // Legacy Method for Managed Package
    public void ProcessObjectMerges(List<C501_Contact_Merge__c> objectMerges, Boolean batchMerge)
    {
        this.ProcessObjectMerges(new List<C501_Merge_Metric__c>(), objectMerges, batchMerge);
    }

    public void ProcessObjectMerges(List<C501_Merge_Metric__c> mergeMetrics, List<C501_Contact_Merge__c> objectMerges, Boolean batchMerge)
    {

        // Jobs with Status = 'Holding' applies to batch jobs in the Apex flex queue (max allowed 100)
        //  https://developer.salesforce.com/docs/atlas.en-us.object_reference.meta/api/sforce_api_objects_asyncapexjob.htm
        List<AsyncApexJob> flexQueueJobs = [SELECT Id FROM AsyncApexJob WHERE Status = 'Holding'];
        System.debug(LoggingLevel.DEBUG, '***** C501_MassMerge_SharedCode_Contact::ProcessObjectMerges - flexQueueJobs ' + flexQueueJobs);
        System.debug(LoggingLevel.DEBUG, '***** C501_MassMerge_SharedCode_Contact::ProcessObjectMerges - QueuableJobs ' + Limits.getQueueableJobs() + ' of ' + Limits.getLimitQueueableJobs());
        Integer remainingFlexQueueJobs = Limits.getLimitQueueableJobs() - flexQueueJobs.size();

        Map<String, Decimal> mergeMetricMap = new Map<String, Decimal>();
        for (C501_Merge_Metric__c mergeMetric :mergeMetrics) {
            mergeMetricMap.put(mergeMetric.Id, mergeMetric.AutoMerge_Percentage__c);
        }

        Set<Id> objectsProcessed = new Set<Id>();
        List<C501_Contact_Merge__c> deleteContactMerges = new List<C501_Contact_Merge__c>();
        for (C501_Contact_Merge__c mergeCandidate : objectMerges) {

            // Check Contact and Target Contact valid
            if (String.isBlank(mergeCandidate.Contact__c) || String.isBlank(mergeCandidate.Merge_Target_Contact__c)) {

                deleteContactMerges.add(mergeCandidate);
                continue;
            }

            // Check Contact and Target Contact same Id
            if (mergeCandidate.Contact__c == mergeCandidate.Merge_Target_Contact__c) {

                deleteContactMerges.add(mergeCandidate);
                continue;
            }

            // Check whether Contact or Target Contact Id merge already attempted
            if (objectsProcessed.contains(mergeCandidate.Contact__c) || objectsProcessed.contains(mergeCandidate.Merge_Target_Contact__c)) {

                deleteContactMerges.add(mergeCandidate);
                continue;
            }

            // Check Automerge Percentage
            if (!mergeMetricMap.containsKey(mergeCandidate.Merge_Metric__c)) {
                String strSoql = C501_MassMerge_SharedCode.GetC501MergeMetricQuery();

                System.debug(LoggingLevel.DEBUG, '***** ' + String.valueOf(this).substring(0,String.valueOf(this).indexOf(':')) + '::ProcessObjectMerges strSoql: ' + strSoql);

                mergeMetrics = Database.Query(strSoql);

                for (C501_Merge_Metric__c mergeMetric :mergeMetrics) {
                    mergeMetricMap.put(mergeMetric.Id, mergeMetric.AutoMerge_Percentage__c);
                }

                if (!mergeMetricMap.containsKey(mergeCandidate.Merge_Metric__c)) {
                    throw new C501_MassMerge_SharedCode_ContactException('Contact Merge Data Error - Invalid Merge Metric: ' + mergeCandidate.Merge_Metric__c + ' on Contact Merge Id: ' + mergeCandidate.Id);
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

            objectsProcessed.add(mergeCandidate.Contact__c);
            objectsProcessed.add(mergeCandidate.Merge_Target_Contact__c);

            if (batchMerge) {

                System.debug(LoggingLevel.DEBUG, '***** ' + String.valueOf(this).substring(0,String.valueOf(this).indexOf(':')) + '::ProcessObjectMerges BatchMerge');

                if (remainingFlexQueueJobs-- <= 0) {
                    break;
                }

                // NPSP - Not supported - throws error in Future call.  Check back if Salesforce fixes issue
                // https://github.com/SalesforceFoundation/Cumulus/issues/4178                

                // Schedule Batch Job in Flex Queue (can schedule up to 100 jobs)
                List<String> objectsMergeIds = new List<String> { mergeCandidate.Contact__c, mergeCandidate.Merge_Target_Contact__c };
                C501_MassMerge_Batch batchMergeRequest = new C501_MassMerge_Batch();
                batchMergeRequest.query = 'SELECT Id FROM Contact WHERE Id in (\'' + String.join(objectsMergeIds, '\',\'') + '\') ORDER BY CreatedDate DESC';
                batchMergeRequest.ContactMerge = true;
                Database.executeBatch(batchMergeRequest);
            }
            else {

                System.debug(LoggingLevel.DEBUG, '***** ' + String.valueOf(this).substring(0,String.valueOf(this).indexOf(':')) + '::ProcessObjectMerges ObjectsMerge');

                ObjectsMerge( 
                    new Contact(
                        Id = mergeCandidate.Contact__c
                    ),
                    new List<Contact> { new Contact(
                        Id = mergeCandidate.Merge_Target_Contact__c
                    )
                } );

                // Limit 1 non-batch merge at a time to avoid any governor limit issues
                break;
            }
        }

        if (!deleteContactMerges.isEmpty()) {
           System.debug(LoggingLevel.DEBUG, '***** ' + String.valueOf(this).substring(0,String.valueOf(this).indexOf(':')) + '::ProcessObjectMerges Remove Invalid Contact Merges: ' + deleteContactMerges);
            delete deleteContactMerges;
        }
    }

    public void ObjectsFieldMerge(Contact masterObject, List<Contact> duplicateObjects) {

        Set<Id> mergeCandidates = new Set<Id> { masterObject.Id };
        for (Contact objectItem :duplicateObjects) {
            mergeCandidates.add(objectItem.Id);
        }

        C501_MassMerge_SharedCode.OutputDebugLogClear();

        Map<String, Schema.SObjectField> objectFields = Schema.SObjectType.Contact.fields.getMap();
        Map<String, Schema.DescribeFieldResult> standardFieldMap = new Map<String, Schema.DescribeFieldResult>();
        Map<String, Schema.DescribeFieldResult> customFieldMap = new Map<String, Schema.DescribeFieldResult>();

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
            if (fResult.isPermissionable() && fResult.isUpdateable() && fResult.isAccessible() && !C501_MassMerge_SharedCode.isNPSPHiddenField(fieldname)) {

                // Special check issue related to Gender__c vs c501mm__Gender__c appears to be same field - ToDo: Remove Gender__c from this managed package
                if (strSoql.toLowerCase().contains('gender__c')) {
                    C501_MassMerge_SharedCode.OutputDebugLogText(true, LoggingLevel.DEBUG, '***** C501_MassMerge_SharedCode::ObjectsFieldMerge - skip field: ' + fieldName + ' strSoql: ' + strSoql);
                    continue;
                }

                // If the field is type lookup, then ignore in the merge fields because is handled during the object merge
                string relName = fResult.getRelationshipName();
                if (string.isBlank(relName)) {
                    strSoql += (fieldName + ',');
                } else {
                    continue;
                }

                if(fResult.isCustom()) {
                    customFieldMap.put(fieldName, fResult);
                } else {
                    standardFieldMap.put(fieldName, fResult);
                }
            }
        }

        strSoql += ' Id from Contact where Id IN ' + C501_MassMerge_SharedCode.GetSafeSoqlList(mergeCandidates);
        
        List<Contact> objectList;
        try {
            objectList = Database.query(strSoql); // Query the records

            // Ensure we got back the same number of records as expected. In case any record got deleted/moved since last search.
            if (objectList == null || objectList.size() != mergeCandidates.size()) {

                C501_MassMerge_SharedCode.OutputDebugLogText(true, LoggingLevel.ERROR, '***** C501_MassMerge_SharedCode::ObjectsFieldMerge Query failed to return expected number of objects');

                return;
            }

            List<C501_Merge_Contact_History__c> insertMergeHistories = new List<C501_Merge_Contact_History__c>();

            // Merge fields into master
            SObject masterRecord = null;
            for (SObject objectItem :objectList) {

                // Check to assign master object
                if (masterRecord == null) {
                    masterRecord = objectItem;
                    continue;
                }

                for (String fieldName :standardFieldMap.keySet()) {

                    if ( (masterRecord.get(fieldName) == null || String.isBlank(String.valueOf(masterRecord.get(fieldName)))) &&
                        objectItem.get(fieldName) <> null && !String.isBlank(String.valueOf(objectItem.get(fieldName)))) {

                        try{
                            masterRecord.put(fieldName, objectItem.get(fieldName));
                        } catch (Exception e) {
                            C501_MassMerge_SharedCode.OutputDebugLogText(true, LoggingLevel.ERROR, '***** C501_MassMerge_SharedCode::ObjectsFieldMerge Update Merge Standard Field - fieldName: ' + fieldName + ' Exception: ' + String.valueOf(e.getMessage()));
                        }
                    }
                    else if (objectItem.get(fieldName) <> null && !String.isBlank(String.valueOf(objectItem.get(fieldName))) &&
                        String.valueOf(masterRecord.get(fieldName)) <> String.valueOf(objectItem.get(fieldName))) {

                        insertMergeHistories.add(new C501_Merge_Contact_History__c(
                            Contact__c = masterRecord.Id,
                            Merge_Value__c = String.valueOf(masterRecord.get(fieldName)),
                            Ignore_Value__c = String.valueOf(objectItem.get(fieldName)),
                            Field__c =  fieldName
                        ));
                    }
                }

                for (String fieldName :customFieldMap.keySet()) {

                    if ( (masterRecord.get(fieldName) == null || String.isBlank(String.valueOf(masterRecord.get(fieldName)))) &&
                        objectItem.get(fieldName) <> null && !String.isBlank(String.valueOf(objectItem.get(fieldName)))) {

                        try{
                            masterRecord.put(fieldName, objectItem.get(fieldName));
                        } catch (Exception e) {
                            C501_MassMerge_SharedCode.OutputDebugLogText(true, LoggingLevel.ERROR, '***** C501_MassMerge_SharedCode::ObjectsFieldMerge Update Merge Custom Field - fieldName: ' + fieldName + ' Exception: ' + String.valueOf(e.getMessage()));
                        }
                    }
                    else if (objectItem.get(fieldName) <> null && !String.isBlank(String.valueOf(objectItem.get(fieldName))) &&
                        String.valueOf(masterRecord.get(fieldName)) <> String.valueOf(objectItem.get(fieldName))) {

                        insertMergeHistories.add(new C501_Merge_Contact_History__c(
                            Contact__c = masterRecord.Id,
                            Merge_Value__c = String.valueOf(masterRecord.get(fieldName)),
                            Ignore_Value__c = String.valueOf(objectItem.get(fieldName)),
                            Field__c =  fieldName
                        ));
                    }
                }
            }

            Database.SaveResult updateResult = Database.update((Contact)masterRecord, false);
            if (!updateResult.isSuccess()) {
                for (Database.Error err : updateResult.getErrors()) {
                    C501_MassMerge_SharedCode.OutputDebugLogText(true, LoggingLevel.ERROR, '***** C501_MassMerge_SharedCode::ObjectsMerge Contact Merge Exception: ' + err.getMessage());
                }
            }
            if (!insertMergeHistories.isEmpty()) {
                Database.SaveResult[] insertResults = Database.insert(insertMergeHistories, false);
                for (Database.SaveResult insertResult :insertResults) {
                    if (!insertResult.isSuccess()) {
                        for (Database.Error err : insertResult.getErrors()) {
                            C501_MassMerge_SharedCode.OutputDebugLogText(true, LoggingLevel.ERROR, '***** C501_MassMerge_SharedCode::ObjectsMerge Contact Merge History Exception: ' + err.getMessage());
                        }
                    }
                }
            }
        } catch (Exception e) {
            C501_MassMerge_SharedCode.OutputDebugLogText(true, LoggingLevel.ERROR, '***** C501_MassMerge_SharedCode::ObjectsFieldMerge update fields exception: ' + String.valueOf(e.getMessage()));
        }

        c501mm.C501_MassMerge_SharedCode.OutputDebugLogFlush();
    }

    public void ObjectsMerge(Contact masterObject, List<Contact> duplicateObjects) {

        if (masterObject == null || duplicateObjects == null || duplicateObjects.isEmpty()) {
            return;
        }

        C501_MassMerge_SharedCode.ObjectsMerge_Contact(masterObject, duplicateObjects);
 
        // Merge fields into master
        ObjectsFieldMerge(masterObject, duplicateObjects);

        // Merge Contacts into master
        Database.MergeResult[] objectResults = Database.merge(masterObject, duplicateObjects, false);
        for(Database.MergeResult res : objectResults) {
            if (!res.isSuccess()) {
                for (Database.Error err : res.getErrors()) {
                    System.debug(LoggingLevel.ERROR, '***** C501_MassMerge_SharedCode::ObjectsMerge Contact Merge Exception: ' + err.getMessage());
                }

                throw new C501_MassMerge_SharedCode_ContactException('Contact Merge Attempt - ' + String.valueOf(res.getErrors()));
            }
        }

        Set<Id> objectIds = new Set<Id> { masterObject.Id };
        for (Contact duplicateObject :duplicateObjects) {
            objectIds.add(duplicateObject.Id);
        }

        String strSoql = C501_MassMerge_SharedCode.GetC501ContactMergeQuery();
        strSoql += ' AND (Contact__c = null OR Merge_Target_Contact__c = null OR Contact__c in ' + C501_MassMerge_SharedCode.GetSafeSoqlList(objectIds) + ' OR Merge_Target_Contact__c in ' + C501_MassMerge_SharedCode.GetSafeSoqlList(objectIds) + ')';

        System.debug(LoggingLevel.DEBUG, '***** ' + String.valueOf(this).substring(0,String.valueOf(this).indexOf(':')) + '::ObjectsMerge strSoql: ' + strSoql);
        
        List<C501_Contact_Merge__c> deleteContactMerges = Database.query(strSoql);

        if (!deleteContactMerges.isEmpty()) {
           System.debug(LoggingLevel.DEBUG, '***** ' + String.valueOf(this).substring(0,String.valueOf(this).indexOf(':')) + '::ProcessObjectMerges Remove Invalid Contact Merges: ' + deleteContactMerges);
            delete deleteContactMerges;
        }
   }
}