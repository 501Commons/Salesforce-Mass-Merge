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
// GENERIC CLASS Helper - C501_MassMerge_SharedCode_[Object].cls are generic classes
//  This is a helper class for method that are unique and not able to support in the generic class
//  Future consideration - use virtual class method override?
//

public without sharing class C501_MassMerge_SharedCode_Object { 

    // Available Fields for merging
    //
    public static Set<String> AddressTypesAvailable = new Set<String> {'Mailing', 'Shipping', 'Other', 'Billing'};
    public static Set<String> AddressFieldsAvailable = new Set<String> {'City', 'Country', 'GeocodeAccuracy', 'Latitude', 'Longitude', 'PostalCode', 'State', 'Street'};

    public static Boolean ObjectsFieldMerge_Account_HandlePhone(String fieldName, Schema.DescribeFieldResult fieldDescribeResult) {

        Boolean handlePhoneField = false;
        
        if (fieldName.containsIgnoreCase('Phone') && fieldDescribeResult.getType() == Schema.DisplayType.PHONE) {
            handlePhoneField = true;
        }

        return handlePhoneField;
    }

    public static Boolean ObjectsFieldMerge_Contact_HandlePhone(String fieldName, Schema.DescribeFieldResult fieldDescribeResult) {

        Boolean handlePhoneField = false;

        if (fieldName.containsIgnoreCase('Phone') && fieldDescribeResult.getType() == Schema.DisplayType.PHONE) {

            // NPSP? then ignore Phone field because is automatically updated based on npe01__PreferredPhone__c picklist
            //
            if (C501_MassMerge_SharedCode.IsNPSPInstalled && fieldName.equalsIgnoreCase('Phone')) {
                C501_MassMerge_SharedCode.OutputDebugLogText(true, LoggingLevel.DEBUG, '***** C501_MassMerge_SharedCode::ObjectsFieldMerge_Contact_HandlePhone Phone Field Skip since NPSP installed');
            }
            else {
                handlePhoneField = true;
            }
        }

        return handlePhoneField;
    }

    public static Boolean ObjectsFieldMerge_Account_HandleEmail(String fieldName, Schema.DescribeFieldResult fieldDescribeResult) {

        Boolean handleEmailField = false;
        
        if (fieldName.containsIgnoreCase('Email') && fieldDescribeResult.getType() == Schema.DisplayType.EMAIL) {
            handleEmailField = true;
        }

        return handleEmailField;
    }

    public static Boolean ObjectsFieldMerge_Contact_HandleEmail(String fieldName, Schema.DescribeFieldResult fieldDescribeResult) {

        Boolean handleEmailField = false;

        if (fieldName.containsIgnoreCase('Email') && fieldDescribeResult.getType() == Schema.DisplayType.EMAIL) {

            // NPSP? then ignore Email field because that is populated automatically based on npe01__Preferred_Email__c picklist
            if (C501_MassMerge_SharedCode.IsNPSPInstalled && fieldName.equalsIgnoreCase('Email')) {
                C501_MassMerge_SharedCode.OutputDebugLogText(true, LoggingLevel.DEBUG, '***** C501_MassMerge_SharedCode::ObjectsFieldMerge_Contact_HandleEmail Email Field Skip since NPSP installed');
            }
            else {
                handleEmailField = true;
            }
        }

        return handleEmailField;
    }

    public static void ObjectsMerge_Account(Account masterAccount, List<Account> duplicateAccounts) {
    }

    public static void ObjectsMerge_Contact(Contact masterContact, List<Contact> duplicateContacts) {

        if (masterContact == null || duplicateContacts == null || duplicateContacts.isEmpty()) {
            return;
        }

        // Merge contacts into master
        String mergeMetricName = 'User Merge Rule Account No Contacts';
        String strSoql = C501_MassMerge_SharedCode.GetC501MergeMetricQuery();
        strSoql += ' AND Name = \'' + mergeMetricName + '\'';

        System.debug(LoggingLevel.DEBUG, '*****C501_MassMerge_SharedCode::ObjectsMerge_Contact strSoql: ' + strSoql);
        List<C501_Merge_Metric__c> mergeMetrics = Database.Query(strSoql);

        if (mergeMetrics.isEmpty()) {

            mergeMetrics.add( new C501_Merge_Metric__c (
                Name = (Test.isRunningTest() ? ('TESTDATA ' + mergeMetricName) : mergeMetricName),
                Confidence__c = 90.0,
                AutoMerge_Percentage__c = 90.0,
                AutoMerge_Objects__c = 'Account',
                Source_Report__c = 'User_Merge_Rule_Account_No_Contacts',
                Source_Object__c = 'Account',
                Source_Object_Account_Id__c = 'Id',
                Source_Object_Contact_Id__c = 'Id',
                Description__c = mergeMetricName,
                Unique_Id__c = 'User_Merge_Rule_Account_No_Contacts'
            ));

            Database.SaveResult[] insertResults = Database.insert(mergeMetrics, false);
            for (Database.SaveResult result : insertResults) {
                if (!result.isSuccess()) {
                    for(Database.Error resultError : result.getErrors()) {
                        C501_MassMerge_SharedCode.LogDatabaseError(resultError, 'C501_MassMerge_SharedCode::ObjectsMerge_Contact');
                    }                        
                }
            }
        }

        Set<Id> contactIds = new Set<Id> { masterContact.Id };
        for (Contact contact :duplicateContacts) {
            contactIds.add(contact.Id);
        }

        Map<Id, String> contactAccounts = new Map<Id, String>();
        for (Contact contact : [
            SELECT Id, AccountId FROM Contact WHERE Id in :contactIds]) {

            // Contact could have null account if not NPSP household model
            if (contact.AccountId <> null) {
                contactAccounts.put(contact.Id, contact.AccountId);
            }
        }

        if (contactAccounts.isEmpty() || contactAccounts.values().isEmpty()) {
            return;
        }

        Map<Id, Integer> accountMembers = new Map<Id, Integer>();
        for (AggregateResult result : [
            SELECT AccountId, Count(Id) NumberOfMembers FROM Contact WHERE AccountId in :contactAccounts.values() group by AccountId]) {

            String accountId = result.get('AccountId').toString();
            Integer numberOfMembers = Integer.valueOf(result.get('NumberOfMembers'));
            accountMembers.put(accountId, numberOfMembers);
        }

        // Check to add an account merge request - can not Account merge here because might be in a batch call which is not supported for Account merging
        String masterAccountId = contactAccounts.get(masterContact.Id);
        Map<String, C501_Account_Merge__c> accountMergeInserts = new Map<String, C501_Account_Merge__c>();
        for (String accountId :contactAccounts.values()) {

            if (accountId <> masterAccountId && accountMembers.get(accountId) <= 1) {

                System.debug(LoggingLevel.DEBUG, '*****C501_MassMerge_SharedCode::ObjectsMerge_Contact queue empty accountId: ' + accountId + ' for merge into masterAccountId: ' + masterAccountId);

                // Create an account merge because the duplicate account now has no children
                String maxId = (masterAccountId > accountId) ? masterAccountId : accountId;
                String minId = (masterAccountId <= accountId) ? masterAccountId : accountId;
                String uniqueId = mergeMetricName + maxId + minId;
                accountMergeInserts.put(uniqueId, new C501_Account_Merge__c(
                    Unique_Id__c = uniqueId,
                    Merge_Confidence__c = mergeMetrics[0].Confidence__c,
                    Account__c = masterAccountId,
                    Merge_Target_Account__c = accountId,
                    Merge_Metric__c = mergeMetrics[0].Id
                ));
            }
        }

        // Delete any account merges for accounts where contact was merged
        strSoql = C501_MassMerge_SharedCode.GetC501AccountMergeQuery();
        strSoql += ' AND (Account__c = null OR Merge_Target_Account__c = null OR Account__c in ' + C501_MassMerge_SharedCode.GetSafeSoqlList(contactAccounts.values()) + ' OR Merge_Target_Account__c in ' + C501_MassMerge_SharedCode.GetSafeSoqlList(contactAccounts.values()) + ')';

        System.debug(LoggingLevel.DEBUG, '*****C501_MassMerge_SharedCode::ObjectsMerge_Contact strSoql: ' + strSoql);
        List<C501_Account_Merge__c> deleteAccountMerges = Database.query(strSoql);

        if (!deleteAccountMerges.isEmpty()) {
           System.debug(LoggingLevel.DEBUG, '*****C501_MassMerge_SharedCode::ObjectsMerge_Contact Delete Account Merges : ' + deleteAccountMerges);
            delete deleteAccountMerges;
        }

        // Queue empty household merge
        if (!accountMergeInserts.isEmpty() && !accountMergeInserts.values().isEmpty()) {
            Database.SaveResult[] insertResults = Database.insert(accountMergeInserts.values(), false);
            for (Database.SaveResult result : insertResults) {
                if (!result.isSuccess()) {
                    for(Database.Error resultError : result.getErrors()) {
                        C501_MassMerge_SharedCode.LogDatabaseError(resultError, 'C501_MassMerge_SharedCode::ObjectsMerge_Contact');
                    }                        
                }
            }
        }
    }
}