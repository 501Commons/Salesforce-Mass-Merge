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

//
// GENERIC CLASS - modify of the C501_MassMerge_SharedCode_[Object].cls classes then copy to the all the others
//
//      1) Make changes to the class
//      2) Copy/Paste entire class to all other C501_MassMerge_SharedCode_[Object].cls classes
//      3) Search and replace all [Object] references with [Target Object]
//
//  This is a workaround for Generic classes since <T> - Templating not supported in Apex
//

@isTest
public class C501_CTRL_MassMergeContacts_TEST {
    
    //==================== TEST METHOD(s) ======================================

    @isTest(SeeAllData='true')
    public static void AllFields_General_DiscoverTest() {

        Test.StartTest();
        C501_MassMerge_SharedCode.TestDiscoverHelper('Contact All General');
        Test.StopTest();
    }

    @isTest(SeeAllData='true')
    public static void AllFields_Group1_DiscoverTest() {

        Test.StartTest();
        C501_MassMerge_SharedCode.TestDiscoverHelper('Contact All Group1');
        Test.StopTest();
    }

    @isTest(SeeAllData='true')
    public static void AllFields_Group2_DiscoverTest() {

        Test.StartTest();
        C501_MassMerge_SharedCode.TestDiscoverHelper('Contact All Group2');
        Test.StopTest();
    }

    @isTest(SeeAllData='true')
    public static void Contact_Address_DiscoverTest() {

        Test.StartTest();
        C501_MassMerge_SharedCode.TestDiscoverHelper('Contact Test');
        Test.StopTest();
    }

    @isTest(SeeAllData='true')
    public static void ProcessObjectMerges_BatchTest() {

        List<C501_Merge_Metric__c> mergeMetrics = C501_MassMerge_SharedCode.TestDiscoverHelper('Contact Test');
        List<Contact> listContacts = [SELECT Id FROM Contact WHERE Name LIKE '%TESTDATA%'];

        C501_Contact_Merge__c objectMerge = new C501_Contact_Merge__c(
                Unique_Id__c = 'TESTDATA Name ' + listContacts[0].Id,
                Merge_Metric__c = mergeMetrics[0].Id,
                Contact__c = listContacts[0].Id,
                Merge_Target_Contact__c = listContacts[1].Id,
                Merge_Confidence__c = 100.0,
                Merge_Action__c = 'Master');

        insert objectMerge;

        Boolean exceptionRaised = false;

        Test.StartTest();

        // Exception Note:  Currently not getting the exception in the test code for Batch scheduled job.  Thinking need to query the batch job status instead to figure out if exception thrown
        //
        try
        {
            C501_MassMerge_SharedCode_Contact c501MassMerge_SharedCode_Contact = new C501_MassMerge_SharedCode_Contact();
            c501MassMerge_SharedCode_Contact.ProcessObjectMerges(mergeMetrics, new List<C501_Contact_Merge__c> { objectMerge });
        }
        catch (C501_MassMerge_SharedCode_Contact.C501_MassMerge_SharedCode_ContactException customException)
        {
            exceptionRaised = true;
        }
        catch (System.DmlException dmlException) {
            exceptionRaised = true;
        }
        catch (Exception generalException) {
            exceptionRaised = true;
        }

        Test.StopTest();

        //System.AssertEquals(true, exceptionRaised, 'Exception should be thrown for Contact merges on Batchable context');
    }

    @isTest(SeeAllData='true')
    public static void ProcessObjectMerges_SyncTest() {

        List<C501_Merge_Metric__c> mergeMetrics = C501_MassMerge_SharedCode.TestDiscoverHelper('Contact Test');
        List<Contact> listContacts = [SELECT Id FROM Contact WHERE Name LIKE '%TESTDATA%'];

        C501_Contact_Merge__c objectMerge = new C501_Contact_Merge__c(
                Unique_Id__c = 'TESTDATA Name ' + listContacts[0].Id,
                Merge_Metric__c = mergeMetrics[0].Id,
                Contact__c = listContacts[0].Id,
                Merge_Target_Contact__c = listContacts[1].Id,
                Merge_Confidence__c = 100.0,
                Merge_Action__c = 'Master');

        insert objectMerge;

        Test.StartTest();

        C501_MassMerge_SharedCode_Contact c501MassMerge_SharedCode_Contact = new C501_MassMerge_SharedCode_Contact();
        c501MassMerge_SharedCode_Contact.ProcessObjectMerges(mergeMetrics, new List<C501_Contact_Merge__c> { objectMerge });

        Test.StopTest();
    }

    @isTest(SeeAllData='true')
    public static void CodeCoverageTests() {

        Test.StartTest();

        List<C501_Merge_Metric__c> mergeMetrics = C501_MassMerge_SharedCode.TestDiscoverHelper('Contact Test');

        Id masterObjectId = [SELECT Id FROM Contact WHERE Name LIKE '%TESTDATA%' ORDER BY Id ASC LIMIT 1].Id;

        C501_MassMerge_SharedCode.OutputDebugLogText(LoggingLevel.DEBUG, '*****CodeCoverageTests - masterObjectId: ' + masterObjectId);

        //point to our VF page
        PageReference p = new PageReference('Page.MassMergeContacts');
        p.getParameters().put('Id', masterObjectId);
        Test.setCurrentPageReference(p);
        
        C501_CTRL_MassMergeContacts ctrl = new C501_CTRL_MassMergeContacts();

        // StopTest will make sure any future, schedulable, and batch calls complete before proceeding
        Test.StopTest();

        System.AssertEquals(masterObjectId, ctrl.objectFilter.Id);
        System.Assert(ctrl.listSOContacts.size() > 1);
        System.Assert(ctrl.listContacts.size() > 0);

        ctrl.objectFilter.Id = null;
        ctrl.ChangeMergeConfidence();
        ctrl.ChangeRule();
        ctrl.ChangeContact();
        ctrl.ResetContacts();
        
        System.Assert(ctrl.listSORules.size() > 0);

        // Check filter by Rule
        ctrl.objectFilter.Id = masterObjectId;
        ctrl.objectFilter.C501_Merge_Rule__c = mergeMetrics[0].Name;
        System.Assert(ctrl.listContacts.size() > 0);

        // Check filter by Confidence
        ctrl.objectFilter.Id = masterObjectId;
        ctrl.objectFilter.C501_Merge_Confidence_From__c = 0.0;
        ctrl.objectFilter.C501_Merge_Confidence_To__c = 100.0;
        System.Assert(ctrl.listContacts.size() > 0);

        // Clear filter
        ctrl.ChangeRule();
        ctrl.objectFilter.Id = masterObjectId;
        System.Assert(ctrl.listContacts.size() > 0);
        System.AssertEquals(0, ctrl.numberOfRecordsDirty);
        
        //ctrl.listContacts.get(0).Amount = 2;
        p.getParameters().put('usersIdDirtied', ctrl.listContacts.get(0).id);
        ctrl.ContactsRecordModified();
        System.AssertEquals(1, ctrl.setContactsIdDirty.size());
        
        Contact objectInstance = ctrl.listContacts.get(ctrl.listContacts.size() - 1);
        p.getParameters().put('usersIdDirtied', null);
        ctrl.ContactsRecordModified();
        System.AssertEquals(1, ctrl.setContactsIdDirty.size());
        System.AssertEquals(1, ctrl.numberOfRecordsDirty);

        // test the save!
        ctrl.listContacts[0].C501_Merge_Action__c = 'Master';
        ctrl.listContacts[ctrl.listContacts.size() - 1].C501_Merge_Action__c = 'Ignore';

        // Contains @Future Calls - proceed with a Test.Stop() to make sure Future is completed
        ctrl.SaveContacts();

        C501_MassMerge_SharedCode.OutputDebugLogText(LoggingLevel.DEBUG, '***** C501_CTRL_MassMergeContacts_TEST::CodeCoverageTests' + ctrl.strSaveResults);
        //System.Assert(ctrl.strSaveResults.startsWith('Error'));
        ctrl.ChangeRule();

        // Contains @Future Calls - proceed with a Test.Stop() to make sure Future is completed
        ctrl.SaveContacts();      

        //System.AssertEquals(0, ctrl.numberOfRecordsDirty);  

        // Clear out household filter
        ctrl.objectFilter.Id = null;

        // Contains @Future Calls - proceed with a Test.Stop() to make sure Future is completed
        ctrl.SaveAndCloseContacts();   
    }

    @isTest(SeeAllData='true')
    public static void testPageMessagesController() {

        C501_MassMerge_SharedCode.TestDiscoverHelper('Contact Test');

        Test.StartTest();

        ApexPages.addMessage(new ApexPages.message(ApexPages.Severity.WARNING,'warning'));
        ApexPages.addMessage(new ApexPages.message(ApexPages.Severity.INFO,'info'));
        ApexPages.addMessage(new ApexPages.message(ApexPages.Severity.FATAL,'fatal'));
        ApexPages.addMessage(new ApexPages.message(ApexPages.Severity.ERROR,'error'));
        ApexPages.addMessage(new ApexPages.message(ApexPages.Severity.CONFIRM,'confirm'));

        C501_UTIL_PageMessages_CTRL controller = new C501_UTIL_PageMessages_CTRL();
        List<C501_UTIL_PageMessages_CTRL.Message> messages = controller.getPageMessages();

        System.assertEquals(5, messages.size(), 'There should be 5 messages on the page.');

        System.assertEquals(Label.Merge_PageMessagesWarning, messages[0].getSeverityLabel(), 'The label should be WARNING.');
        System.assertEquals('', messages[1].getSeverityLabel(), 'The label should be INFO.');
        System.assertEquals(Label.Merge_PageMessagesFatal, messages[2].getSeverityLabel(), 'The label should be FATAL.');
        System.assertEquals(Label.Merge_PageMessagesError, messages[3].getSeverityLabel(), 'The label should be ERROR.');
        System.assertEquals(Label.Merge_PageMessagesConfirm, messages[4].getSeverityLabel(), 'The label should be CONFIRM.');

        System.assertEquals('warning', messages[0].getSeverityTheme(), 'The theme should be warning.');
        System.assertEquals('info', messages[1].getSeverityTheme(), 'The theme should be info.');
        System.assertEquals('error', messages[2].getSeverityTheme(), 'The theme should be fatal.');
        System.assertEquals('error', messages[3].getSeverityTheme(), 'The theme should be error.');
        System.assertEquals('success', messages[4].getSeverityTheme(), 'The theme should be confirm.');

        Test.StopTest();
    }

    @isTest(SeeAllData='true')
    public static void ProcessObjectMerges_NoMergeBlankAutoMergeObjectsTest() {

        List<C501_Merge_Metric__c> mergeMetrics = C501_MassMerge_SharedCode.TestDiscoverHelper('Contact Test');

        String strSoql = C501_MassMerge_SharedCode.GetC501ContactMergeQuery();
        if (String.isBlank(strSoql)) {
            return;
        }
        strSoql += ' AND Merge_Metric__r.Name LIKE \'TESTDATA%\'';

        List<C501_Contact_Merge__c> objectMergesBefore = Database.query(strSoql);
        System.Assert(objectMergesBefore.size() > 0);

        for (C501_Merge_Metric__c mergeMetric :mergeMetrics) {

            mergeMetric.AutoMerge_Percentage__c = 0.0;
            mergeMetric.AutoMerge_Objects__c = '';
        }

        update mergeMetrics;

        Test.StartTest();

        C501_MM_Rule_Merge mergeRule = new C501_MM_Rule_Merge();
        for (C501_Merge_Metric__c mergeMetric :mergeMetrics) {
            mergeRule.executeMetricById(mergeMetric.Id);
        }

        Test.StopTest();

        List<C501_Contact_Merge__c> objectMergesAfter = Database.query(strSoql);
        System.AssertEquals(objectMergesBefore.size(), objectMergesAfter.size());
    }

    @isTest(SeeAllData='true')
    public static void ProcessObjectMerges_MergeAboveAutoMergePercentageTest() {

        List<C501_Merge_Metric__c> mergeMetrics = C501_MassMerge_SharedCode.TestDiscoverHelper('Contact Test');

        String strSoql = C501_MassMerge_SharedCode.GetC501ContactMergeQuery();
        if (String.isBlank(strSoql)) {
            return;
        }
        strSoql += ' AND Merge_Metric__r.Name LIKE \'TESTDATA%\'';

        List<C501_Contact_Merge__c> objectMergesBefore = Database.query(strSoql);
        System.Assert(objectMergesBefore.size() > 0);

        String autoMergeObjects = C501_MassMerge_SharedCode.TestHelperAutoMergeObjects('Contact', 'Object');
        if (String.isBlank(autoMergeObjects)) {
            return;
        }

        for (C501_Merge_Metric__c mergeMetric :mergeMetrics) {
            mergeMetric.AutoMerge_Objects__c = autoMergeObjects;
            mergeMetric.AutoMerge_Percentage__c = 0.0;
        }

        update mergeMetrics;

        Test.StartTest();

        C501_MM_Rule_Merge mergeRule = new C501_MM_Rule_Merge();
        for (C501_Merge_Metric__c mergeMetric :mergeMetrics) {
            mergeRule.executeMetricById(mergeMetric.Id);
        }

        Test.StopTest();

        List<C501_Contact_Merge__c> objectMergesAfter = Database.query(strSoql);
        System.AssertNotEquals(objectMergesBefore.size(), objectMergesAfter.size());
    }

    @isTest(SeeAllData='true')
    public static void ProcessObjectMerges_MergeEqualAutoMergePercentageNoAutoMergeObjectsTest() {

        List<C501_Merge_Metric__c> mergeMetrics = C501_MassMerge_SharedCode.TestDiscoverHelper('Contact Test');

        String strSoql = C501_MassMerge_SharedCode.GetC501ContactMergeQuery();
        if (String.isBlank(strSoql)) {
            return;
        }
        strSoql += ' AND Merge_Metric__r.Name LIKE \'TESTDATA%\'';

        List<C501_Contact_Merge__c> objectMergesBefore = Database.query(strSoql);
        System.Assert(objectMergesBefore.size() > 0);

        for (C501_Merge_Metric__c mergeMetric :mergeMetrics) {
            mergeMetric.AutoMerge_Percentage__c = objectMergesBefore[0].Merge_Confidence__c;
            mergeMetric.AutoMerge_Objects__c = '';
        }

        update mergeMetrics;

        Test.StartTest();

        C501_MM_Rule_Merge mergeRule = new C501_MM_Rule_Merge();
        for (C501_Merge_Metric__c mergeMetric :mergeMetrics) {
            mergeRule.executeMetricById(mergeMetric.Id);
        }

        Test.StopTest();

        List<C501_Contact_Merge__c> objectMergesAfter = Database.query(strSoql);
        System.AssertEquals(objectMergesBefore.size(), objectMergesAfter.size());
    }

    @isTest(SeeAllData='true')
    public static void ProcessObjectMerges_MergeEqualAutoMergePercentageTest() {

        List<C501_Merge_Metric__c> mergeMetrics = C501_MassMerge_SharedCode.TestDiscoverHelper('Contact Test');

        String strSoql = C501_MassMerge_SharedCode.GetC501ContactMergeQuery();
        if (String.isBlank(strSoql)) {
            return;
        }
        strSoql += ' AND Merge_Metric__r.Name LIKE \'TESTDATA%\'';

        List<C501_Contact_Merge__c> objectMergesBefore = Database.query(strSoql);
        System.Assert(objectMergesBefore.size() > 0);

        String autoMergeObjects = C501_MassMerge_SharedCode.TestHelperAutoMergeObjects('Contact', 'Object');
        if (String.isBlank(autoMergeObjects)) {
            return;
        }

        for (C501_Merge_Metric__c mergeMetric :mergeMetrics) {

            mergeMetric.AutoMerge_Objects__c = autoMergeObjects;
            mergeMetric.AutoMerge_Percentage__c = objectMergesBefore[0].Merge_Confidence__c;
        }

        update mergeMetrics;

        Test.StartTest();

        C501_MM_Rule_Merge mergeRule = new C501_MM_Rule_Merge();
        for (C501_Merge_Metric__c mergeMetric :mergeMetrics) {
            mergeRule.executeMetricById(mergeMetric.Id);
        }

        Test.StopTest();

        List<C501_Contact_Merge__c> objectMergesAfter = Database.query(strSoql);
        System.AssertNotEquals(objectMergesBefore.size(), objectMergesAfter.size());
    }

    @isTest(SeeAllData='true')
    public static void ProcessObjectMerges_AutoMergeObjectsByObject() {

       String autoMergeObjects = C501_MassMerge_SharedCode.TestHelperAutoMergeObjects('Contact', 'Object');
        if (String.isBlank(autoMergeObjects)) {
            return;
        }

        List<C501_Merge_Metric__c> mergeMetrics = C501_MassMerge_SharedCode.TestDiscoverHelper('Contact Test');

        String strSoql = C501_MassMerge_SharedCode.GetC501ContactMergeQuery();
        if (String.isBlank(strSoql)) {
            return;
        }
        strSoql += ' AND Merge_Metric__r.Name LIKE \'TESTDATA%\'';

        List<C501_Contact_Merge__c> objectMergesBefore = Database.query(strSoql);
        System.Assert(objectMergesBefore.size() > 0);

        for (C501_Merge_Metric__c mergeMetric :mergeMetrics) {
            mergeMetric.AutoMerge_Objects__c = autoMergeObjects;
            mergeMetric.AutoMerge_Percentage__c = objectMergesBefore[0].Merge_Confidence__c;
        }

        update mergeMetrics;

        Test.StartTest();

        C501_MM_Rule_Merge mergeRule = new C501_MM_Rule_Merge();
        for (C501_Merge_Metric__c mergeMetric :mergeMetrics) {
            mergeRule.executeMetricById(mergeMetric.Id);
        }

        Test.StopTest();

        List<C501_Contact_Merge__c> objectMergesAfter = Database.query(strSoql);
        System.AssertNotEquals(objectMergesBefore.size(), objectMergesAfter.size());
    }

    @isTest(SeeAllData='true')
    public static void ProcessObjectMerges_AutoMergeObjectsByObjectsWithinObject() {

        String autoMergeObjects = C501_MassMerge_SharedCode.TestHelperAutoMergeObjects('Contact', 'ObjectsWithinObject');
        if (String.isBlank(autoMergeObjects)) {
            return;
        }

        List<C501_Merge_Metric__c> mergeMetrics = C501_MassMerge_SharedCode.TestDiscoverHelper('Contact Test');

        String strSoql = C501_MassMerge_SharedCode.GetC501ContactMergeQuery();
        if (String.isBlank(strSoql)) {
            return;
        }
        strSoql += ' AND Merge_Metric__r.Name LIKE \'TESTDATA%\'';

        List<C501_Contact_Merge__c> objectMergesBefore = Database.query(strSoql);
        System.Assert(objectMergesBefore.size() > 0);

        for (C501_Merge_Metric__c mergeMetric :mergeMetrics) {
            mergeMetric.AutoMerge_Objects__c = autoMergeObjects;
            mergeMetric.AutoMerge_Percentage__c = objectMergesBefore[0].Merge_Confidence__c;
        }

        update mergeMetrics;

        Test.StartTest();

        C501_MM_Rule_Merge mergeRule = new C501_MM_Rule_Merge();
        for (C501_Merge_Metric__c mergeMetric :mergeMetrics) {
            mergeRule.executeMetricById(mergeMetric.Id);
        }

        Test.StopTest();

        List<C501_Contact_Merge__c> objectMergesAfter = Database.query(strSoql);
        System.AssertNotEquals(objectMergesBefore.size(), objectMergesAfter.size());
    }

    @isTest(SeeAllData='true')
    public static void ProcessObjectMerges_AutoMergeObjectsByObjectsAcrossObjects() {

        String autoMergeObjects = C501_MassMerge_SharedCode.TestHelperAutoMergeObjects('Contact', 'ObjectsAcrossObjects');
        if (String.isBlank(autoMergeObjects)) {
            return;
        }

        List<C501_Merge_Metric__c> mergeMetrics = C501_MassMerge_SharedCode.TestDiscoverHelper('Contact Test');

        String strSoql = C501_MassMerge_SharedCode.GetC501ContactMergeQuery();
        if (String.isBlank(strSoql)) {
            return;
        }
        strSoql += ' AND Merge_Metric__r.Name LIKE \'TESTDATA%\'';

        List<C501_Contact_Merge__c> objectMergesBefore = Database.query(strSoql);
        System.Assert(objectMergesBefore.size() > 0);

        for (C501_Merge_Metric__c mergeMetric :mergeMetrics) {
            mergeMetric.AutoMerge_Objects__c = autoMergeObjects;
        }

        update mergeMetrics;

        Test.StartTest();

        C501_MM_Rule_Merge mergeRule = new C501_MM_Rule_Merge();
        for (C501_Merge_Metric__c mergeMetric :mergeMetrics) {
            mergeRule.executeMetricById(mergeMetric.Id);
        }

        Test.StopTest();

        List<C501_Contact_Merge__c> objectMergesAfter = Database.query(strSoql);
        System.AssertNotEquals(objectMergesBefore.size(), objectMergesAfter.size());
    }

    @isTest(SeeAllData='true')
    public static void ProcessObjectMerges_DeleteMergeMetricMergesTest() {

        Set<Id> mergeMetricIds = new Set<Id>();
        List<C501_Merge_Metric__c> mergeMetrics = C501_MassMerge_SharedCode.TestDiscoverHelper('Contact Test');

        String strSoql = C501_MassMerge_SharedCode.GetC501ContactMergeQuery();
        if (String.isBlank(strSoql)) {
            return;
        }
        strSoql += ' AND Merge_Metric__r.Name LIKE \'TESTDATA%\'';

        List<C501_Contact_Merge__c> objectMergesBefore = Database.query(strSoql);
        System.Assert(objectMergesBefore.size() > 0);

        for (C501_Merge_Metric__c mergeMetric :mergeMetrics) {

            mergeMetricIds.add(mergeMetric.Id);
            mergeMetric.AutoMerge_Percentage__c = objectMergesBefore[0].Merge_Confidence__c;
        }

        update mergeMetrics;

        Test.StartTest();

        C501_MassMerge_SharedCode.DeleteMergeMetricChildren(mergeMetricIds);

        Test.StopTest();

        List<C501_Contact_Merge__c> objectMergesAfter = Database.query(strSoql);
        System.AssertNotEquals(objectMergesBefore.size(), objectMergesAfter.size());
    }

}